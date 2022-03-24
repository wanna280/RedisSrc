/* Background I/O service for Redis.
 *
 * This file implements operations that we need to perform in the background.
 * Currently there is only a single operation, that is a background close(2)
 * system call. This is needed as when the process is the last owner of a
 * reference to a file closing it means unlinking it, and the deletion of the
 * file is slow, blocking the server.
 *
 * In the future we'll either continue implementing new things we need or
 * we'll switch to libeio. However there are probably long term uses for this
 * file as we may want to put here Redis specific background tasks (for instance
 * it is not impossible that we'll need a non blocking FLUSHDB/FLUSHALL
 * implementation).
 *
 * DESIGN
 * ------
 *
 * The design is trivial, we have a structure representing a job to perform
 * and a different thread and job queue for every job type.
 * Every thread wait for new jobs in its queue, and process every job
 * sequentially.
 *
 * Jobs of the same type are guaranteed to be processed from the least
 * recently inserted to the most recently inserted (older jobs processed
 * first).
 *
 * Currently there is no way for the creator of the job to be notified about
 * the completion of the operation, this will only be added when/if needed.
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"
#include "bio.h"

static pthread_t bio_threads[REDIS_BIO_NUM_OPS];      // bio的线程数组
static pthread_mutex_t bio_mutex[REDIS_BIO_NUM_OPS];  // 对应类型bio线程的锁
static pthread_cond_t bio_condvar[REDIS_BIO_NUM_OPS]; // 对应类型的bio线程的阻塞队列
static list *bio_jobs[REDIS_BIO_NUM_OPS];             // 对应类型的bio的任务队列
/* The following array is used to hold the number of pending jobs for every
 * OP type. This allows us to export the bioPendingJobsOfType() API that is
 * useful when the main thread wants to perform some operation that may involve
 * objects shared with the background thread. The main thread will just wait
 * that there are no longer jobs of this type to be executed before performing
 * the sensible operation. This data is also useful for reporting. */
static unsigned long long bio_pending[REDIS_BIO_NUM_OPS];

/* This structure represents a background Job. It is only used locally to this
 * file as the API does not expose the internals at all. */
struct bio_job
{
    time_t time; /* Time at which the job was created. */
    /* Job specific arguments pointers. If we need to pass more than three
     * arguments we can just pass a pointer to a structure or alike. */
    // 特定类型的参数指针，如果我们想要传递更多的参数，我们只能通过类似传递一个结构体指针的方式去进行传递
    void *arg1, *arg2, *arg3;
};

void *bioProcessBackgroundJobs(void *arg);

/* Make sure we have enough stack to perform all the things we do in the
 * main thread. */
#define REDIS_THREAD_STACK_SIZE (1024 * 1024 * 4)

/* Initialize the background system, spawning the thread. */
void bioInit(void)
{
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;
    int j;

    /* Initialization of state vars and objects */
    // 初始化bio相关的mutex锁和条件变量、任务队列，任务队列中的任务数量等相关对象
    for (j = 0; j < REDIS_BIO_NUM_OPS; j++)
    {
        pthread_mutex_init(&bio_mutex[j], NULL);  // 初始化mutex锁
        pthread_cond_init(&bio_condvar[j], NULL); // 初始化条件变量，也就是等待队列

        // 在进行bio_jobs以及bio_pending的操作时，都需要获取到该类型任务对应的mutex锁
        bio_jobs[j] = listCreate(); // 初始化任务队列
        bio_pending[j] = 0;         // 任务队列中的任务数量
    }

    /* Set the stack size as by default it may be small in some system */
    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr, &stacksize);
    if (!stacksize)
        stacksize = 1; /* The world is full of Solaris Fixes */
    while (stacksize < REDIS_THREAD_STACK_SIZE)
        stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    /* Ready to spawn our threads. We use the single argument the thread
     * function accepts in order to pass the job ID the thread is
     * responsible of. */
    // 准备去产生(spawn)线程，我们使用一个参数的线程函数去进行接收，目的是跳过这个线程所关联的job ID
    for (j = 0; j < REDIS_BIO_NUM_OPS; j++)
    {
        void *arg = (void *)(unsigned long)j;

        // 使用pthread去创建线程...并设置线程要执行的任务为bioProcessBackgroundJobs
        if (pthread_create(&thread, &attr, bioProcessBackgroundJobs, arg) != 0)
        {
            redisLog(REDIS_WARNING, "Fatal: Can't initialize Background Jobs.");
            exit(1);
        }
        bio_threads[j] = thread; // 保存bio线程
    }
}

// 给后台线程去添加要执行的任务，需要获取到锁
void bioCreateBackgroundJob(int type, void *arg1, void *arg2, void *arg3)
{
    struct bio_job *job = zmalloc(sizeof(*job));

    // 设置要job的要用到的参数
    job->time = time(NULL); // 设置job的start time
    job->arg1 = arg1;
    job->arg2 = arg2;
    job->arg3 = arg3;

    // 拿到job类型的锁，lock，等后台线程因为没有任务被挂起
    // 或者它在执行io任务的时候，会把锁放开，可以在这一段去把锁抢过来，去对任务队列去进行添加任务
    pthread_mutex_lock(&bio_mutex[type]);

    listAddNodeTail(bio_jobs[type], job); // 往job类型对应的类型的队列当中添加任务
    bio_pending[type]++;                  // 该类型的任务数量++

    pthread_cond_signal(&bio_condvar[type]); // 如果必要的话(如果后台线程因为没有任务而阻塞)，唤醒后台线程去进行fsync，signal...

    pthread_mutex_unlock(&bio_mutex[type]); // 操作完之后，unlock，后台线程可以把锁拿回来了...
}

void *bioProcessBackgroundJobs(void *arg)
{
    struct bio_job *job;
    unsigned long type = (unsigned long)arg;
    sigset_t sigset;

    /* Make the thread killable at any time, so that bioKillThreads()
     * can work reliably. */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    pthread_mutex_lock(&bio_mutex[type]); // 加上对应类型任务的mutex锁
    /* Block SIGALRM so we are sure that only the main thread will
     * receive the watchdog signal. */
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGALRM);
    if (pthread_sigmask(SIG_BLOCK, &sigset, NULL))
        redisLog(REDIS_WARNING,
                 "Warning: can't mask SIGALRM in bio.c thread: %s", strerror(errno));

    while (1)
    {
        listNode *ln;

        /* The loop always starts with the lock hold. */
        // 如果该类型的bio的任务数量为0，那么使用条件变量在这里挂着等着(释放锁...主线程才能来添加任务)
        // 当有主线程有任务了，就会将任务添加到队列当中，并且使用signal去唤醒当前线程去继续执行任务
        if (listLength(bio_jobs[type]) == 0)
        {
            pthread_cond_wait(&bio_condvar[type], &bio_mutex[type]);
            continue;
        }
        /* Pop the job from the queue. */
        ln = listFirst(bio_jobs[type]); // 获取链表的head节点
        job = ln->value;
        /* It is now possible to unlock the background system as we know have
         * a stand alone job structure to process.*/
        pthread_mutex_unlock(&bio_mutex[type]); // 释放对应类型的锁，目的是让主线程可以去方便去进行添加任务，或者去获取任务的数量

        /* Process the job accordingly to its type. */
        if (type == REDIS_BIO_CLOSE_FILE)
        { // 如果是关闭文件的任务的话
            close((long)job->arg1);
        }
        else if (type == REDIS_BIO_AOF_FSYNC)
        { // 如果是aof的 fsync的任务的话
            aof_fsync((long)job->arg1);
        }
        else
        {
            redisPanic("Wrong job type in bioProcessBackgroundJobs().");
        }
        zfree(job); // 释放内存

        /* Lock again before reiterating the loop, if there are no longer
         * jobs to process we'll block again in pthread_cond_wait(). */

        // 再下次迭代之前，需要需要再次加上对应类型的锁，如果没有任务了，我们需要使用条件变量去进行挂起
        pthread_mutex_lock(&bio_mutex[type]);
        listDelNode(bio_jobs[type], ln); // 删除头节点
        bio_pending[type]--;             // 该类型的任务数量--
    }
}

/* Return the number of pending jobs of the specified type. */
// 返回特定的类型的pending job数量
unsigned long long bioPendingJobsOfType(int type)
{
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);   // 拿到锁(在执行close/fsync的时候，会释放开锁，可以趁这一段去拿到锁，获取后台的任务队列的任务数量)
    val = bio_pending[type];                // 获取该类型的任务队列的任务数量...
    pthread_mutex_unlock(&bio_mutex[type]); // 释放锁，让后台线程可以继续执行任务...
    return val;
}

/* Kill the running bio threads in an unclean way. This function should be
 * used only when it's critical to stop the threads for some reason.
 * Currently Redis does this only on crash (for instance on SIGSEGV) in order
 * to perform a fast memory check without other threads messing with memory. */
void bioKillThreads(void)
{
    int err, j;

    for (j = 0; j < REDIS_BIO_NUM_OPS; j++)
    {
        if (pthread_cancel(bio_threads[j]) == 0)
        {
            if ((err = pthread_join(bio_threads[j], NULL)) != 0)
            {
                redisLog(REDIS_WARNING,
                         "Bio thread for job type #%d can be joined: %s",
                         j, strerror(err));
            }
            else
            {
                redisLog(REDIS_WARNING,
                         "Bio thread for job type #%d terminated", j);
            }
        }
    }
}
