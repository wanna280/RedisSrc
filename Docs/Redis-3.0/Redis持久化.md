在`beforeSleep`的过程中，会进行aof的刷盘，在主线程当中将内存中的rdb_buf刷到aof_fd对应的磁盘当中的文件当中去。

在定时任务(`serverCron`)中会fork一个进程去进行rdb文件的刷盘

还会检查aof文件是否应该被重写(调用了bgsave命令或者文件到达阈值)，如果应该被重写时，那么也应该fork一个进程去进行aof的重写。

cron是计划任务的意思

在进行aof的rewrite/rdb时，会关闭所有的server的fd。