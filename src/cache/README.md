Block Cache Layer
===

```
+----------------+
|     Client     |
+----------------+
        |
        | (put、range...)
        v
+----------------+  -----upload----->  +----------------+
|  Block  Cache  |                     |       S3       |
+----------------+  <----download----  +----------------+
        |
        | (stage、removestage、cache、load...)
        v
+----------------+
|  Cache  Store  |
+----------------+
        |
        | (writefile、readfile...)
        v
+----------------+
|      Disk      |
+----------------+
```

* `block_cache_throttle`
* `block_cache_uploader`
* `disk_cache_group`
* `disk_cache`
    * `disk_cache_loader`
    * `disk_cache_manager`
    * `disk_cache_metric`
    * `disk_cache_watcher`
    * `disk_state_health_checker`

Cache Group Layer
===

The server side for distrubuted block cache.

```
     +--------------------------+
     | Cache Group Node Server  |  // brpc server
     +--------------------------+
                  |
                  v
     +--------------------------+
     | Cache Group Node Service |  // brpc service
     +--------------------------+
                  |
                  v
     +--------------------------+
     |   Cache  Group  Node     |
     +--------------------------+
       |
       v
+--------------+     +--------------+
| Block  Cache |     |  Heartbeat   |
+--------------+     +--------------+
```


Remote Block Cache Layer
===

The client side for distrubuted block cache.

```
   +--------------------------+
   |          Client          |
   +--------------------------+
                |
                | range
                v
   +--------------------------+    download   +----------+
   |    Remote Block Cache    | <------------ |    S3    |
   +--------------------------+               +----------+
                |
                | select cache group node by consitent hash algorithm
                v
   +--------------------------+
   | Cache Group Node Manager |
   +--------------------------+
                |
                | create rpc client for specified cache group node
                v
   +--------------------------+
   |        RPC  Client       |
   +--------------------------+
                |
                | range
                |
        (N)     |       (Y)
   +-----------<?>------------+
   |   (support local read?)  |
   |                          |
   |                          |
   v                          v
network         +--------------------------+
                |       Cache  Store       |
                +--------------------------+
                              |
                              |
                     +--------+-------+
                     |                |
                +----------+    +----------+
                |   Disk   |    |    3FS   |
                +----------+    +----------+
```
