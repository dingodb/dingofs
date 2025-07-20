Tier Block Cache
===

Block Cache
===

```
+----------------+
|     Client     |
+----------------+
        |
        | (put, range, cache, prefetch)
        v
+----------------+  -----upload----->  +----------------+
|  Block  Cache  |                     |       S3       |
+----------------+  <----download----  +----------------+
         |
         | (stage, removestage, cache, load)
         v
+------------------+
|   Cache  Store   |
+------------------+
         |
         |
         v
+------------------+
| Local FileSystem |
+------------------+
         |
         | (writefile、readfile...)
         v
+----------------+
|    IO Uring    |
+----------------+
         |
         | (write, read)
         v
+----------------+
|      Disk      |
+----------------+
```

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
