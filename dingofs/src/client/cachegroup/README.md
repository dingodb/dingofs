Cache Group
===

Cache group is c/s architecture, it consists of two modules:

* `Remote Block Cache`: Client
* `Block Cache Service`: Server

```
       +-----------------------------------+
       |               Client              |
       +-----------------------------------+
           |                           |
           | (posix)                   |
           v                           v
+---------------------+    +----------------------+
|  Local Block Cache  |    |  Remote Block Cache  | (client)
+---------------------+    +----------------------+
                                       |
                                       | (rpc)
                                       |
                                       v
                           +----------------------+
                           |      BRPC Server     | (server)
                           +----------------------+
                                       |
                                       v
                           +----------------------+
                           | Block Cache Service  |
                           +----------------------+
                                       |
                                       v
                           +----------------------+
                           |  Local Block Cache   |
                           +----------------------+
```
