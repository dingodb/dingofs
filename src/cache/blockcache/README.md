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
