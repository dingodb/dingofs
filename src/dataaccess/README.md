Data Access
===

DataAccesser is a class that provides a way to access data from a data source. It is a base class for all data access classes.

Module Level
---

```
              +-- S3 Accesser -> S3 Adapter -> { aws_crt_client / aws_lagacy_client }
              |
Data Accesser +
              |
              +--> Rados Accesser
```
