# File System Real-time Monitoring
## 1. Real-time Monitoring 
Use the dingo tool to monitor mounted filesystems in real time, and print out metrics data at regular intervals.
```
dingo stats <mount-point>
```
- Parameter Description \
mount-point: Path where the file system is mounted.\
The stats command has the following options:
schema The default value is ufmsbo, which stands for: u: usage, f: fuse, m: metaserver,s: mds, b: blockcache, o: object
- Example: show only fuse metrics
```
dingo fs stats /mnt/dingofs --schema f
```
--interval duration , the default value is 1s, and the minimum value is also 1s, which means how often the indicator data will be output to show on the screen.\
Example: output indicator data every two seconds
```
dingo fs stats /mnt/dingofs  --interval 2s
```
--verbose , show more detailed information. \
Example: show detailed data of s3
```
dingo fs stats /mnt/dingofs  --schema o --verbose
```

## 2. Monitoring Results 
! [alt text](../../images/monitoring_results.png)
## 3. Monitoring Metrics in Detail
- usage\
cpu: client program CPU usage in percentage. \
mem: client program physical memory usage. \
wbuf: size of data write buffer already used by the client program.
- fuse\
opt/lat : Number of requests per second processed through the fuse interface and their average latency in milliseconds. Note: A file may be created multiple times by oops, e.g., open, write, flush, close, etc. The file may be created by a number of oops. \
read/write: read/write bandwidth processed per second through the fuse interface.
- metaserver\
opt/lat : The number of metadata requests processed by the metaserver per second and its average latency in milliseconds. Main operations createdentry, listDentry, deleteDentry, etc.
- blockcache\
Currently blockcache mainly refers to the disk cache read and write situation, if the disk cache is enabled, the data will be read and written from the disk cache first, if the data is not in the disk cache or the data has been invalidated, it will be read and written from the object store. If the blockcache read/write traffic is much larger than the get and put traffic in the object, it means the data is cached.
read/write: the client's local disk cache read/write traffic per second.
- object\
object represents a metric related to the object store. In the presence of a disk cache scenario, a read request penetrating into the object store will significantly reduce read performance, which can be used to determine whether the data is fully cached. \
get/ops/lat: the value of the bandwidth of the object store to process read requests per second, the number of interface requests and their average latency (in milliseconds). \
put/ops/lat: the value of the bandwidth per second that the object store processes write requests, the number of interface requests and their average latency (in milliseconds).
