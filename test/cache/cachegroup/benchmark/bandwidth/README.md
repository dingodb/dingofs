

* `memcpy` 4MiB about `400us`
* `memset` 4MiB about `1900us`


# 发送缓存块的几种方式

## 方式一：提前分配内存 + AioRead(io_direct)
* 分配内存 (malloc + memset): `1900 us`
* 读取文件到内存 (aio + direct-io): `1434 us`
* 内存写到 socket (IOBuf + write): `1840 us`

一共约 `5174 us`，约 `5ms`

```C
buffer = malloc(4194304)
memset(buffer, 0, 4194304)

aio_read(fd, buffer, 4194304, 0)

IOBuf iobuf;
iobuf.append(buffer)
```

## 方式二：AioRead(io_direct)

* 分配内存 (malloc): `12 us`
* 读取文件到内存 (aio + direct-io): `2254 us`
* 内存写到 socket (IOBuf + write): `1730 us`

一共约 `3996 us`，约 `4ms`

## 方式三：Read(io_direct)

* 分配内存 (malloc): `11 us`
* 读取文件到内存 (read + direct-io): `2288 us`
* 内存写到 socket (IOBuf + write): `1919 us`

一共约 `4207 us`，约 `4ms`

## 方式四：Read

* 分配内存 (malloc): `13 us`
* 读取文件到内存 (read): `3565 us`
* 内存写到 socket (IOBuf + write): `969 us`

如果已经缓存了，第 2 步和第 3 步的时间大约为 `2365 us` 和 `964 us`

一共约 `4538 us`，约 `4.5ms`

再次读约 `3342 us`

## 方式五：mmap

* mamp 映射文件：`5us`
* 将映射的内容直接写到 socket (IOBuf + write): `3082 us`

一共约 `3082 us`，约 `3ms`

> 需要注意的是，mmap 会先将文件内容读到 page cache，然后将 mmap 映射的虚拟空间与 page cache 的虚拟空间指向相同的页，所以系统的 `buffer/cache` 会增大；
> 第一次读的是 `3082 us`，如果缓存了再次读的话大约为 `2148 us`

需要注意的是，如果需要清理文件对应的 page cache，可以调用 `posix_fadvise`，这个系统调用大概需要消耗 `400us` 的时间，所以最好做成异步。

遗留问题：
* 怎么清除内核中的 buffer?
* 为什么 `O_DIRECT` 对 mmap 不生效？

## 方式六：mmap + MAP_POPULATE

* mmap 映射并提前预读：`1864us`
* 将映射的内容直接写到 socket (IOBuf + write): `1894 us`

一共约 `3758 us`，约 `4ms`

需要注意的是，由于 page cache 的存在，再次 mmap 的时候会减少到 `296us` 左右

## 方式七：mmap + MAP_LOCKED

* mmap 映射并提前预读：`1935us`
* 将映射的内容直接写到 socket (IOBuf + write): `1958 us`

一共约 `3893 us`，约 `4ms`

## 方式八：sendfile

如果文件已经缓存，大概需要 `770us`

* 大约需要 `2054us`

一共约 `2054 us`，约 `2ms`

如果文件已缓存的话，大概需要 `800us`

## 方式九：sendfile + O_DIRECT

* 大概需要 `4017 us`

一共约 `4017 us`，约 `4ms`

### 其他

* 如果用 mmap 的话大概 `1776us`
* 正常边读边写: buf 1MB (`3513us`)
* 正常边读边写: buf 1MB (`3767us`)


## 传输性能

目前一个客户端请求服务端拿回 block 大概需要 `7ms`

* 16986 字节约 `15us`，所以 4MB 大约需要 `3703 us`

