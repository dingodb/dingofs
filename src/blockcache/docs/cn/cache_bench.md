

# case 1: 4K 并发读

参数

```bash
--remote_rdma=true
--remote_rdma_device=mlx5_0
--remote_rdma_port_num=1
--fsid=10000

--shards=8
--pin_cpu=true

--threads=16
--iodepth=64

--op=get
--blocks=100000
--blksize=4096
--length=4096
```

结果
```bash
1526752 op/s  5963 MB/s  lat(0.000667 0.115074 0.000026)
```

# case 2: 4M 单线程