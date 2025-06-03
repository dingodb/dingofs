Cluster Disk Swap
===

Step 1: Commit the list of added disks
---

```shell 
$ vim format.yaml 
```

```yaml 
host.
  - server-host4
  - server-host5
  - server-host6 
disk.
  - /dev/sda:/data/chunkserver0:90 # Replace the disk that came up
  - /dev/sdb:/data/chunkserver1:90 # Replaced disk
  - /dev/sdc:/data/chunkserver2:90 # Replacement disk 
```

> ⚠️ **WARNING:** 
> 
> The `format.yaml` file should only be filled with the list of disks on the added machines, **DO NOT** fill it with the list of disks that are already in service on the cluster, to avoid irreparable damage.


Step 2: Pull up the service
--- 
```shell 
$ dingoadm start --id serviceId 
```