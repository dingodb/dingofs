Deploying the DingoFS client 
===

Step 1: Environment Preparation
----
* [Hardware and software requirements](../dingoadm/install.md#software-and-hardware-environment-requirements)
* [Installing Dependencies](../dingoadm/install.md#installing-dependencies)

Step 2: Import the host
---

Users need to import the list of hosts required for the client, if you have already imported the client hosts while deploying the cluster, you can skip this step directly.
Please make sure that the hosts specified in the subsequent mount/unmount are imported, see [host management](../dingoadm/hosts.md) for details.

### 1. Prepare the list of hosts

```shell 
$ vim hosts.yaml 
```

``` yaml 
global: 
 user: dingo 
 ssh_port: 22 
 private_key_file: /home/dingo/.ssh/id_rsa

private_key_file: /home/dingo/.ssh/id_rsa
  - host: server-host1 
 hostname: 10.0.1.1
  - host: server-host2 
 hostname: 10.0.1.2
  - host: server-host3 
 hostname: 10.0.1.3
  - host: client-host 
 hostname: 10.0.1.4 
```

### 2. import hosts list 
```shell 
$ dingoadm hosts commit hosts.yaml 
```

Step 3: Deploy Minio (optional)
---

This step is optional.

So you need to deploy an S3 storage or use a public cloud object storage such as Amazon S3, AliCloud OSS, Tencent Cloud OSS, etc.
The following will show if you can quickly deploy a [Minio][minio] for S3 backend storage using Docker:

```shell 
$ mkdir minio-data 
$ sudo docker run -d --name minio \
-p 9000:9000 \
-p 9900:9900 \
-v minio-data:/data \\
--restart unless-stopped \ 
minio/minio server /data --console-address ":9900" 
``` 
> 📢 **Note:** 
> 
> minio-data in the run parameter is a local path, you need to create this directory in advance before running the minio You need to create this directory in advance before running the container

> 💡 **Reminder:**
> 
> The following information will be used to fill in the [S3-related-config](../dingoadm/topology.md#important-dingofs-configuration-items) in the client configuration file in step 4: 
> * The default `Access Key` and `Secret Key` for the root user are both `minioadmin` 
> * The S3 service access address is `http://$IP:9000`, and you need to access `http://$IP:9000` from your browser to create a bucket 
> * For more details on deployment, you can refer to [deploy-minio-standalone][deploy-minio-standalone].

Step 4: Prepare the client configuration file
---

```shell 
$ vim client.yaml 
```

```yaml 
kind: dingofs
s3.ak: <>
s3.sk: <>
s3.endpoint: <>
s3.bucket_name: <> 
container_image: dingodatabase/dingofs:latest 
mdsOpt.rpcRetryOpt.addrs: 10.0.1.1:6700,10.0.1.2:6700,10.0.1.3:6700 
log _dir: /home/dingo/logs/client 
data_dir: /home/dingo/data/dingofs 
disk_cache.cache_dir: /dingofs/client/cache/1:10240;/dingofs/client/ cache/2:10240;/dingofs/client/cache/3:10240 # container_path:size 
mount_dirs: /mnt/cache/1:/dingofs/client/cache/1;/mnt/cache/2:/ dingofs/client/cache/2;/mnt/cache/3:/dingofs/client/cache/3 # host_path:container_path

# quota 
quota.capacity: 10 
quota.inodes: 1000 
```

Configuration entries in the client configuration file have the same meaning as those in the cluster topology file, see [DingoFS important-config](../dingoadm/topology.md#important-dingofs-configuration-items).

For all configuration items that do not appear in the client configuration file, we will use the default configuration values. 
You can view the configuration items and their associated default values by clicking [client configuration file][dingofs-client-conf].

> 💡 About the `mdsOpt.rpcRetryOpt.addrs` configuration item 
> 
> Since all the routing information exists in the MDS service, the client only needs to know the address of the MDS service in the cluster in order to perform IO reads and writes normally.
> 
> The `mdsOpt.rpcRetryOpt.addrs` configuration item in the configuration file needs to be filled with the address of the MDS service in the cluster. After deploying the DingoFS cluster, 
> You can check the address of the MDS service in the cluster via `dingoadm status`: 
> 
> ```shell 
> $ dingoadm status 
> Get Service Status: [OK] 
> 
> cluster name : my-cluster 
> cluster kind : dingofs 
> cluster mds addr: 10.0.1.1:6700,10.0.1.2:6700,10.0.1.3:6700 
> cluster mds leader: 10.0.1.1:6700 / 505da008b59c 
> ...

> 📢 **Note:** 
> 
> Please make sure to configure the data_dir configuration item if users need to enable local disk caching.

> 📢 **Note:** 
> 
> dingofs supports multiple s3, one fs corresponds to one s3 backend.
> All s3 information (ak, sk, endpoint, and bucket_name) is stored in mds, and other components get it from mds.
> And the s3 information in the mds is specified when the fs is created.
> Therefore, the s3 information in client.yaml is required and will be synchronized to the configuration file of the fs creation tool.
> If the fs have already been created, please keep the same information, otherwise the mount will fail; 
> If the fs have not been created yet, please make sure the s3 information is available, otherwise the mount will fail.

Step 5: Mount the DingoFS file system
---

```shell
$ dingoadm mount <dingofs-name> <mount-point> --host <host> -c client.yaml
```
```
* `<dingofs-name>`: filesystem name, user-defined, but must be a combination of **lowercase letters, numbers, and hyphens**, i.e., satisfy the regular expression `^([a-z0-9]+\\-?) +$`
* `<mount-point>`: mount path, user-defined, but must be **absolute path**.
* `--host`: Mount the volume to the specified host, user can choose, please make sure the host has been imported.

If the file system is mounted successfully, you can query the corresponding DingoFS file system on **the corresponding host**:

```shell 
$ mount | grep <mount-point> 
```

The user can also check the status of all clients on the central console:

```shell 
$ dingoadm client status 
```

``` 
Get Client Status: [OK]

Id Kind Host Container Id Status Aux Info
-- ---- ---- ------------ ------ -------- 
462d538778ad dingofs client-host1 dfa00fd01ae8 Up 36 hours {"fsname": "test1", "mount_point":"/mnt/ test1"} 
c0d56cfaad14 dingofs client-host2 c1301eff2af0 Up 36 hours {"fsname": "test2", "mount_point":"/mnt/test2"} 
d700e1f6acab dingofs client- host3 62554173a54f Up 36 hours {"fsname": "test3", "mount_point":"/mnt/test3"} 
```


> 📢 **Note:** 
> 
> If dingofs needs to use multiple s3 functions, just modify the configuration of the s3 information in client.yaml for different fs.

### Example 
```shell 
$ dingoadm mount test /mnt/test --host client-host -c client.yaml 
```

Other: unmount the filesystem
---

```shell 
$ dingoadm umount <mount-point> --host client-host 
```

[minio]: https://github.com/minio/minio
[deploy-minio-standalone]: https://docs.min.io/minio/baremetal/installation/deploy-minio-standalone.html
[hosts]: ../hosts.md
[important-config]: ../topology.md
[dingofs-client-conf]: https://github.com/dingodb/dingofs/tree/main/conf/client.conf