# Client Architecture
## 1.Summary 
As a DingoFS client, the client uses the rpc interface to send requests to the back-end metadata cluster and data cluster, and calls the back-end interface to realize the corresponding functions. DingoFS client supports S3 compatible object storage.

## 2.Functions
### Provide standard POSIX file system interface 
DingoFS client supports fuse userland file system by interfacing with libfuse's lowlevel fuse api to realize standard POSIX file system interface.

### Support S3 storage engine to store data and cache 
DingoFS client supports to convert file system data into object storage through certain format, and save the file data in S3 storage engine through S3 interface compatible client (using S3 C++ sdk).
The DingoFS client supports two levels of data caching, memory and disk caching, which accelerates the performance of reading and writing S3 data.

### Metadata Fetching and Caching 
The DingoFS client stores file system metadata in the DingoFS metadata cluster. The DingoFS client supports caching metadata on the client side to provide faster metadata access. The cached metadata information includes.
- File system global information, i.e. FsInfo.
- FsInfo. Metadata for each file and directory, including dentry information and inode information.
- The metadata of each file and directory is distributed in Copyset and Partition information, which is cached by the DingoFS client to request metadata operations from the corresponding Copyset and Partition.
- Topology information of metadata clusters, DingoFS client needs to know the topology information of metadata clusters such as ip, port, etc., so as to know which ip, port to send rpc to.

### Exception Handling and Retries for Metadata Requests 
The DingoFS mds and DingoFS metaserver clusters, which enable highly available deployments, where the
- DingoFS mds has only one mds providing service at the same time, and other mds nodes are listening through etcd. When the main mds has an exception, the backup mds can replace the leader at any time, and at this time, the DingoFS client needs to look for the main mds and retry the rpc request to deal with the situation.
- The DingoFS metaserver cluster is a mutiraft cluster that provides services to DingoFS clients, and there are similar scenarios such as leader switching, which requires the DingoFS client to GetLeader and retry rpc requests after switching leaders.
- In addition, the DingoFS client's communication with the above components may also result in rpc timeouts due to network busyness, etc., which also requires the DingoFS client to retry the request.

## 3.Architecture

The DingoFS client consists of several main modules:

- libfuse, which interfaces with its lowlevel fuse api to support the fuse userland filesystem;
- metadata cache, including fsinfo, inode cache, dentry cache, to realize the cache of metadata;
- meta rpc client, mainly interfacing with metadata cluster, realizing meta op sending, timeout retry and other functions;
- S3 client, through the interface to S3, the data will be stored in S3;
- S3 data cache, which is the memory cache layer of the S3 datastore, serves as a data cache to accelerate the read and write performance of S3 data;
- S3 disk cache, which is the local persistent cache of the S3 data store, temporarily caches the reads and writes to the S3 data on the local disk through the disk cache and later uploads them to S3 asynchronously, thus effectively reducing the latency and providing throughput;

## 4.IO Flow 
The IO flow of DingoFS Client is divided into two major parts, which are Meta Data Meta IO flow and Data Data IO flow.

### Meta IO Flow
![](../../images/mknod_flow.png)
The Meta IO flow for DingoFS, in the case of MkNod, consists of the following process:
- The user invokes the file system MkNod interface, which passes through the user-state file system fuse low level api and reaches the DingoFS client interface;
- MkNod needs to perform two operations, CreateInode and CreateDentry, the CreateInode process first needs to decide the partition to create the Inode, usually the topo information and partition information are cached in the DingoFS client, if there is no such information in the cache, then the DingoFS client will not be able to create the Inode, and the DingoFS client will not be able to create the partition. If there is no such information in the cache, the DingoFS client will first go to the mds to get the information;
- Based on the partition information in the cache, the DingoFS client decides to create the Inode's partiton according to a certain policy;
- Based on the topo information and partiton information in the cache or obtained, find the copyset where the Inode needs to be created;
- If the leader information is cached in the copyset, then the CreateInode rpc request can be sent directly to the corresponding metaserver, otherwise, it is necessary to obtain the leader information from any metaserver in the copyset;
- After calling CreateInode's rpc to create the Inode, the next step is to CreateDentry;
- Similarly, the Create Dentry process first needs to decide the partiton for creating the Dentry according to a certain strategy;
- After that, the Create Dentry process finds the copyset where the Dentry is to be created based on the topo information and partiton information in the cache or obtained;
- If the leader information is cached in the copyset, then the CreateDentry rpc request can be sent directly to the corresponding metaserver; otherwise, it needs to get the leader information from any metaserver in the copyset;
- After CreateDentry is completed, the function of MkNod is finished.

### Data IO stream 
![](../../images/s3_dataio_flow.png)
The Data IO flow stored to S3 consists of the following process:
- The user invokes the file system write interface, which passes through the userland file system fuse low level api and reaches the DingoFS client interface;
- The DingoFS client's write interface writes data to the Data Cache first;
- When the DataCache is full or the periodic refresh time arrives, DingoFS client will start the data sync process;
- DingoFS client will first write the data to S3 (if there is a disk cache, it will write the data to the disk cache first, and then asynchronously write the data to S3 later);; After the data is written to S3, DingoFS client will write the data to S3 asynchronously, and then write the data to S3 asynchronously.
- After the data is written to S3, the DingoFS client will record the meta information of the data written to S3 and organize it into S3ChunkInfo;
- If the DingoFS client does not cache the Inode information at this time, then it will follow the metadata flow in the previous section to get the Inode from the metaserver.


- After getting the Inode, DingoFS client will add S3ChunkInfo information to the Inode;
- After completing the local Inode update, DingoFS client will call AppendS3ChunkInfo RPC interface to incrementally update the Inode information on the metaserver side;

## 5.Exception Handling 
The exception handling of the DingoFS client mainly refers to idempotent request returns for various exceptions of the metadata cluster. It mainly involves retrying the rpc requests to the metadata cluster mds and metaserver, which involves including the following functions:
- The rpc request to the mds node, if found that the mds request timeout, then need to retry, if multiple retries fail, then may have switched the main mds, then need to switch the mds to continue to retry;
- Request rpc to the node of the metaserver, if you receive a redirect reply or request timeout, it may be due to the switch of the leader, then you need to re-acquire the leader, and then retry the request;

The above retry process also needs to ensure the idempotency of the request, DingoFS Client for the idempotency of the request to ensure, mainly through the following ways:
- For requests of the delete class, if a NOT EXIST error is returned, then DingoFS Client will directly assume that the deletion has been successful, and thus idempotent execution is successful.
- For requests to mds, such as Mount FS, which do not have high performance requirements, the DingoFS Client first gets the current mount point via Get FsInfo, and then goes to Mount FS. In this way, it is guaranteed that the Mount FS request was not mounted before it was sent, and if it still returns EXIST, then it is certain that the rpc retry request is a successful one. If it still returns EXIST, then it can be sure that it is caused by the rpc retry request, so the idempotent return execution is successful.
- Some other requests, such as CreateDentry, etc., according to the uniqueness of InodeId in the content of the CreateDentry request (the inodeId of the request in the retry CreateDentry is the same, and the inodeId of the request in the non-retry request must be different) to distinguish whether it is a retry request or not, and thus idempotent return is successful, or return the EXIST error.

## 6.Key design
### Rename 
The rename interface of DingoFS borrows from leveldb and etcd(boltdb) to ensure atomicity, and the design is shown below:

![rename.png](../../images/rename.png)
 
- The rename mechanism is designed to add a txid field to all MDS copysets to store the successful transaction ids of the current copyset (the transaction ids are incremented in order, and one is added for each successful transaction);
- At the beginning of each rename, add 1 (copyset_txid+1) to the txid corresponding to the copyset where srcDentry, dstDentry are located to delete/create/modify the dentry (in fact, it is the creation of a copy, no matter delete/create/modify, it is to create a copy of the corresponding copyset_txid+1 is key), the original dentry does not move, the original dentry does not move. The original dentry remains intact, and set PendingTx to the current transaction;
- If the previous step succeeds, submit the transaction, add 1 to the txid of the copyset where srcDentry, dstDentry are located (this step is guaranteed by the transaction of etcd), if the previous step fails or this step fails, since the txid is unchanged, and the original version of the data is also there, or to ensure the atomicity (in fact, a txid corresponds to a version of the data);
- The next time you access the copyset, bring the latest txid (copyset_txid) of the corresponding copyset, judge the PendingTx, if (copyset_txid >= PendingTxId && rpc_request.key == PendingTxKey), the transaction corresponding to PendingTx has been successful, if the transaction corresponding to PendingTx happens to operate the requested dentry, return the copy dentry corresponding to PendingTxKey + PendingTxId; otherwise, return the original dentry; otherwise, return the original dentry. If the transaction corresponding to PendingTx happens to operate on the requested dentry, the copy of the dentry corresponding to PendingTxKey + PendingTxId is returned, otherwise the original dentry is returned;
- PendingTx and dentry copy are one-to-one correspondence, each copyset only needs one PendingTx (i.e., at most one copy dentry will be kept in the whole copyset);

DingoFS client's rename mechanism finally realizes the atomicity of the whole Rename by committing the transaction to etcd atomically on the mds side.