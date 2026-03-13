# DINGOFS Change Log
All notable changes to this project are documented in this file.

## [5.1.0]

**Client**
- Support storing tiny file data directly to the metadata system
- Show FUSE connection info in client dashboard
- Show chunk cache stats on monitoring page
- Add block cache statistics to dashboard
- Add `client_access_logging_verbose` flag for verbose access logging
- Improve getxattr performance via inode cache
- Support prefetching one or more blocks ahead
- Support FUSE read zero-copy
- Submit prefetch tasks asynchronously
- Refactored VFS block prefetch logic
- Refactored warmup: progress now measured by blocks instead of files
- Tuned write path, random read, and readahead performance
- Refactored umount client logic
- Removed unix socket path gflag; changed inode_blocks_service gflags to const
- Fixed FUSE attr/entry cache timeout configuration
- Fixed read-after-write across multiple file descriptors
- Fixed flush-all-slice assert failure
- Fixed compact chunk and compact timeout issues
- Fixed chunk version assert failure
- Fixed file session assert failure
- Fixed file close / invalidate deadlock
- Fixed async open/close issues
- Fixed readslice issues
- Fixed ctime/mtime exception
- Fixed prefetch: block limit was capped at 4G; fixed missing blocks in prefetch
- Fixed read exception causing file truncation
- Fixed chunk cache update causing data corruption
- Fixed coredump when building meta fails
- Fixed `dingo-fuse` hang when mounting a filesystem in deleted state
- Fixed read file hang when using `fuse_reply_iov` with oversized iov
- Fixed flush sequence incorrectness under concurrent write/flush
- Fixed read incorrect data issue
- Fixed coredump in read path

**BlockCache**
- Improve cache read/write performance
- Use direct I/O for all file operations and optimize io_uring usage
- Optimized connection pooling with fixed connection pool
- Support move for IOBuffer
- Fix `--cache_dir` parameter not supporting semicolon-separated directories
- Fix `--conf` parameter not taking effect in certain cases
- Fix `--free_space_ratio` parameter not working
- Fix cache node unable to exit properly
- Fix incorrect cache size displayed on startup
- Fix block not cleaned up when prefetch fails
- Add storage upload retry timeout policy
- Log errors when cache node fails to start

**Metadata Service (MDS)**
- Support TiKV async transaction
- Optimize store operation schedule thread
- Support client RPC failover
- Support file and directory read cache at FUSE layer
- Support getting directory quota for `statfs`
- Support renaming deleted filesystem name to avoid conflict with new FS of same name
- Support validating queue wait timeout
- Support showing MDS cache summary
- Add trace support for dingo-sdk transactions
- Add `update fs s3/rados info` command
- Add IP field for mountpoint info
- Add client retry for `ESTORE_MAYBE_RETRY`
- Fixed TiKV scan out-of-range issues
- Fixed TiKV transaction lost update
- Fixed batch delete block exceeding max value (1000)
- Fixed compact chunk issues (using old file length)
- Fixed readdir issues
- Fixed fs tree view display issues on page
- Fixed WorkerSet queue wait/run latency metrics
- Fixed store operation known issues

**Common / General**
- Support POSIX ioctl
- Support automatically cleaning logs
- Redirect standard input/output log path in daemonize mode
- Change default runtime directory to `/var` when run as root
- Add block access benchmark tool
- Add Rados configuration support
- Refactored project layout and config modules
- Add open telemetry trace
- Fixed inconsistency between command-line help and log file values for flags
- Fixed wrong gflags current value shown in log file
- Fixed coredump when wrapper is destroyed

**Monitoring**
- Add slice metrics
- Fix wrong in-flight operation count metrics
- Restructure dingo monitor commands
- Use snake_case parameters in result response
- Fix monitor script exception when no target is configured
- Add `mdsaddr` flag for `target_json.py`
- Add cluster performance dashboard

## [5.0.0]

**Client**
- Complete redesign and reimplementation of the client architecture
- Support for seamless FUSE upgrades without service interruption
- New monitoring dashboard integrated into the client interface
- Added caching capabilities for dentry, inode, and chunk metadata
- Supports S3 and Ceph Rados as persistent storage
    
**BlockCache**
- Introduced distributed caching system
- Added asynchronous operation interfaces to the cache system, enhancing I/O concurrency
- Integrated `io_uring` support for high-efficiency file read/write operations, reducing system call overhead
   
**Metadata Service (MDS)**
- The Metadata Service (MDS) has been completely refactored with full support for POSIX semantics
- Support for both Mono (single-partition) and Hash partition strategies for metadata distribution
- Implementation of distributed locking mechanism
- Built-in fault recovery capabilities
- Support for filesystem-level and directory-level quotas
- Internal operational status visualization for improved maintenance and debugging
- Automatic compression and organization of chunks
- Backup and restore functionality for filesystem metadata
- Dynamic addition and removal of MDS nodes
- Support for dingo-store as backend metadata storage engine
- Unified management of distributed cache nodes

**Monitoring & Observability**
- Native integration with Grafana and Prometheus for comprehensive monitoring
- Added monitoring for distributed cache and new metadata services
- New tracing module implemented
