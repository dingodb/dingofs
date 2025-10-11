# DINGOFS Change Log
All notable changes to this project are documented in this file.

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
