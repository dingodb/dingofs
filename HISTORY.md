# DingoFS Change Log

## v3.0.0

### New Features
- Support multi-disk cache
- Support disk health check
- Support disk replacement in the fly
- Support disk cache expire evit policy
- Support enable nocto for specified file
- Support quota for filesystem or directory
- Add fuse client performance metrics
- Support query s3 object for specified file

### Optimization
- Optimize file storage format
- Optimize disk capacity manage policy
- Replace bazel with cmake
- CICD auto compile and image publish

### Bug Fix
- Fix data inconsistency issue
- Fix incorrect header setting in s3 range  request
- Fix segment fault and deadlock issues

### Others
- Upgrade major third-party libraries such as brpc, braft, and rocksdb to the latest versions
