/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef DINGOFS_SRC_CLIENT_VFS_OLD_COMMON_CONFIG_H_
#define DINGOFS_SRC_CLIENT_VFS_OLD_COMMON_CONFIG_H_

#include <cstdint>
#include <string>

#include "aws/s3_adapter.h"
#include "client/common/config.h"
#include "dingofs/common.pb.h"
#include "stub/common/config.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {
namespace common {

using ::dingofs::client::common::BlockCacheOption;
using ::dingofs::client::common::DataStreamOption;

struct BlockDeviceClientOptions {
  std::string configPath;
};

struct LeaseOpt {
  uint32_t refreshTimesPerLease = 5;
  // default = 20s
  uint32_t leaseTimeUs = 20000000;
};

struct SpaceAllocServerOption {
  std::string spaceaddr;
  uint64_t rpcTimeoutMs;
};

struct KVClientManagerOpt {
  int setThreadPooln = 4;
  int getThreadPooln = 4;
};

struct S3ClientAdaptorOption {
  uint64_t blockSize;
  uint64_t chunkSize;
  uint64_t pageSize;
  uint32_t prefetchBlocks;
  uint32_t prefetchExecQueueNum;
  uint32_t intervalMs;
  uint32_t flushIntervalSec;
  uint64_t writeCacheMaxByte;
  uint64_t readCacheMaxByte;
  uint32_t readCacheThreads;
  uint32_t nearfullRatio;
  uint32_t baseSleepUs;
  uint32_t maxReadRetryIntervalMs;
  uint32_t readRetryIntervalMs;
  uint32_t objectPrefix;
};

struct S3Option {
  S3ClientAdaptorOption s3ClientAdaptorOpt;
  aws::S3AdapterOption s3AdaptrOpt;
};

struct BlockGroupOption {
  uint32_t allocateOnce;
};

struct BitmapAllocatorOption {
  uint64_t sizePerBit;
  double smallAllocProportion;
};

struct VolumeAllocatorOption {
  std::string type;
  BitmapAllocatorOption bitmapAllocatorOption;
  BlockGroupOption blockGroupOption;
};

struct VolumeOption {
  uint64_t bigFileSize;
  uint64_t volBlockSize;
  uint64_t fsBlockSize;
  VolumeAllocatorOption allocatorOption;
};

struct ExtentManagerOption {
  uint64_t preAllocSize;
};

struct RefreshDataOption {
  uint64_t maxDataSize = 1024;
  uint32_t refreshDataIntervalSec = 30;
};

// { filesystem option
struct KernelCacheOption {
  uint32_t entryTimeoutSec;
  uint32_t dirEntryTimeoutSec;
  uint32_t attrTimeoutSec;
  uint32_t dirAttrTimeoutSec;
};

struct LookupCacheOption {
  uint64_t lruSize;
  uint32_t negativeTimeoutSec;
  uint32_t minUses;
};

struct DirCacheOption {
  uint64_t lruSize;
  uint32_t timeoutSec;
};

struct AttrWatcherOption {
  uint64_t lruSize;
};

struct OpenFilesOption {
  uint64_t lruSize;
  uint32_t deferSyncSecond;
};

struct RPCOption {
  uint32_t listDentryLimit;
};

struct DeferSyncOption {
  uint32_t delay;
  bool deferDirMtime;
};

struct FileSystemOption {
  bool cto;
  std::string nocto_suffix;
  bool disableXAttr;
  uint32_t maxNameLength;
  uint32_t blockSize = 0x10000u;
  KernelCacheOption kernelCacheOption;
  LookupCacheOption lookupCacheOption;
  DirCacheOption dirCacheOption;
  OpenFilesOption openFilesOption;
  AttrWatcherOption attrWatcherOption;
  RPCOption rpcOption;
  DeferSyncOption deferSyncOption;
};
// }

struct FuseClientOption {
  stub::common::MdsOption mdsOpt;
  stub::common::MetaCacheOpt metaCacheOpt;
  stub::common::ExcutorOpt excutorOpt;
  stub::common::ExcutorOpt excutorInternalOpt;
  SpaceAllocServerOption spaceOpt;
  BlockDeviceClientOptions bdevOpt;
  S3Option s3Opt;
  ExtentManagerOption extentManagerOpt;
  VolumeOption volumeOpt;
  LeaseOpt leaseOpt;
  RefreshDataOption refreshDataOption;
  KVClientManagerOpt kvClientManagerOpt;
  FileSystemOption fileSystemOption;
  DataStreamOption data_stream_option;
  BlockCacheOption block_cache_option;

  uint32_t listDentryLimit;
  uint32_t listDentryThreads;
  uint32_t dummyServerStartPort;
  bool enableMultiMountPointRename = false;
  bool enableFuseSplice = false;
  uint32_t downloadMaxRetryTimes;
  uint32_t warmupThreadsNum = 10;
};

void InitFuseClientOption(utils::Configuration* conf,
                          FuseClientOption* clientOption);

void SetFuseClientS3Option(FuseClientOption* clientOption,
                           const aws::S3InfoOption& fsS3Opt);

void S3Info2FsS3Option(const pb::common::S3Info& s3,
                       aws::S3InfoOption* fsS3Opt);

void InitLeaseOpt(utils::Configuration* conf, LeaseOpt* leaseOpt);

void RewriteCacheDir(BlockCacheOption* option, std::string uuid);

}  // namespace common
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_OLD_COMMON_CONFIG_H_
