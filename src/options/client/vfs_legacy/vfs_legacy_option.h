/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DINGOFS_SRC_CLIENT_OPTIONS_VFS_LEGACY_OPTION_H_
#define DINGOFS_SRC_CLIENT_OPTIONS_VFS_LEGACY_OPTION_H_

#include <cstdint>

#include "blockaccess/accesser_common.h"
#include "options/cache/blockcache.h"
#include "options/cache/tiercache.h"
#include "options/client/fuse/fuse_option.h"
#include "options/client/vfs_legacy/data_stream/data_stream_option.h"
#include "stub/common/config.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {

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
};

struct LeaseOpt {
  uint32_t refreshTimesPerLease = 5;
  // default = 20s
  uint32_t leaseTimeUs = 20000000;
};

struct RefreshDataOption {
  uint64_t maxDataSize = 1024;
  uint32_t refreshDataIntervalSec = 30;
};

struct KVClientManagerOpt {
  int setThreadPooln = 4;
  int getThreadPooln = 4;
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
// }

struct FileSystemOption {
  bool cto;
  std::string writeback_suffix;
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

struct VFSLegacyOption {
  stub::common::MdsOption mdsOpt;
  stub::common::MetaCacheOpt metaCacheOpt;
  stub::common::ExcutorOpt excutorOpt;
  stub::common::ExcutorOpt excutorInternalOpt;
  S3ClientAdaptorOption s3_client_adaptor_opt;       // from config
  blockaccess::BlockAccessOptions block_access_opt;  // from config
  LeaseOpt leaseOpt;
  RefreshDataOption refreshDataOption;
  KVClientManagerOpt kvClientManagerOpt;
  FileSystemOption fileSystemOption;
  DataStreamOption data_stream_option;
  cache::BlockCacheOption block_cache_option;
  cache::RemoteBlockCacheOption remote_block_cache_option;
  FuseOption fuse_option;

  uint32_t listDentryLimit;
  uint32_t listDentryThreads;
  uint32_t dummyServerStartPort;
  bool enableMultiMountPointRename = false;
  uint32_t downloadMaxRetryTimes;
  uint32_t warmupThreadsNum = 10;
};

void InitVFSLegacyOption(utils::Configuration* conf, VFSLegacyOption* option);

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_OPTIONS_VFS_LEGACY_OPTION_H_