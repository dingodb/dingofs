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

#ifndef DINGOFS_SRC_CLIENT_COMMON_CONFIG_H_
#define DINGOFS_SRC_CLIENT_COMMON_CONFIG_H_

#include <cstdint>
#include <string>

#include "blockaccess/accesser_common.h"
#include "cache/config/config.h"
#include "cache/config/remote_cache.h"
#include "dingofs/common.pb.h"
#include "stub/common/config.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {
namespace common {

// { data stream option
struct BackgroundFlushOption {
  double trigger_force_memory_ratio;
};

struct FileOption {
  uint64_t flush_workers;
  uint64_t flush_queue_size;
};

struct SliceOption {
  uint64_t flush_workers;
  uint64_t flush_queue_size;
};

struct ChunkOption {
  uint64_t flush_workers;
  uint64_t flush_queue_size;
};

struct PageOption {
  uint64_t page_size;
  uint64_t total_size;
  bool use_pool;
};

struct DataStreamOption {
  BackgroundFlushOption background_flush_option;
  FileOption file_option;
  ChunkOption chunk_option;
  SliceOption slice_option;
  PageOption page_option;
};
// }

struct LeaseOpt {
  uint32_t refreshTimesPerLease = 5;
  // default = 20s
  uint32_t leaseTimeUs = 20000000;
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
};

struct RefreshDataOption {
  uint64_t maxDataSize = 1024;
  uint32_t refreshDataIntervalSec = 30;
};

// { fuse module option
struct FuseConnInfo {
  bool want_splice_move;
  bool want_splice_read;
  bool want_splice_write;
  bool want_auto_inval_data;
};

struct FuseFileInfo {
  bool keep_cache;
};

struct FuseOption {
  FuseConnInfo conn_info;
  FuseFileInfo file_info;
};
// }

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

struct UdsOption {
  std::string fd_comm_path;
};
// }

struct ClientOption {
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

void InitClientOption(utils::Configuration* conf, ClientOption* client_option);

void RewriteCacheDir(cache::BlockCacheOption* option, std::string uuid);

void InitUdsOption(utils::Configuration* conf, UdsOption* uds_option);

}  // namespace common
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_COMMON_CONFIG_H_
