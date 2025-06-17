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

/*
 * Project: DingoFS
 * Created Date: 2025-06-03
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_CONST_H_
#define DINGOFS_SRC_CACHE_COMMON_CONST_H_

#include <cstdint>
#include <string>

namespace dingofs {
namespace cache {

static constexpr uint64_t kKiB = 1024ULL;
static constexpr uint64_t kMiB = 1024ULL * kKiB;
static constexpr uint64_t kGiB = 1024ULL * kMiB;
static constexpr uint64_t kTiB = 1024ULL * kGiB;

// module name
static const std::string kTierBlockCacheMoudule = "tiercache";
static const std::string kRmoteBlockCacheMoudule = "remotecache";
static const std::string kServiceModule = "service";
static const std::string kCacheGroupNodeModule = "cachenode";
static const std::string kBlockCacheMoudule = "blockcache";
static const std::string kBlockCacheUploaderMoudule = "uploader";
static const std::string kDiskCacheMoudule = "diskcache";
static const std::string kFileSysteMoudle = "filesystem";
static const std::string kAioModule = "aio";
static const std::string kPageCacheModule = "pagecache";
static const std::string kStorageMoudule = "storage";

// step: up-level module, like tier cache and cache group node service
static const std::string kLocalPut = "local_put";
static const std::string kRemotePut = "remote_put";
static const std::string kLocalRange = "local_range";
static const std::string kRemoteRange = "remote_range";
static const std::string kLocalCache = "local_cache";
static const std::string kRemoteCache = "remote_cache";
static const std::string kLocalPrefetch = "local_prefetch";
static const std::string kRemotePrefetch = "remote_prefetch";
static const std::string kNodePut = "node_put";
static const std::string kNodeRange = "node_range";
static const std::string kNodeCache = "node_cache";
static const std::string kNodePrefetch = "node_prefetch";
static const std::string kSendResponse = "send_response";

// step: disk cache
static const std::string kStageBlock = "stage";
static const std::string kRemoveStageBlock = "removestage";
static const std::string kCacheBlock = "cache";
static const std::string kLoadBlock = "load";
static const std::string kCacheAdd = "cache_add";

// step: filesystem
static const std::string kMkDir = "mkdir";
static const std::string kOpenFile = "open";
static const std::string kCreatFile = "creat";
static const std::string kWriteFile = "write";
static const std::string kReadFile = "read";
static const std::string kLinkFile = "link";
static const std::string kRemoveFile = "unlink";
static const std::string kRenameFile = "rename";
static const std::string kMmap = "mmap";

// step: aio
static const std::string kAioWrite = "aio_write";
static const std::string kAioRead = "aio_read";
static const std::string kWaitThrottle = "throttle";
static const std::string kCheckIo = "check";
static const std::string kPrepareIO = "prepare";
static const std::string kExecuteIO = "execute";
static const std::string kRunClosure = "closure";
static const std::string kDropPageCache = "dropcache";

// step: storage
static const std::string kGetStorage = "get_storage";
static const std::string kS3Put = "s3_put";
static const std::string kS3Range = "s3_range";
static const std::string kS3Get = "s3_get";

// step: others
static const std::string kAsyncCache = "async_cache";
static const std::string kEnqueue = "enqueue";

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_CONST_H_
