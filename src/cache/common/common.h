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
 * Created Date: 2025-05-10
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_H_
#define DINGOFS_SRC_CACHE_COMMON_H_

#include <options/cache/app.h>

#include "common/io_buffer.h"
#include "common/status.h"
#include "dingofs/blockcache.pb.h"
#include "dingofs/cachegroup.pb.h"
#include "dingofs/mds.pb.h"
#include "options/cache/block_cache.h"
#include "options/cache/cache_group_node.h"

namespace dingofs {
namespace cache {

using IOBuffer = dingofs::common::IOBuffer;

using dingofs::options::cache::AppOption;               // NOLINT
using dingofs::options::cache::BlockCacheOption;        // NOLINT
using dingofs::options::cache::CacheGroupNodeOption;    // NOLINT
using dingofs::options::cache::DiskCacheOption;         // NOLINT
using dingofs::options::cache::RemoteBlockCacheOption;  // NOLINT
using dingofs::options::cache::RemoteNodeOption;        // NOLINT

using dingofs::options::cache::FLAGS_block_cache_logging;  // NOLINT
using dingofs::options::cache::
    FLAGS_block_cache_stage_bandwidth_throttle_enable;  // NOLINT
using dingofs::options::cache::
    FLAGS_block_cache_stage_bandwidth_throttle_mb;  // NOLINT

using dingofs::options::cache::
    FLAGS_disk_cache_cleanup_expire_interval_ms;                   // NOLINT
using dingofs::options::cache::FLAGS_disk_cache_drop_page_cache;   // NOLINT
using dingofs::options::cache::FLAGS_disk_cache_expire_s;          // NOLINT
using dingofs::options::cache::FLAGS_disk_cache_free_space_ratio;  // NOLINT

using dingofs::options::cache::
    FLAGS_disk_state_disk_check_duration_ms;  // NOLINT
using dingofs::options::cache::
    FLAGS_disk_state_normal2unstable_io_error_num;                // NOLINT
using dingofs::options::cache::FLAGS_disk_state_tick_duration_s;  // NOLINT
using dingofs::options::cache::FLAGS_disk_state_unstable2down_s;  // NOLINT
using dingofs::options::cache::
    FLAGS_disk_state_unstable2normal_io_succ_num;  // NOLINT

using dingofs::pb::cache::blockcache::BlockCacheErrCode;       // NOLINT
using dingofs::pb::cache::blockcache::BlockCacheErrCode_Name;  // NOLINT
using dingofs::pb::cache::blockcache::BlockCacheErrFailure;    // NOLINT
using dingofs::pb::cache::blockcache::BlockCacheErrIOError;    // NOLINT
using dingofs::pb::cache::blockcache::BlockCacheErrNotFound;   // NOLINT
using dingofs::pb::cache::blockcache::BlockCacheErrUnknown;    // NOLINT
using dingofs::pb::cache::blockcache::BlockCacheOk;            // NOLINT

using dingofs::pb::mds::FSStatusCode;  // NOLINT

BlockCacheErrCode PbErr(Status status);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_H_
