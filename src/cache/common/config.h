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
 * Created Date: 2025-02-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_CONFIG_H_
#define DINGOFS_SRC_CACHE_COMMON_CONFIG_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cache/blockcache/block_cache.h"

namespace dingofs {
namespace cache {
namespace common {

struct MdsClientOptions {
}

struct S3ClientOptions {
  // Constructed with default options.
  S3ClientOptions();

  std::string ak;
  std::string sk;
  std::string endpoint;
  std::string bucket_name;
};

struct DiskStateOptions {
  // Constructed with default options.
  DiskStateOptions();

  uint32_t tick_duration_s;
};

struct DiskCacheOptions {
  // Constructed with default options.
  DiskCacheOptions();

  uint32_t index;

  // Directory for store cache block
  // Default: /var/run/dingofs
  std::string cache_dir;

  // Default: 100 (MiB)
  uint64_t cache_size_mb;
};

struct BlockCacheOptions {
  // Constructed with default options.
  BlockCacheOptions();

  std::string cache_store;

  // Default: true
  bool stage;

  bool stage_throttle_enable;

  uint64_t stage_throttle_bandwidth_mb;

  uint32_t flush_file_workers;

  uint32_t flush_file_queue_size;

  uint32_t flush_slice_workers;

  uint32_t flush_slice_queue_size;

  uint64_t upload_stage_workers;

  uint64_t upload_stage_queue_size;

  std::vector<DiskCacheOptions> disks;
};

struct CacheGroupNodeOptions {
  // Constructed with default options.
  CacheGroupNodeOptions();

  std::string group_name;

  std::string listen_ip;

  // Default: 9301
  uint32_t listen_port;

  uint32_t group_weight;

  uint32_t max_range_size_kb;

  std::string metadata_filepath;

  BlockCacheOptions block_cache_options;

  MdsClientOptions mds_client_options;
};

}  // namespace common
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_STATUS_H_
