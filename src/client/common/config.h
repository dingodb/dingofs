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

// { block cache option
struct DiskCacheOption {
  uint32_t index;
  std::string cache_dir;
  uint64_t cache_size;  // bytes
};

struct BlockCacheOption {
  std::string cache_store;
  bool stage;
  bool stage_throttle_enable;
  uint64_t stage_throttle_bandwidth_mb;
  uint32_t flush_file_workers;
  uint32_t flush_file_queue_size;
  uint32_t flush_slice_workers;
  uint32_t flush_slice_queue_size;
  uint64_t upload_stage_workers;
  uint64_t upload_stage_queue_size;
  std::vector<DiskCacheOption> disk_cache_options;
};
// }

}  // namespace common
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_COMMON_CONFIG_H_
