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
 * Created Date: 2025-05-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_OPTIONS_CLIENT_DATA_H_
#define DINGOFS_SRC_OPTIONS_CLIENT_DATA_H_

#include "options/options.h"

namespace dingofs {
namespace options {
namespace client {

class BackgroundFlushOption : public BaseOption {
  BIND_int32(interval_ms, 1000, "");
  BIND_double(trigger_force_memory_ratio, 0.90, "");
};

class FlushFileOption : public BaseOption {
  BIND_int32(flush_workers, 10, "");
  BIND_int32(flush_queue_size, 500, "");
};

class FlushChunkOption : public BaseOption {
  BIND_int32(flush_workers, 10, "");
  BIND_int32(flush_queue_size, 500, "");
};

class FlushSliceOption : public BaseOption {
  BIND_int32(flush_workers, 10, "");
  BIND_int32(flush_queue_size, 500, "");
  BIND_int32(stay_in_memory_max_s, 5, "");
};

class PageOption : public BaseOption {
  BIND_int32(page_size, 65536, "");
  BIND_int32(total_size_mb, 1024, "");
  BIND_bool(use_pool, true, "");
};

class DataOption : public BaseOption {
  BIND_suboption(background_flush_option, "background_flush",
                 BackgroundFlushOption);
  BIND_suboption(flush_file_option, "flush_file", FlushFileOption);
  BIND_suboption(flush_chunk_option, "flush_chunk", FlushChunkOption);
  BIND_suboption(flush_slice_option, "flush_slice", FlushSliceOption);
  BIND_suboption(page_option, "page", PageOption);
};

}  // namespace client
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CLIENT_DATA_H_
