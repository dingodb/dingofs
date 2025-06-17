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

#ifndef DINGOFS_SRC_CLIENT_OPTIONS_DATA_STREAM_OPTION_H_
#define DINGOFS_SRC_CLIENT_OPTIONS_DATA_STREAM_OPTION_H_

#include "common/const.h"
#include "options/client/memory/page_option.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {

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

// TODO: refacto  this, sepecrate the background_flush_option with page option
struct DataStreamOption {
  BackgroundFlushOption background_flush_option;
  FileOption file_option;
  ChunkOption chunk_option;
  SliceOption slice_option;
  PageOption page_option;
};
// }

static void InitDataStreamOption(utils::Configuration* c,
                                 DataStreamOption* option) {
  {  // background flush option
    auto* o = &option->background_flush_option;
    c->GetValueFatalIfFail(
        "data_stream.background_flush.trigger_force_memory_ratio",
        &o->trigger_force_memory_ratio);
  }
  {  // file option
    auto* o = &option->file_option;
    c->GetValueFatalIfFail("data_stream.file.flush_workers", &o->flush_workers);
    c->GetValueFatalIfFail("data_stream.file.flush_queue_size",
                           &o->flush_queue_size);
  }
  {  // chunk option
    auto* o = &option->chunk_option;
    c->GetValueFatalIfFail("data_stream.chunk.flush_workers",
                           &o->flush_workers);
    c->GetValueFatalIfFail("data_stream.chunk.flush_queue_size",
                           &o->flush_queue_size);
  }
  {  // slice option
    auto* o = &option->slice_option;
    c->GetValueFatalIfFail("data_stream.slice.flush_workers",
                           &o->flush_workers);
    c->GetValueFatalIfFail("data_stream.slice.flush_queue_size",
                           &o->flush_queue_size);
  }
  {  // page option
    InitMemoryPageOption(c, &option->page_option);

    double trigger_force_flush_memory_ratio =
        option->background_flush_option.trigger_force_memory_ratio;
    if (option->page_option.total_size *
            (1.0 - trigger_force_flush_memory_ratio) <
        32 * kMiB) {
      CHECK(false) << "Please gurantee the free memory size greater than 32MiB "
                      "before force flush.";
    }
  }
}

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_OPTIONS_DATA_STREAM_OPTION_H_