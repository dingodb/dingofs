/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_DATASTREAM_DATA_STREAM_H_
#define DINGOFS_SRC_CLIENT_DATASTREAM_DATA_STREAM_H_

#include <cassert>
#include <functional>
#include <memory>

#include "client/datastream/metric.h"
#include "options/client/options/common_option.h"
#include "client/datastream/page_allocator.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace client {
namespace datastream {

using ::dingofs::utils::TaskThreadPool;

class DataStream {
  using TaskFunc = std::function<void()>;

 public:
  static DataStream& GetInstance() {
    static DataStream instance;
    return instance;
  }

 public:
  bool Init(DataStreamOption option);

  void Shutdown();

  void EnterFlushFileQueue(TaskFunc task);

  void EnterFlushChunkQueue(TaskFunc task);

  void EnterFlushSliceQueue(TaskFunc task);

  char* NewPage();

  void FreePage(char* p);

  bool MemoryNearFull();

 private:
  std::shared_ptr<TaskThreadPool<>> flush_file_thread_pool_;
  std::shared_ptr<TaskThreadPool<>> flush_chunk_thread_pool_;
  std::shared_ptr<TaskThreadPool<>> flush_slice_thread_pool_;
  std::shared_ptr<PageAllocator> page_allocator_;
  std::unique_ptr<DataStreamMetric> metric_;
  DataStreamOption option_;
};

}  // namespace datastream
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_DATASTREAM_DATA_STREAM_H_
