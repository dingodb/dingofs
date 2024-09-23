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
 * Created Date: 2024-09-18
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_DATASTREAM_METRIC_H_
#define CURVEFS_SRC_CLIENT_DATASTREAM_METRIC_H_

#include <bvar/bvar.h>
#include <bvar/passive_status.h>

#include <memory>

#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/datastream/page_allocator.h"
#include "src/common/concurrent/task_thread_pool.h"

namespace curvefs {
namespace client {
namespace datastream {

using ::curve::common::TaskThreadPool;
using ::curvefs::client::common::DataStreamOption;

static uint32_t GetQueueSize(void* arg) {
  auto* thread_pool = reinterpret_cast<TaskThreadPool<>*>(arg);
  return thread_pool->QueueSize();
}

static uint64_t GetFreePages(void* arg) {
  auto* page_allocator = reinterpret_cast<PageAllocator*>(arg);
  return page_allocator->GetFreePages();
}

class DataStreamMetric {
 public:
  struct AuxMembers {
    std::shared_ptr<TaskThreadPool<>> flush_file_thread_pool;
    std::shared_ptr<TaskThreadPool<>> flush_chunk_thread_pool;
    std::shared_ptr<TaskThreadPool<>> flush_slice_thread_pool;
    std::shared_ptr<PageAllocator> page_allocator;
  };

 public:
  DataStreamMetric(DataStreamOption option, AuxMembers aux_members)
      : metric_("dingofs_data_stream", aux_members) {
    // file
    {
      auto o = option.file_option;
      metric_.flush_file_workers.set_value(o.flush_workers);
      metric_.flush_file_queue_size.set_value(o.flush_queue_size);
    }

    // chunk
    {
      auto o = option.chunk_option;
      metric_.flush_chunk_workers.set_value(o.flush_workers);
      metric_.flush_chunk_queue_size.set_value(o.flush_queue_size);
    }

    // slice
    {
      auto o = option.slice_option;
      metric_.flush_slice_workers.set_value(o.flush_workers);
      metric_.flush_slice_queue_size.set_value(o.flush_queue_size);
    }

    // page
    {
      auto o = option.page_option;
      metric_.use_page_pool.set_value(o.use_pool);
    }
  }

  virtual ~DataStreamMetric() = default;

 private:
  struct Metric {
    Metric(const std::string& prefix, AuxMembers aux_members)
        : flush_file_pending_tasks(prefix, "flush_file_pending_tasks",
                                   &GetQueueSize,
                                   aux_members.flush_file_thread_pool.get()),
          flush_file_workers(prefix, "flush_file_workers", 0),
          flush_file_queue_size(prefix, "flush_file_queue_capacity", 0),
          flush_chunk_pending_tasks(prefix, "flush_chunk_pending_tasks",
                                    &GetQueueSize,
                                    aux_members.flush_chunk_thread_pool.get()),
          flush_chunk_workers(prefix, "flush_chunk_workers", 0),
          flush_chunk_queue_size(prefix, "flush_chunk_queue_size", 0),
          flush_slice_pending_tasks(prefix, "flush_slice_pending_tasks",
                                    &GetQueueSize,
                                    aux_members.flush_slice_thread_pool.get()),
          flush_slice_workers(prefix, "flush_slice_workers", 0),
          flush_slice_queue_size(prefix, "flush_slice_queue_size", 0),
          use_page_pool(prefix, "use_page_pool", false),
          free_pages(prefix, "free_pages", &GetFreePages,
                     aux_members.page_allocator.get()) {}

    bvar::PassiveStatus<uint32_t> flush_file_pending_tasks;
    bvar::Status<uint32_t> flush_file_workers;
    bvar::Status<uint32_t> flush_file_queue_size;
    bvar::PassiveStatus<uint32_t> flush_chunk_pending_tasks;
    bvar::Status<uint32_t> flush_chunk_workers;
    bvar::Status<uint32_t> flush_chunk_queue_size;
    bvar::PassiveStatus<uint32_t> flush_slice_pending_tasks;
    bvar::Status<uint32_t> flush_slice_workers;
    bvar::Status<uint32_t> flush_slice_queue_size;
    bvar::Status<uint32_t> s3_async_upload_workers;
    bvar::Status<bool> use_page_pool;
    bvar::PassiveStatus<uint64_t> free_pages;
  };

  Metric metric_;
};

}  // namespace datastream
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_DATASTREAM_METRIC_H_
