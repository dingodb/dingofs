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

#ifndef DINGOFS_SRC_CLIENT_VFS_LEGACY_DATASTREAM_METRIC_H_
#define DINGOFS_SRC_CLIENT_VFS_LEGACY_DATASTREAM_METRIC_H_

#include <bvar/bvar.h>
#include <bvar/passive_status.h>

#include <memory>

#include "options/client/vfs_legacy/data_stream/data_stream_option.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace client {
namespace datastream {

using ::dingofs::utils::TaskThreadPool;

static uint32_t GetQueueSize(void* arg) {
  auto* thread_pool = reinterpret_cast<TaskThreadPool<>*>(arg);
  return thread_pool->QueueSize();
}

class DataStreamMetric {
 public:
  struct AuxMembers {
    std::shared_ptr<TaskThreadPool<>> flush_file_thread_pool;
    std::shared_ptr<TaskThreadPool<>> flush_chunk_thread_pool;
    std::shared_ptr<TaskThreadPool<>> flush_slice_thread_pool;
  };

 public:
  DataStreamMetric(DataStreamOption option, AuxMembers aux_members)
      : metric_("dingofs_data_stream", aux_members) {
    // file
    {
      auto o = option.file_option;
      metric_.flush_file_workers.set_value(o.flush_workers);
      metric_.flush_file_queue_capacity.set_value(o.flush_queue_size);
    }

    // chunk
    {
      auto o = option.chunk_option;
      metric_.flush_chunk_workers.set_value(o.flush_workers);
      metric_.flush_chunk_queue_capacity.set_value(o.flush_queue_size);
    }

    // slice
    {
      auto o = option.slice_option;
      metric_.flush_slice_workers.set_value(o.flush_workers);
      metric_.flush_slice_queue_capacity.set_value(o.flush_queue_size);
    }
  }

  virtual ~DataStreamMetric() = default;

 private:
  struct Metric {
    Metric(const std::string& prefix, AuxMembers aux_members)
        :  // file
          flush_file_workers(prefix, "flush_file_workers", 0),
          flush_file_queue_capacity(prefix, "flush_file_queue_capacity", 0),
          flush_file_pending_tasks(prefix, "flush_file_pending_tasks",
                                   &GetQueueSize,
                                   aux_members.flush_file_thread_pool.get()),
          // chunk
          flush_chunk_workers(prefix, "flush_chunk_workers", 0),
          flush_chunk_queue_capacity(prefix, "flush_chunk_queue_capacity", 0),
          flush_chunk_pending_tasks(prefix, "flush_chunk_pending_tasks",
                                    &GetQueueSize,
                                    aux_members.flush_chunk_thread_pool.get()),
          // slice
          flush_slice_workers(prefix, "flush_slice_workers", 0),
          flush_slice_queue_capacity(prefix, "flush_slice_queue_capacity", 0),
          flush_slice_pending_tasks(prefix, "flush_slice_pending_tasks",
                                    &GetQueueSize,
                                    aux_members.flush_slice_thread_pool.get()) {
    }

    // file
    bvar::Status<uint32_t> flush_file_workers;
    bvar::Status<uint32_t> flush_file_queue_capacity;
    bvar::PassiveStatus<uint32_t> flush_file_pending_tasks;
    // chunk
    bvar::Status<uint32_t> flush_chunk_workers;
    bvar::Status<uint32_t> flush_chunk_queue_capacity;
    bvar::PassiveStatus<uint32_t> flush_chunk_pending_tasks;
    // slice
    bvar::Status<uint32_t> flush_slice_workers;
    bvar::Status<uint32_t> flush_slice_queue_capacity;
    bvar::PassiveStatus<uint32_t> flush_slice_pending_tasks;
    bvar::Status<uint32_t> s3_async_upload_workers;
  };

  Metric metric_;
};

}  // namespace datastream
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_LEGACY_DATASTREAM_METRIC_H_
