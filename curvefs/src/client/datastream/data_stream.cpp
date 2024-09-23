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
 * Created Date: 2024-09-17
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/datastream/data_stream.h"

#include <glog/logging.h>

#include <cassert>
#include <cstring>

#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/datastream/metric.h"
#include "curvefs/src/client/datastream/page_allocator.h"

namespace curvefs {
namespace client {
namespace datastream {

bool DataStream::Init(DataStreamOption option) {
  option_ = option;

  // file
  {
    auto o = option.file_option;
    flush_file_thread_pool_ =
        std::make_shared<TaskThreadPool<>>("flush_file_worker");
    auto rc =
        flush_file_thread_pool_->Start(o.flush_workers, o.flush_queue_size);
    if (rc != 0) {
      LOG(ERROR) << "Start flush file thread pool failed, rc = " << rc;
      return false;
    }
  }

  // chunk
  {
    auto o = option.chunk_option;
    flush_chunk_thread_pool_ =
        std::make_shared<TaskThreadPool<>>("flush_chunk_worker");
    auto rc =
        flush_chunk_thread_pool_->Start(o.flush_workers, o.flush_queue_size);
    if (rc != 0) {
      LOG(ERROR) << "Start flush chunk thread pool failed, rc = " << rc;
      return false;
    }
  }

  // slice
  {
    auto o = option.slice_option;
    flush_slice_thread_pool_ =
        std::make_shared<TaskThreadPool<>>("flush_slice_worker");
    auto rc =
        flush_slice_thread_pool_->Start(o.flush_workers, o.flush_queue_size);
    if (rc != 0) {
      LOG(ERROR) << "Start flush slice thread pool failed, rc = " << rc;
      return false;
    }
  }

  // page
  {
    auto o = option.page_option;
    if (o.use_pool) {
      page_allocator_ = std::make_shared<PagePool>();
    } else {
      page_allocator_ = std::make_shared<DefaultPageAllocator>();
    }

    auto ok = page_allocator_->Init(o.page_size, o.total_size / o.page_size);
    if (!ok) {
      LOG(ERROR) << "Init page allocator failed.";
      return false;
    }
  }

  // metric
  auto aux_members = DataStreamMetric::AuxMembers{
      .flush_file_thread_pool = flush_file_thread_pool_,
      .flush_chunk_thread_pool = flush_chunk_thread_pool_,
      .flush_slice_thread_pool = flush_slice_thread_pool_,
      .page_allocator = page_allocator_,
  };
  metric_ = std::make_unique<DataStreamMetric>(option, aux_members);
  return true;
}

void DataStream::Shutdown() {
  flush_file_thread_pool_->Stop();
  flush_chunk_thread_pool_->Stop();
  flush_slice_thread_pool_->Stop();
}

void DataStream::EnterFlushFileQueue(TaskFunc task) {
  flush_file_thread_pool_->Enqueue(task);
}

void DataStream::EnterFlushChunkQueue(TaskFunc task) {
  flush_chunk_thread_pool_->Enqueue(task);
}

void DataStream::EnterFlushSliceQueue(TaskFunc task) {
  flush_slice_thread_pool_->Enqueue(task);
}

char* DataStream::NewPage() { return page_allocator_->Allocate(); }

void DataStream::FreePage(char* page) { page_allocator_->DeAllocate(page); }

bool DataStream::MemoryNearFull() {
  double trigger_force_memory_ratio =
      option_.background_flush_option.trigger_force_memory_ratio;
  uint64_t page_size = option_.page_option.page_size;
  uint64_t total_size = option_.page_option.total_size;
  bool is_full = page_allocator_->GetFreePages() * page_size <=
                 total_size * (1.0 - trigger_force_memory_ratio);
  return is_full;
}

}  // namespace datastream
}  // namespace client
}  // namespace curvefs
