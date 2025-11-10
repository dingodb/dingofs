// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/vfs/metasystem/mds/write_slice_processor.h"

#include <glog/logging.h>
#include <sys/types.h>

#include <cstdint>
#include <vector>

#include "common/options/client.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

static const uint32_t kBatchOperationReserveSize = 256;

WriteSliceProcessor::WriteSliceProcessor(MDSClientSPtr mds_client,
                                         ChunkMemo& chunk_memo)
    : mds_client_(mds_client), chunk_memo_(chunk_memo) {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0)
      << fmt::format("[meta.writeslice] bthread_mutex_init fail.");
  CHECK(bthread_cond_init(&cond_, nullptr) == 0)
      << fmt::format("[meta.writeslice] bthread_cond_init fail.");
}

WriteSliceProcessor::~WriteSliceProcessor() {
  bthread_cond_destroy(&cond_);
  bthread_mutex_destroy(&mutex_);
}

bool WriteSliceProcessor::Init() {
  CHECK(mds_client_ != nullptr)
      << fmt::format("[meta.writeslice] mds_client_ is nullptr.");

  struct Param {
    WriteSliceProcessorSPtr self;
  };

  Param* param = new Param({.self = GetSelfPtr()});

  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  if (bthread_start_background(
          &tid_, &attr,
          [](void* arg) -> void* {
            Param* param = reinterpret_cast<Param*>(arg);

            param->self->ProcessOperation();

            delete param;
            return nullptr;
          },
          param) != 0) {
    tid_ = 0;
    delete param;
    LOG(FATAL) << "[meta.writeslice] start background thread fail.";
    return false;
  }

  return true;
}

bool WriteSliceProcessor::Destroy() {
  is_stop_.store(true);

  if (tid_ > 0) {
    bthread_cond_signal(&cond_);

    if (bthread_stop(tid_) != 0) {
      LOG(ERROR) << fmt::format("[meta.writeslice] bthread_stop fail.");
    }

    if (bthread_join(tid_, nullptr) != 0) {
      LOG(ERROR) << fmt::format("[meta.writeslice] bthread_join fail.");
    }
  }

  return true;
}

bool WriteSliceProcessor::AsyncRun(WriteSliceOperationSPtr operation) {
  if (is_stop_.load(std::memory_order_relaxed)) {
    return false;
  }

  operations_.Enqueue(operation);

  bthread_cond_signal(&cond_);

  return true;
}

void WriteSliceProcessor::ProcessOperation() {
  std::vector<WriteSliceOperationSPtr> stage_operations;
  stage_operations.reserve(kBatchOperationReserveSize);

  while (true) {
    stage_operations.clear();

    WriteSliceOperationSPtr operation;
    while (!operations_.Dequeue(operation) &&
           !is_stop_.load(std::memory_order_relaxed)) {
      bthread_mutex_lock(&mutex_);
      bthread_cond_wait(&cond_, &mutex_);
      bthread_mutex_unlock(&mutex_);
    }

    if (is_stop_.load(std::memory_order_relaxed) && stage_operations.empty()) {
      break;
    }

    if (FLAGS_client_write_slicce_operation_merge_delay_us > 0) {
      bthread_usleep(FLAGS_client_write_slicce_operation_merge_delay_us);
    }

    do {
      stage_operations.push_back(operation);
    } while (operations_.Dequeue(operation));

    auto batch_operation_map = Grouping(stage_operations);
    for (auto& [_, batch_operation] : batch_operation_map) {
      LaunchExecuteBatchOperation(batch_operation);
    }
  }
}

std::map<Ino, BatchOperation> WriteSliceProcessor::Grouping(
    std::vector<WriteSliceOperationSPtr>& operations) {
  std::map<Ino, BatchOperation> batch_operation_map;

  for (const auto& operation : operations) {
    auto it = batch_operation_map.find(operation->ino);
    if (it == batch_operation_map.end()) {
      batch_operation_map[operation->ino] =
          BatchOperation{.ino = operation->ino, .operations = {operation}};
    } else {
      it->second.operations.push_back(operation);
    }
  }

  return batch_operation_map;
}

void WriteSliceProcessor::LaunchExecuteBatchOperation(
    const BatchOperation& batch_operation) {
  struct Params {
    WriteSliceProcessorSPtr self;
    BatchOperation batch_operation;
  };

  Params* params =
      new Params({.self = GetSelfPtr(), .batch_operation = batch_operation});

  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_SMALL;
  if (bthread_start_background(
          &tid, &attr,
          [](void* arg) -> void* {
            Params* params = reinterpret_cast<Params*>(arg);

            params->self->ExecuteBatchOperation(params->batch_operation);

            delete params;

            return nullptr;
          },
          params) != 0) {
    delete params;
    LOG(FATAL) << "[meta.writeslice] start background thread fail.";
  }
}

void WriteSliceProcessor::ExecuteBatchOperation(
    BatchOperation& batch_operation) {
  CHECK(!batch_operation.operations.empty())
      << fmt::format("[meta.writeslice] batch_operation.operations is empty.");

  const Ino ino = batch_operation.ino;
  const uint64_t fh = batch_operation.operations.front()->fh;
  ContextSPtr ctx = batch_operation.operations.front()->ctx;

  // prepare delta slice entries
  std::vector<mds::DeltaSliceEntry> delta_slice_entries;
  for (auto& operation : batch_operation.operations) {
    mds::DeltaSliceEntry delta_slice_entry;

    delta_slice_entry.set_chunk_index(operation->index);
    for (const auto& slice : operation->slices) {
      *delta_slice_entry.add_slices() = Helper::ToSlice(slice);
    }

    delta_slice_entries.push_back(std::move(delta_slice_entry));
  }

  LOG(INFO) << fmt::format(
      "[meta.writeslice.{}.{}] execute batch operation, "
      "delta_slice_entries({}).",
      ino, fh, delta_slice_entries.size());

  std::vector<mds::ChunkDescriptor> chunk_descriptors;
  auto status =
      mds_client_->WriteSlice(ctx, ino, delta_slice_entries, chunk_descriptors);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.writeslice.{}.{}] writeslice fail, error({}).", ino, fh,
        status.ToString());
  }

  // update chunk memo
  for (const auto& chunk_descriptor : chunk_descriptors) {
    chunk_memo_.Remember(ino, chunk_descriptor.index(),
                         chunk_descriptor.version());
  }

  for (auto& operation : batch_operation.operations) {
    operation->done(status);
  }
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs