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

#include "client/vfs/metasystem/mds/batch_processor.h"

#include <glog/logging.h>

#include <memory>

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

static const uint32_t kBatchOperationReserveSize = 256;

void WriteSliceOperation::BatchRun(MDSClient& mds_client,
                                   BatchOperation& batch_operation) {
  const Ino ino = batch_operation.ino;

  CHECK(batch_operation.type == Operation::OpType::kWriteSlice) << fmt::format(
      "not match batch_operation type({}), ino({}) expect writeslice.",
      static_cast<uint32_t>(batch_operation.type), ino);

  // cast type
  std::vector<WriteSliceOperationSPtr> operations;
  operations.reserve(batch_operation.operations.size());
  for (auto& operation : batch_operation.operations) {
    WriteSliceOperationSPtr write_slice_operation =
        std::dynamic_pointer_cast<WriteSliceOperation>(operation);

    operations.push_back(write_slice_operation);
  }

  // prepare params
  std::vector<mds::DeltaSliceEntry> delta_slice_entries;
  for (auto& operation : operations) {
    operation->PreHandle(delta_slice_entries);
  }

  LOG(INFO) << fmt::format(
      "[meta.batch_processor.{}] execute batch operation, "
      "delta_slice_entries({}).",
      ino, delta_slice_entries.size());

  std::vector<mds::ChunkDescriptor> chunk_descriptors;
  auto ctx = operations[0]->GetContext();
  if (ctx == nullptr) ctx = std::make_shared<Context>("");
  auto status =
      mds_client.WriteSlice(ctx, ino, delta_slice_entries, chunk_descriptors);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.batch_processor.{}] writeslice fail, error({}).", ino,
        status.ToString());
  }

  for (auto& operation : operations) {
    operation->Done(status, chunk_descriptors);
  }
}

void WriteSliceOperation::PreHandle(
    std::vector<mds::DeltaSliceEntry>& delta_slice_entries) {
  auto delta_slices = task->DeltaSlices();

  for (const auto& delta_slice : delta_slices) {
    mds::DeltaSliceEntry delta_slice_entry;
    delta_slice_entry.set_chunk_index(delta_slice.chunk_index);

    for (const auto& slice : delta_slice.slices) {
      *delta_slice_entry.add_slices() = Helper::ToSlice(slice);
    }

    delta_slice_entries.push_back(std::move(delta_slice_entry));
  }
}

void MkNodOperation::BatchRun(MDSClient& mds_client,
                              BatchOperation& batch_operation) {
  const Ino parent = batch_operation.ino;

  CHECK(batch_operation.type == Operation::OpType::kMkNod) << fmt::format(
      "not match batch_operation type({}), ino({}) expect mknod.",
      static_cast<uint32_t>(batch_operation.type), parent);

  // cast type
  std::vector<MkNodOperationSPtr> operations;
  operations.reserve(batch_operation.operations.size());
  for (auto& operation : batch_operation.operations) {
    MkNodOperationSPtr mknod_operation =
        std::dynamic_pointer_cast<MkNodOperation>(operation);

    operations.push_back(mknod_operation);
  }

  // prepare params
  auto ctx = operations[0]->GetContext();
  std::vector<MDSClient::MkNodParam> params;
  for (const auto& operation : operations) {
    MDSClient::MkNodParam param;
    param.name = operation->name;
    param.uid = operation->uid;
    param.gid = operation->gid;
    param.mode = operation->mode;
    param.rdev = operation->rdev;

    params.push_back(std::move(param));
  }
  std::vector<AttrEntry> attr_entries;
  AttrEntry parent_attr_entry;

  auto status = mds_client.BatchMkNod(ctx, parent, params, attr_entries,
                                      parent_attr_entry);

  if (!status.ok()) {
    for (const auto& operation : operations) {
      operation->SetStatus(status);
    }

  } else {
    CHECK(attr_entries.size() == operations.size())
        << fmt::format("attr_entries size({}) not match operations size({}).",
                       attr_entries.size(), operations.size());
    // set result
    for (size_t i = 0; i < operations.size(); ++i) {
      operations[i]->SetResult(std::move(attr_entries[i]), parent_attr_entry);
    }
  }

  // notify done
  for (auto& operation : operations) operation->NotifyEvent();
}

void MkDirOperation::BatchRun(MDSClient& mds_client,
                              BatchOperation& batch_operation) {
  const Ino parent = batch_operation.ino;

  CHECK(batch_operation.type == Operation::OpType::kMkDir) << fmt::format(
      "not match batch_operation type({}), ino({}) expect mkdir.",
      static_cast<uint32_t>(batch_operation.type), parent);

  // cast type
  std::vector<MkDirOperationSPtr> operations;
  operations.reserve(batch_operation.operations.size());
  for (auto& operation : batch_operation.operations) {
    MkDirOperationSPtr mkdir_operation =
        std::dynamic_pointer_cast<MkDirOperation>(operation);

    operations.push_back(mkdir_operation);
  }

  // prepare params
  auto ctx = operations[0]->GetContext();
  std::vector<MDSClient::MkDirParam> params;
  for (const auto& operation : operations) {
    MDSClient::MkDirParam param;
    param.name = operation->name;
    param.uid = operation->uid;
    param.gid = operation->gid;
    param.mode = operation->mode;

    params.push_back(std::move(param));
  }

  std::vector<AttrEntry> attr_entries;
  AttrEntry parent_attr_entry;
  auto status = mds_client.BatchMkDir(ctx, parent, params, attr_entries,
                                      parent_attr_entry);

  if (!status.ok()) {
    for (const auto& operation : operations) {
      operation->SetStatus(status);
    }

  } else {
    CHECK(attr_entries.size() == operations.size())
        << fmt::format("attr_entries size({}) not match operations size({}).",
                       attr_entries.size(), operations.size());
    // set result
    for (size_t i = 0; i < operations.size(); ++i) {
      operations[i]->SetResult(std::move(attr_entries[i]), parent_attr_entry);
    }
  }

  // notify done
  for (auto& operation : operations) operation->NotifyEvent();
}

void UnlinkOperation::BatchRun(MDSClient& mds_client,
                               BatchOperation& batch_operation) {
  const Ino parent = batch_operation.ino;

  CHECK(batch_operation.type == Operation::OpType::kUnlink) << fmt::format(
      "not match batch_operation type({}), ino({}) expect unlink.",
      static_cast<uint32_t>(batch_operation.type), parent);

  // cast type
  std::vector<UnlinkOperationSPtr> operations;
  operations.reserve(batch_operation.operations.size());
  for (auto& operation : batch_operation.operations) {
    UnlinkOperationSPtr unlink_operation =
        std::dynamic_pointer_cast<UnlinkOperation>(operation);
    operations.push_back(unlink_operation);
  }

  // prepare params
  auto ctx = operations[0]->GetContext();
  std::vector<std::string> names;
  names.reserve(operations.size());
  for (const auto& operation : operations) {
    names.push_back(operation->name);
  }

  std::vector<AttrEntry> attr_entries;
  AttrEntry parent_attr_entry;
  auto status = mds_client.BatchUnLink(ctx, parent, names, attr_entries,
                                       parent_attr_entry);
  if (!status.ok()) {
    for (const auto& operation : operations) {
      operation->SetStatus(status);
    }

  } else {
    CHECK(attr_entries.size() == operations.size())
        << fmt::format("attr_entries size({}) not match operations size({}).",
                       attr_entries.size(), operations.size());
    // set result
    for (size_t i = 0; i < operations.size(); ++i) {
      operations[i]->SetResult(std::move(attr_entries[i]), parent_attr_entry);
    }
  }

  // notify done
  for (auto& operation : operations) operation->NotifyEvent();
}

BatchProcessor::BatchProcessor(MDSClient& mds_client)
    : mds_client_(mds_client) {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0)
      << fmt::format("[meta.batch_processor] bthread_mutex_init fail.");
  CHECK(bthread_cond_init(&cond_, nullptr) == 0)
      << fmt::format("[meta.batch_processor] bthread_cond_init fail.");
}

BatchProcessor::~BatchProcessor() {
  bthread_cond_destroy(&cond_);
  bthread_mutex_destroy(&mutex_);
}

bool BatchProcessor::Init() {
  struct Param {
    BatchProcessor& self;
  };

  Param* param = new Param({.self = *this});

  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  if (bthread_start_background(
          &tid_, &attr,
          [](void* arg) -> void* {
            Param* param = reinterpret_cast<Param*>(arg);

            param->self.ProcessOperation();

            delete param;
            return nullptr;
          },
          param) != 0) {
    tid_ = 0;
    delete param;
    LOG(FATAL) << "[meta.batch_processor] start background thread fail.";
    return false;
  }

  return true;
}

bool BatchProcessor::Destroy() {
  is_stop_.store(true);

  if (tid_ > 0) {
    bthread_cond_signal(&cond_);

    if (bthread_stop(tid_) != 0) {
      LOG(ERROR) << fmt::format("[meta.batch_processor] bthread_stop fail.");
    }

    if (bthread_join(tid_, nullptr) != 0) {
      LOG(ERROR) << fmt::format("[meta.batch_processor] bthread_join fail.");
    }
  }

  return true;
}

bool BatchProcessor::AsyncRun(OperationSPtr operation) {
  if (is_stop_.load(std::memory_order_relaxed)) {
    return false;
  }

  operations_.Enqueue(operation);

  bthread_cond_signal(&cond_);

  return true;
}

bool BatchProcessor::RunBatched(OperationSPtr operation) {
  if (is_stop_.load(std::memory_order_relaxed)) {
    return false;
  }

  operations_.Enqueue(operation);

  bthread_cond_signal(&cond_);
  return true;
}

void BatchProcessor::ProcessOperation() {
  std::vector<OperationSPtr> stage_operations;
  stage_operations.reserve(kBatchOperationReserveSize);

  OperationSPtr operation;
  while (true) {
    operation = nullptr;
    stage_operations.clear();

    while (!operations_.Dequeue(operation) &&
           !is_stop_.load(std::memory_order_relaxed)) {
      bthread_mutex_lock(&mutex_);
      bthread_cond_wait(&cond_, &mutex_);
      bthread_mutex_unlock(&mutex_);
    }

    if (operation) stage_operations.push_back(operation);

    if (is_stop_.load(std::memory_order_relaxed) && stage_operations.empty()) {
      break;
    }

    bool is_waited = false;
    do {
      if (!operations_.Dequeue(operation)) break;

      stage_operations.push_back(operation);

      if (!is_waited && FLAGS_vfs_meta_batch_operation_merge_delay_us > 0) {
        bthread_usleep(FLAGS_vfs_meta_batch_operation_merge_delay_us);
        is_waited = true;
      }

    } while (true);

    auto batch_operation_map = Grouping(stage_operations);
    for (auto& [_, batch_operation] : batch_operation_map) {
      LaunchExecuteBatchOperation(std::move(batch_operation));
    }
  }

  // print pending operations
  while (operations_.Dequeue(operation)) {
    LOG(INFO) << fmt::format(
        "[meta.batch_processor] pending operation type({}) ino({}).",
        operation->OpName(), operation->GetIno());
  }
}

std::map<BatchProcessor::Key, BatchOperation> BatchProcessor::Grouping(
    std::vector<OperationSPtr>& operations) {
  std::map<Key, BatchOperation> batch_operation_map;

  for (const auto& operation : operations) {
    Key key{.type = operation->GetOpType(), .ino = operation->GetIno()};
    auto it = batch_operation_map.find(key);
    if (it == batch_operation_map.end()) {
      batch_operation_map[key] = BatchOperation{
          .ino = key.ino, .type = key.type, .operations = {operation}};
    } else {
      it->second.operations.push_back(operation);
    }
  }

  return batch_operation_map;
}

void BatchProcessor::LaunchExecuteBatchOperation(
    BatchOperation&& batch_operation) {
  struct Params {
    MDSClient& mds_client;
    BatchOperation batch_operation;
  };

  Params* params = new Params({.mds_client = mds_client_,
                               .batch_operation = std::move(batch_operation)});

  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_SMALL;
  if (bthread_start_background(
          &tid, &attr,
          [](void* arg) -> void* {
            Params* params = reinterpret_cast<Params*>(arg);

            ExecuteBatchOperation(params->mds_client, params->batch_operation);

            delete params;

            return nullptr;
          },
          params) != 0) {
    delete params;
    LOG(FATAL) << "[meta.batch_processor] start background thread fail.";
  }
}

void BatchProcessor::ExecuteBatchOperation(MDSClient& mds_client,
                                           BatchOperation& batch_operation) {
  CHECK(!batch_operation.operations.empty()) << fmt::format(
      "[meta.batch_processor] batch_operation.operations is empty.");

  switch (batch_operation.type) {
    case Operation::OpType::kWriteSlice: {
      WriteSliceOperation::BatchRun(mds_client, batch_operation);
    } break;
    case Operation::OpType::kMkDir: {
      MkDirOperation::BatchRun(mds_client, batch_operation);
    } break;
    case Operation::OpType::kMkNod: {
      MkNodOperation::BatchRun(mds_client, batch_operation);
    } break;
    case Operation::OpType::kUnlink: {
      UnlinkOperation::BatchRun(mds_client, batch_operation);
    } break;
    default: {
      LOG(FATAL) << fmt::format(
          "[meta.batch_processor] unknown batch_operation type({}).",
          Operation::OpName(batch_operation.type));
    }
  }
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs