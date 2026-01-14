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

#include "client/vfs/metasystem/mds/compact.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstdint>

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

const std::string kCompactWorkerSetName = "compact_worker_set";

DEFINE_uint32(compact_worker_num, 4096, "number of compact workers");
DEFINE_uint32(compact_worker_max_pending_num, 259072,
              "compact worker max pending num");
DEFINE_bool(compact_worker_use_pthread, false, "compact worker use pthread");

void CompactChunkTask::Run() {
  auto status = Compact();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}.{}] compact chunk fail, status({}).",
                              ino_, chunk_->GetIndex(), status.ToString());
  }
}

Status CompactChunkTask::Compact() {
  if (!chunk_->IsNeedCompaction()) {
    LOG(INFO) << fmt::format("[meta.fs.{}.{}] chunk no need compact.", ino_,
                             chunk_->GetIndex());
    return Status::OK();
  }

  // do compact

  uint64_t version = 0;
  auto slices = chunk_->GetCommitedSlice(version);

  auto ctx = std::make_shared<Context>("");
  MDSClient::CompactChunkParam param;
  param.version = version;
  param.start_pos = 0;
  param.start_slice_id = slices.front().id;
  param.end_pos = slices.size() - 1;
  param.end_slice_id = slices.back().id;
  // param.new_slices;

  mds::ChunkEntry chunk_entry;
  auto status = mds_client_.CompactChunk(ctx, ino_, chunk_->GetIndex(), param,
                                         chunk_entry);
  if (!status.ok()) return status;

  chunk_->Put(chunk_entry);

  return Status::OK();
}

void CompactChunkTask::CompactCompletelyOverlap() {}

bool CompactProcessor::Init() {
  worker_set_ = mds::ExecqWorkerSet::NewUnique(
      kCompactWorkerSetName, FLAGS_compact_worker_num,
      FLAGS_compact_worker_max_pending_num);

  if (!worker_set_->Init()) {
    LOG(ERROR) << "init compact worker set fail.";
    return false;
  }

  return true;
}

void CompactProcessor::Stop() { worker_set_->Destroy(); }

void CompactProcessor::Execute(TaskRunnablePtr task) {
  if (IsExistTask(task->Key())) return;

  RememberTask(task->Key());
  if (!worker_set_->ExecuteLeastQueue(task)) {
    LOG(WARNING) << "[meta.compact] execute task fail.";
    ForgetTask(task->Key());
  }
}

bool CompactProcessor::IsExistTask(const std::string& key) {
  utils::ReadLockGuard guard(lock_);

  return doing_tasks_.contains(key);
}

void CompactProcessor::RememberTask(const std::string& key) {
  utils::WriteLockGuard guard(lock_);

  doing_tasks_.insert(key);
}
void CompactProcessor::ForgetTask(const std::string& key) {
  utils::WriteLockGuard guard(lock_);

  doing_tasks_.erase(key);
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs