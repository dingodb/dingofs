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

#include "fmt/format.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

const std::string kCompactWorkerSetName = "compact_worker_set";

DEFINE_uint32(vfs_compact_worker_num, 8, "number of compact workers");
DEFINE_uint32(vfs_compact_worker_max_pending_num, 256,
              "compact worker max pending num");
DEFINE_bool(vfs_compact_worker_use_pthread, true, "compact worker use pthread");

constexpr size_t kCompactedVersionMemoMaxSize = 1024 * 1024;  // 1M

void CompactChunkTask::Run() {
  if (compact_processor_.IsStopped() || IsDeleted()) {
    LOG(INFO) << fmt::format(
        "[meta.compact.{}.{}.{}] compact chunk task is stopped or deleted.",
        ino_, chunk_->GetIndex(), Id());

    Signal();
    return;
  }

  auto status = Compact();
  if (!status.ok() && !status.IsNotFit() && !status.IsStop()) {
    LOG(ERROR) << fmt::format(
        "[meta.compact.{}.{}.{}] compact chunk fail, status({}).", ino_,
        chunk_->GetIndex(), Id(), status.ToString());
  }

  status_ = status;

  Signal();
}

void CompactChunkTask::TryCleanupUncommittedSlices(
    const std::vector<Slice>& old_slices,
    const std::vector<Slice>& new_slices) {
  // Cleanup uncommitted slices that are not in the new_slices list
  std::vector<Slice> pure_new_slices;
  for (const auto& slice : new_slices) {
    if (slice.id == 0) continue;

    bool is_old_slice = false;
    for (const auto& old_slice : old_slices) {
      if (slice.id == old_slice.id) {
        is_old_slice = true;
        break;
      }
    }
    if (!is_old_slice) pure_new_slices.push_back(slice);
  }

  if (pure_new_slices.empty()) return;

  ContextSPtr ctx = std::make_shared<Context>("");
  Status status = compactor_.CleanupUncommittedSlices(ctx, pure_new_slices);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.compact.{}.{}.{}] cleanup slices fail, status({}).", ino_,
        chunk_->GetIndex(), Id(), status.ToString());

  } else {
    LOG_DEBUG << fmt::format(
        "[meta.compact.{}.{}.{}] cleanup slices success, slices({}).", ino_,
        chunk_->GetIndex(), Id(), Helper::ToString(pure_new_slices));
  }
}

Status CompactChunkTask::Compact() {
  const uint32_t chunk_index = chunk_->GetIndex();

  auto status = chunk_->IsNeedCompaction(false);
  if (!status.ok()) return status;

  // do compact
  uint64_t version = 0;
  std::vector<Slice> old_slices = chunk_->GetCommitedSlice(version);
  if (old_slices.empty()) return Status::OK();

  // check version
  uint64_t compacted_version =
      compact_processor_.GetCompactedVersion(ino_, chunk_index);
  if (version <= compacted_version) {
    return Status::NotFit(
        fmt::format("stale version, {}<={}", version, compacted_version));
  }

  std::vector<Slice> new_slices;
  ContextSPtr ctx = std::make_shared<Context>("");
  status = compactor_.Compact(ctx, ino_, chunk_index, old_slices, new_slices);
  if (!status.ok()) return status;
  if (new_slices.empty()) return Status::NotFit("all slices skipped");
  if (IsDeleted()) {
    TryCleanupUncommittedSlices(old_slices, new_slices);
    return Status::OK();
  }

  MDSClient::CompactChunkParam param;
  param.version = version;
  param.start_pos = 0;
  param.start_slice_id = old_slices.front().id;
  param.end_pos = old_slices.size() - 1;
  param.end_slice_id = old_slices.back().id;
  for (auto& slice : new_slices) {
    param.new_slices.push_back(Helper::ToSlice(slice));
  }

  mds::ChunkEntry chunk_entry;
  status = mds_client_.CompactChunk(ctx, ino_, chunk_->GetIndex(), param,
                                    chunk_entry);
  if (!status.ok() && !status.IsTimeout() && !status.IsNetError() &&
      !status.IsIoError()) {
    TryCleanupUncommittedSlices(old_slices, new_slices);
  }

  if (status.IsTimeout()) {
    chunk_->SetNotCompleted();

  } else if (status.ok() || status.IsInvalidParam()) {
    bool extra_local_compact = false;
    if (chunk_entry.version() > version &&
        !chunk_->Put(chunk_entry, "compact")) {
      if (status.ok()) {
        extra_local_compact =
            chunk_->Compact(param.start_pos, param.start_slice_id,
                            param.end_pos, param.end_slice_id, new_slices);
      }
    }

    compact_processor_.UpdateComapctedVersion(ino_, chunk_index,
                                              chunk_entry.version());

    LOG(INFO) << fmt::format(
        "[meta.compact.{}.{}.{}] do compact chunk finish, version({}->{}) "
        "old_slice({}|{}|{}) new_slices({}) final_slices({}) extra({}) "
        "status({}).",
        ino_, chunk_index, Id(), version, chunk_entry.version(),
        param.start_slice_id, param.end_slice_id, old_slices.size(),
        Helper::ToString(new_slices), chunk_entry.slices_size(),
        extra_local_compact, status.ToString());
  }

  return status;
}

CompactProcessor::CompactProcessor()
    : executor_(kCompactWorkerSetName, FLAGS_vfs_compact_worker_num,
                FLAGS_vfs_compact_worker_max_pending_num,
                FLAGS_vfs_compact_worker_use_pthread) {}

bool CompactProcessor::Init() { return executor_.Init(); }

void CompactProcessor::Stop() {
  is_stopped_.store(true);

  executor_.Stop();
}

Status CompactProcessor::LaunchCompact(Ino ino, InodeSPtr inode,
                                       ChunkSPtr& chunk, MDSClient& mds_client,
                                       Compactor& compactor, bool is_async) {
  auto task =
      CompactChunkTask::New(ino, inode, chunk, mds_client, compactor, *this);

  int64_t hash_id = ino + chunk->GetIndex();
  if (!executor_.ExecuteByHash(hash_id, task, false)) {
    LOG(WARNING) << fmt::format(
        "[meta.compact.{}.{}] commit compact task fail, beyond max pending "
        "num.",
        ino, chunk->GetIndex());

    return Status::Internal("commit compact task fail, beyond max pending num");
  }

  if (!is_async) {
    task->Wait();

    return task->GetStatus();
  }

  return Status::OK();
}

uint64_t CompactProcessor::GetCompactedVersion(Ino ino, uint32_t chunk_index) {
  utils::ReadLockGuard guard(lock_);

  const std::string key = fmt::format("{}:{}", ino, chunk_index);
  auto it = compacted_version_memo_.find(key);
  return (it != compacted_version_memo_.end()) ? it->second.version : 0;
}

void CompactProcessor::UpdateComapctedVersion(Ino ino, uint32_t chunk_index,
                                              uint64_t version) {
  utils::WriteLockGuard guard(lock_);

  const std::string key = fmt::format("{}:{}", ino, chunk_index);
  auto [it, inserted] = compacted_version_memo_.try_emplace(
      key, Value{.version = version, .last_active_time_s = utils::Timestamp()});
  if (!inserted && it->second.version < version) {
    it->second.version = version;
    it->second.last_active_time_s = utils::Timestamp();
  }
}

void CompactProcessor::CleanExpired(uint64_t expire_time_s) {
  utils::WriteLockGuard guard(lock_);

  if (compacted_version_memo_.size() < kCompactedVersionMemoMaxSize) return;

  for (auto it = compacted_version_memo_.begin();
       it != compacted_version_memo_.end();) {
    if (it->second.last_active_time_s < expire_time_s) {
      auto tmp = it++;
      compacted_version_memo_.erase(tmp);

    } else {
      ++it;
    }
  }
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
