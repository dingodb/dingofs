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

#include "client/vfs/metasystem/mds/chunk.h"

#include <atomic>
#include <cstdint>
#include <vector>

#include "boost/range/algorithm/remove_if.hpp"
#include "client/vfs/metasystem/mds/helper.h"
#include "common/options/client.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "json/value.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

static const uint32_t kChunkCommitIntervalMs = 1000;  // milliseconds

static std::atomic<uint64_t> task_id_generator{10000};

void WriteMemo::AddRange(uint64_t offset, uint64_t size) {
  ranges_.emplace_back(Range{.start = offset, .end = offset + size});
  last_time_ns_ = utils::TimestampNs();
}

uint64_t WriteMemo::GetLength() {
  uint64_t length = 0;
  for (const auto& range : ranges_) {
    length = std::max(length, range.end);
  }

  return length;
}

// output json format string
bool WriteMemo::Dump(Json::Value& value) {
  // dump ranges
  Json::Value range_items = Json::arrayValue;
  for (const auto& range : ranges_) {
    Json::Value range_item;
    range_item["start"] = range.start;
    range_item["end"] = range.end;
    range_items.append(range_item);
  }
  value["ranges"] = range_items;

  value["last_time_ns"] = last_time_ns_;

  return true;
}

bool WriteMemo::Load(const Json::Value& value) {
  if (value.isNull()) return true;
  if (!value.isObject()) {
    LOG(ERROR) << "[meta.filesession] write_memo is not object.";
    return false;
  }

  if (!value["ranges"].isNull()) {
    if (!value["ranges"].isArray()) {
      LOG(ERROR) << "[meta.filesession] write_memo.ranges is not array.";
      return false;
    }

    for (const auto& range_item : value["ranges"]) {
      Range range;
      range.start = range_item["start"].asUInt64();
      range.end = range_item["end"].asUInt64();
      ranges_.push_back(range);
    }
  }

  last_time_ns_ = value["last_time_ns"].asUInt64();

  return true;
}

void Chunk::Put(const ChunkEntry& chunk) {
  CHECK(chunk.index() == index_)
      << fmt::format("[meta.chunk.{}.{}] mismatch chunk index({}|{}).", ino_,
                     index_, index_, chunk.index());

  utils::WriteLockGuard guard(lock_);

  is_completed_ = true;

  if (chunk.version() <= commited_version_) return;

  commited_slices_.clear();
  for (const auto& slice : chunk.slices()) {
    commited_slices_.push_back(Helper::ToSlice(slice));
  }

  // delete repetition delta slices
  if (!commiting_slices_.empty()) {
    auto new_end = boost::range::remove_if(
        commiting_slices_, [this](const Slice& commiting_slice) {
          for (auto& slice : commited_slices_) {
            if (commiting_slice.id == slice.id) {
              return true;
            }
          }
          return false;
        });
    commiting_slices_.erase(new_end, commiting_slices_.end());
  }

  commited_version_ = chunk.version();
  last_compaction_time_ms_ = chunk.last_compaction_time_ms();
}

void Chunk::AppendSlice(const std::vector<Slice>& slices) {
  utils::WriteLockGuard guard(lock_);

  stage_slices_.insert(stage_slices_.end(), slices.begin(), slices.end());
}

bool Chunk::IsCompleted() {
  utils::ReadLockGuard guard(lock_);

  return is_completed_;
}

bool Chunk::HasStage() {
  utils::ReadLockGuard guard(lock_);

  return !stage_slices_.empty();
}

bool Chunk::HasCommitting() {
  utils::ReadLockGuard guard(lock_);

  return !commiting_slices_.empty();
}

std::vector<Slice> Chunk::CommitSlice() {
  utils::WriteLockGuard guard(lock_);

  if (!commiting_slices_.empty()) return {};

  uint32_t max_num = FLAGS_vfs_meta_commit_slice_max_num;
  if (stage_slices_.size() < max_num) {
    commiting_slices_.swap(stage_slices_);
  } else {
    commiting_slices_.insert(commiting_slices_.end(), stage_slices_.begin(),
                             stage_slices_.begin() + max_num);
    stage_slices_.erase(stage_slices_.begin(), stage_slices_.begin() + max_num);
  }

  return commiting_slices_;
}

void Chunk::MarkCommited(uint64_t version) {
  utils::WriteLockGuard guard(lock_);

  if (version <= commited_version_) {
    commiting_slices_.clear();
    return;
  }

  commited_version_ = version;

  commited_slices_.insert(commited_slices_.end(), commiting_slices_.begin(),
                          commiting_slices_.end());
  commiting_slices_.clear();
}

uint64_t Chunk::GetVersion() {
  utils::ReadLockGuard guard(lock_);

  return commited_version_;
}

std::vector<Slice> Chunk::GetAllSlice() {
  utils::ReadLockGuard lk(lock_);

  std::vector<Slice> slices;
  slices.reserve(commited_slices_.size() + commiting_slices_.size() +
                 stage_slices_.size());

  slices.insert(slices.end(), commited_slices_.begin(), commited_slices_.end());
  slices.insert(slices.end(), commiting_slices_.begin(),
                commiting_slices_.end());
  slices.insert(slices.end(), stage_slices_.begin(), stage_slices_.end());

  // todo: remove duplicate slices

  return slices;
}

// output json format string
bool Chunk::Dump(Json::Value& value) {
  Json::Value stage_slices_items = Json::arrayValue;
  for (const auto& slice : stage_slices_) {
    stage_slices_items.append(Helper::DumpSlice(slice));
  }

  value["stage_slices"] = stage_slices_items;

  Json::Value commiting_slices_items = Json::arrayValue;
  for (const auto& slice : commiting_slices_) {
    stage_slices_items.append(Helper::DumpSlice(slice));
  }

  value["commiting_slices"] = commiting_slices_items;

  Json::Value commited_slices_items = Json::arrayValue;
  for (const auto& slice : commited_slices_) {
    commited_slices_items.append(Helper::DumpSlice(slice));
  }

  value["commited_slices"] = commited_slices_items;

  value["is_completed"] = is_completed_;
  value["commited_version"] = commited_version_;
  value["last_compaction_time_ms"] = last_compaction_time_ms_;

  return true;
}

bool Chunk::Load(const Json::Value& value) {
  if (value.isNull()) return true;

  if (!value.isObject()) {
    LOG(ERROR) << fmt::format("[meta.chunk.{}.{}] chunk is not object.", ino_,
                              index_);
    return false;
  }

  // load stage_slices
  const auto& stage_slices_value = value["stage_slices"];
  if (!stage_slices_value.isArray()) {
    LOG(ERROR) << fmt::format("[meta.chunk.{}.{}] stage_slices is not array.",
                              ino_, index_);
    return false;
  }

  for (const auto& slice_item : stage_slices_value) {
    stage_slices_.push_back(Helper::LoadSlice(slice_item));
  }

  // load commiting_slices
  const auto& commiting_slices_value = value["commiting_slices"];
  if (!commiting_slices_value.isArray()) {
    LOG(ERROR) << fmt::format(
        "[meta.chunk.{}.{}] commiting_slices is not array.", ino_, index_);
    return false;
  }

  for (const auto& slice_item : commiting_slices_value) {
    commiting_slices_.push_back(Helper::LoadSlice(slice_item));
  }

  // load commited_slices
  const auto& commited_slices_value = value["commited_slices"];
  if (!commited_slices_value.isArray()) {
    LOG(ERROR) << fmt::format(
        "[meta.chunk.{}.{}] commited_slices is not array.", ino_, index_);
    return false;
  }

  for (const auto& slice_item : commited_slices_value) {
    commited_slices_.push_back(Helper::LoadSlice(slice_item));
  }

  is_completed_ = value["is_completed"].asBool();
  commited_version_ = value["commited_version"].asUInt64();
  last_compaction_time_ms_ = value["last_compaction_time_ms"].asUInt64();

  return true;
}

// output json format string
bool CommitTask::Dump(Json::Value& value) {
  value["task_id"] = task_id_;

  Json::Value delta_slices_items = Json::arrayValue;
  for (const auto& delta_slice : delta_slices_) {
    Json::Value delta_slice_item = Json::objectValue;
    delta_slice_item["chunk_index"] = delta_slice.chunk_index;

    Json::Value slices_items = Json::arrayValue;
    for (const auto& slice : delta_slice.slices) {
      slices_items.append(Helper::DumpSlice(slice));
    }
    delta_slice_item["slices"] = slices_items;

    delta_slices_items.append(delta_slice_item);
  }
  value["delta_slices"] = delta_slices_items;

  value["state"] = static_cast<uint32_t>(state_);
  value["status"] = status_.ok() ? "OK" : status_.ToString();
  value["retries"] = retries_.load();

  return true;
}

bool CommitTask::Load(const Json::Value& value) {
  if (value.isNull()) return true;

  if (!value.isObject()) {
    LOG(ERROR) << "[meta.commit_task] commit_task is not object.";
    return false;
  }

  state_ = static_cast<State>(value["state"].asUInt());
  if (value["status"].asString() != "OK") {
    status_ = Status::Internal(value["status"].asString());
  }
  retries_.store(value["retries"].asUInt());

  return true;
}

void ChunkSet::AddWriteMemo(uint64_t offset, uint64_t size) {
  utils::WriteLockGuard lk(lock_);

  write_memo_.AddRange(offset, size);
}

uint64_t ChunkSet::GetLength() {
  utils::ReadLockGuard lk(lock_);

  return write_memo_.GetLength();
}

uint64_t ChunkSet::GetLastWriteTimeNs() {
  utils::ReadLockGuard lk(lock_);

  return write_memo_.LastTimeNs();
}

void ChunkSet::Append(uint32_t index, const std::vector<Slice>& slices) {
  utils::WriteLockGuard guard(lock_);

  auto it = chunk_map_.find(index);
  if (it != chunk_map_.end()) {
    auto& chunk = it->second;
    chunk->AppendSlice(slices);

  } else {
    auto chunk = Chunk::New(ino_, index);
    chunk->AppendSlice(slices);
    chunk_map_.emplace(index, chunk);
  }
}

void ChunkSet::Put(const std::vector<ChunkEntry>& chunks) {
  utils::WriteLockGuard guard(lock_);

  for (const auto& chunk : chunks) {
    auto it = chunk_map_.find(chunk.index());
    if (it != chunk_map_.end()) {
      it->second->Put(chunk);
    } else {
      chunk_map_.emplace(chunk.index(), Chunk::New(ino_, chunk));
    }
  }
}

bool ChunkSet::HasStage() {
  utils::ReadLockGuard guard(lock_);

  for (const auto& [index, chunk] : chunk_map_) {
    if (chunk->HasStage()) {
      return true;
    }
  }

  return false;
}

bool ChunkSet::HasCommitting() {
  utils::ReadLockGuard guard(lock_);

  for (const auto& [index, chunk] : chunk_map_) {
    if (chunk->HasCommitting()) {
      return true;
    }
  }

  return false;
}

uint32_t ChunkSet::TryCommitSlice(bool is_force) {
  uint64_t now_ms = utils::TimestampMs();
  if (!is_force &&
      now_ms < (last_commit_time_ms_.load(std::memory_order_relaxed) +
                kChunkCommitIntervalMs)) {
    return 0;
  }

  last_commit_time_ms_.store(now_ms, std::memory_order_relaxed);

  utils::WriteLockGuard guard(lock_);

  uint32_t count = 0, total_count = 0;
  std::vector<CommitTask::DeltaSlice> delta_slices;
  delta_slices.reserve(1024);
  for (auto& [index, chunk] : chunk_map_) {
    auto slices = chunk->CommitSlice();
    if (slices.empty()) continue;

    CommitTask::DeltaSlice delta_slice;
    delta_slice.chunk_index = index;
    delta_slice.slices = std::move(slices);
    count += delta_slice.slices.size();
    total_count += delta_slice.slices.size();

    delta_slices.push_back(std::move(delta_slice));

    if (count > FLAGS_vfs_meta_commit_slice_max_num) {
      CreateCommitTask(std::move(delta_slices));
      delta_slices.clear();
      count = 0;
    }
  }

  if (!delta_slices.empty()) {
    CreateCommitTask(std::move(delta_slices));
  }

  return total_count;
}

void ChunkSet::MarkCommited(
    const std::vector<ChunkDescriptor>& chunk_descriptors) {
  utils::ReadLockGuard guard(lock_);

  for (const auto& chunk_descriptor : chunk_descriptors) {
    auto it = chunk_map_.find(chunk_descriptor.index());
    if (it != chunk_map_.end()) {
      it->second->MarkCommited(chunk_descriptor.version());
    }
  }
}

bool ChunkSet::HasCommitTask() {
  utils::ReadLockGuard guard(lock_);

  return !commit_task_list_.empty();
}

CommitTaskSPtr ChunkSet::CreateCommitTask(
    std::vector<CommitTask::DeltaSlice>&& delta_slices) {
  auto task = std::make_shared<CommitTask>(task_id_generator.fetch_add(1),
                                           std::move(delta_slices));

  commit_task_list_.push_back(task);

  return task;
}

void ChunkSet::DeleteCommitTask(uint64_t task_id) {
  utils::WriteLockGuard guard(lock_);

  commit_task_list_.remove_if([task_id](const CommitTaskSPtr& task) {
    return task->TaskID() == task_id;
  });
}

std::vector<CommitTaskSPtr> ChunkSet::ListCommitTask() {
  utils::ReadLockGuard guard(lock_);

  std::vector<CommitTaskSPtr> tasks;
  tasks.reserve(commit_task_list_.size());

  for (const auto& task : commit_task_list_) {
    tasks.push_back(task);
  }

  return tasks;
}

uint64_t ChunkSet::GetVersion(uint32_t index) {
  utils::ReadLockGuard guard(lock_);

  auto it = chunk_map_.find(index);
  if (it != chunk_map_.end()) {
    return it->second->GetVersion();
  }

  return 0;
}

std::vector<std::pair<uint32_t, uint64_t>> ChunkSet::GetAllVersion() {
  utils::ReadLockGuard guard(lock_);

  std::vector<std::pair<uint32_t, uint64_t>> versions;

  for (auto& [index, chunk] : chunk_map_) {
    versions.emplace_back(index, chunk->GetVersion());
  }

  return versions;
}

bool ChunkSet::Exist(uint32_t index) {
  utils::ReadLockGuard guard(lock_);

  return chunk_map_.find(index) != chunk_map_.end();
}

ChunkSPtr ChunkSet::Get(uint32_t index) {
  utils::ReadLockGuard guard(lock_);

  auto it = chunk_map_.find(index);
  return (it != chunk_map_.end()) ? it->second : nullptr;
}

// output json format string
bool ChunkSet::Dump(Json::Value& value) {
  value["ino"] = ino_;

  // dump write_memo
  Json::Value write_memo_value = Json::objectValue;
  write_memo_.Dump(write_memo_value);
  value["write_memo"] = write_memo_value;

  // dump chunk_map
  Json::Value chunk_items = Json::arrayValue;
  for (const auto& [index, chunk] : chunk_map_) {
    Json::Value chunk_value = Json::objectValue;
    chunk_value["index"] = index;
    chunk->Dump(chunk_value);
    chunk_items.append(chunk_value);
  }
  value["chunk_map"] = chunk_items;

  // dump commit_task_list
  Json::Value commit_task_list_items = Json::arrayValue;
  for (const auto& task : commit_task_list_) {
    Json::Value task_value = Json::objectValue;
    task->Dump(task_value);
    commit_task_list_items.append(task_value);
  }

  value["id_generator"] = task_id_generator.load();
  value["last_commit_time_ms"] = last_commit_time_ms_.load();

  return true;
}

bool ChunkSet::Load(const Json::Value& value) {
  if (value.isNull()) return true;
  if (!value.isObject()) {
    LOG(ERROR) << "[meta.chunkset] chunkset is not object.";
    return false;
  }

  // load write_memo
  if (!value["write_memo"].isNull()) {
    if (!write_memo_.Load(value["write_memo"])) {
      LOG(ERROR) << "[meta.chunkset] load write_memo fail.";
      return false;
    }
  }

  // load chunk_map
  const auto& chunk_map_value = value["chunk_map"];
  if (!chunk_map_value.isArray()) {
    LOG(ERROR) << "[meta.chunkset] chunk_map is not array.";
    return false;
  }

  for (const auto& chunk_item : chunk_map_value) {
    uint32_t index = chunk_item["index"].asUInt();
    auto chunk = Chunk::New(ino_, index);
    if (!chunk->Load(chunk_item)) {
      LOG(ERROR) << "[meta.chunkset] load chunk fail.";
      return false;
    }
    chunk_map_.emplace(index, chunk);
  }

  // load commit_task_list
  const auto& commit_task_list_value = value["commit_task_list"];
  if (!commit_task_list_value.isArray()) {
    LOG(ERROR) << "[meta.chunkset] commit_task_list is not array.";
    return false;
  }

  for (const auto& item : commit_task_list_value) {
    uint64_t task_id = item["task_id"].asUInt64();

    // load delta_slices
    if (!item["delta_slices"].isArray()) {
      LOG(ERROR) << "[meta.commit_task] delta_slices is not array.";
      return false;
    }

    std::vector<CommitTask::DeltaSlice> delta_slices;
    for (const auto& delta_slice_item : item["delta_slices"]) {
      CommitTask::DeltaSlice delta_slice;
      delta_slice.chunk_index = delta_slice_item["chunk_index"].asUInt();

      if (delta_slice_item["slices"].isArray()) {
        for (const auto& slice_item : delta_slice_item["slices"]) {
          delta_slice.slices.push_back(Helper::LoadSlice(slice_item));
        }
      }

      delta_slices.push_back(delta_slice);
    }

    auto task = std::make_shared<CommitTask>(task_id, std::move(delta_slices));
    if (!task->Load(item)) {
      LOG(ERROR) << "[meta.chunkset] load commit_task fail.";
      return false;
    }
    commit_task_list_.push_back(task);
  }

  task_id_generator.store(
      std::max(task_id_generator.load(), value["id_generator"].asUInt64()));

  last_commit_time_ms_.store(value["last_commit_time_ms"].asUInt64());

  return true;
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs