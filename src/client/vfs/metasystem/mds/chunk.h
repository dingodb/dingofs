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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_CHUNK_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_CHUNK_H_

#include <sys/types.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "client/vfs/metasystem/mds/helper.h"
#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "mds/common/synchronization.h"
#include "mds/common/type.h"
#include "utils/concurrent/concurrent.h"
#include "utils/shards.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

using dingofs::mds::ChunkDescriptor;
using dingofs::mds::ChunkEntry;

class Chunk;
using ChunkSPtr = std::shared_ptr<Chunk>;

class ChunkSet;
using ChunkSetSPtr = std::shared_ptr<ChunkSet>;

class Chunk {
 public:
  Chunk(Ino ino, uint32_t index) : ino_(ino), index_(index) {}
  Chunk(Ino ino, const ChunkEntry& chunk, const char* reason)
      : ino_(ino), index_(chunk.index()) {
    Put(chunk, reason);
  }

  ~Chunk() = default;

  static ChunkSPtr New(Ino ino, uint32_t index) {
    return std::make_shared<Chunk>(ino, index);
  }

  static ChunkSPtr New(Ino ino, const ChunkEntry& chunk, const char* reason) {
    return std::make_shared<Chunk>(ino, chunk, reason);
  }

  Ino GetIno() const { return ino_; }
  uint32_t GetIndex() const { return index_; }

  bool Put(const ChunkEntry& chunk, const char* reason);
  bool Compact(uint32_t start_pos, uint64_t start_slice_id, uint32_t end_pos,
               uint64_t end_slice_id, const std::vector<Slice>& new_slices);
  void AppendSlice(const std::vector<Slice>& slices);

  bool IsCompleted() const {
    utils::ReadLockGuard guard(lock_);
    return is_completed_;
  }

  bool HasStage() const {
    utils::ReadLockGuard guard(lock_);
    return !stage_slices_.empty();
  }
  bool HasCommitting() const {
    utils::ReadLockGuard guard(lock_);

    return !commiting_slices_.empty();
  }
  std::vector<Slice> CommitSlice();
  void MarkCommited(uint64_t version);

  Status IsNeedCompaction(bool check_interval = true);

  uint64_t GetVersion() {
    utils::ReadLockGuard guard(lock_);
    return commited_version_;
  }

  std::vector<Slice> GetAllSlice(uint64_t& version);
  std::vector<Slice> GetCommitedSlice(uint64_t& version);

  // output json format string
  bool Dump(Json::Value& value, bool is_summary = false);
  bool Load(const Json::Value& value);

 private:
  mutable utils::RWLock lock_;

  const Ino ino_;
  const uint32_t index_{0};

  // use put ChunkEntry's is completed
  bool is_completed_{false};

  std::vector<Slice> stage_slices_;
  std::vector<Slice> commiting_slices_;
  std::vector<Slice> commited_slices_;

  uint64_t commited_version_{0};
  uint64_t last_compaction_time_ms_{0};
};

class CommitTask;
using CommitTaskSPtr = std::shared_ptr<CommitTask>;

// for commit new slice
class CommitTask {
 public:
  struct DeltaSlice {
    uint32_t chunk_index{0};
    std::vector<Slice> slices;
  };

  CommitTask(uint64_t task_id, std::vector<DeltaSlice>&& delta_slices)
      : task_id_(task_id), delta_slices_(std::move(delta_slices)), cond(1) {}

  uint64_t TaskID() const { return task_id_; }
  const std::vector<DeltaSlice>& DeltaSlices() const { return delta_slices_; }

  std::vector<uint32_t> GetChunkIndexs() const {
    utils::ReadLockGuard lk(lock_);

    std::vector<uint32_t> indexs;
    indexs.reserve(delta_slices_.size());
    for (const auto& delta_slice : delta_slices_) {
      indexs.push_back(delta_slice.chunk_index);
    }
    return indexs;
  }

  std::string Describe() const {
    std::string str;
    str.reserve(1024);
    for (uint32_t i = 0; i < delta_slices_.size(); ++i) {
      const auto& delta_slice = delta_slices_[i];
      str += fmt::format("{}:{}", delta_slice.chunk_index,
                         Helper::ToString(delta_slice.slices));
      if (i + 1 != delta_slices_.size()) str += ";";
    }
    return fmt::format("task_id({}) retry({}) slices({})", task_id_, Retries(),
                       str);
  }

  void SetDone(const Status& s) {
    utils::WriteLockGuard lk(lock_);

    state_ = State::DONE;
    status_ = s;

    Signal();
  }

  bool IsDone() {
    utils::ReadLockGuard lk(lock_);

    return state_ == State::DONE;
  }

  Status GetStatus() {
    utils::ReadLockGuard lk(lock_);
    return status_;
  }

  bool MaybeRun() {
    utils::WriteLockGuard lk(lock_);

    if (state_ == State::INIT) {
      state_ = State::RUNNING;
      return true;

    } else if (state_ == State::RUNNING) {
      return false;

    } else {
      if (status_.ok()) return false;

      retries_.fetch_add(1, std::memory_order_relaxed);
      state_ = State::RUNNING;
      status_ = Status::OK();
      cond.Reset(1);

      return true;
    }
  }

  uint32_t Retries() const { return retries_.load(std::memory_order_relaxed); }

  void Wait() { cond.Wait(); }

  void Signal() { cond.DecreaseBroadcast(); }

  // output json format string
  bool Dump(Json::Value& value);

  enum class State : uint8_t {
    INIT = 0,
    RUNNING = 1,
    DONE = 2,
  };

 private:
  mutable utils::RWLock lock_;

  const uint64_t task_id_;
  const std::vector<DeltaSlice> delta_slices_;

  State state_{State::INIT};
  Status status_;

  std::atomic<uint32_t> retries_{0};

  mds::BthreadCond cond;
};

// chunk set per file
class ChunkSet {
 public:
  ChunkSet(Ino ino) : ino_(ino) { RefreshLastActiveTime(); }
  ~ChunkSet() = default;

  static ChunkSetSPtr New(Ino ino) { return std::make_shared<ChunkSet>(ino); }

  Ino GetIno() const { return ino_; }

  // write memo operations
  void SetLastWriteLength(uint64_t offset, uint64_t size) {
    utils::WriteLockGuard lk(lock_);
    last_write_length_ = std::max(last_write_length_, offset + size);
    last_time_ns_ = utils::TimestampNs();
  }
  void ResetLastWriteLength() {
    utils::WriteLockGuard lk(lock_);
    last_write_length_ = 0;
  }
  uint64_t GetLastWriteLength() const {
    utils::ReadLockGuard guard(lock_);
    return last_write_length_;
  }

  uint64_t GetLastWriteTimeNs() const {
    utils::ReadLockGuard guard(lock_);
    return last_time_ns_;
  }

  // chunk operations
  void Append(uint32_t index, const std::vector<Slice>& slices);
  void Put(const std::vector<ChunkEntry>& chunks, const char* reason);

  size_t GetChunkSize() const {
    utils::ReadLockGuard guard(lock_);
    return chunk_map_.size();
  }

  bool Exist(uint32_t index) {
    utils::ReadLockGuard guard(lock_);
    return chunk_map_.find(index) != chunk_map_.end();
  }

  ChunkSPtr Get(uint32_t index) {
    utils::ReadLockGuard guard(lock_);

    auto it = chunk_map_.find(index);
    return (it != chunk_map_.end()) ? it->second : nullptr;
  }

  std::vector<ChunkSPtr> GetAll();

  uint64_t GetVersion(uint32_t index);
  std::vector<std::pair<uint32_t, uint64_t>> GetAllVersion();

  bool HasStage();
  bool HasCommitting();

  // task operations
  bool HasCommitTask() {
    utils::ReadLockGuard guard(lock_);
    return !commit_task_map_.empty();
  }

  uint32_t TryCommitSlice(bool is_force);

  void FinishCommitTask(uint64_t task_id,
                        const std::vector<ChunkDescriptor>& chunk_descriptors);
  std::vector<CommitTaskSPtr> ListCommitTask();

  size_t GetCommitTaskSize() const {
    utils::ReadLockGuard guard(lock_);
    return commit_task_map_.size();
  }

  bool HasUncommitedSlice();

  uint64_t LastActiveTimeS() const {
    return last_active_s_.load(std::memory_order_relaxed);
  }
  void RefreshLastActiveTime() {
    last_active_s_.store(utils::Timestamp(), std::memory_order_relaxed);
  }

  // output json format string
  bool Dump(Json::Value& valuem, bool is_summary = false);
  bool Load(const Json::Value& value);

 private:
  bool HasSpecificChunkCommitTaskUnlock(uint32_t chunk_index) {
    return committing_chunk_index_set_.find(chunk_index) !=
           committing_chunk_index_set_.end();
  }

  CommitTaskSPtr CreateCommitTaskUnlock(
      std::vector<CommitTask::DeltaSlice>&& delta_slices);

  const Ino ino_;

  mutable utils::RWLock lock_;

  uint64_t last_write_length_{0};
  uint64_t last_time_ns_{0};

  // chunk index -> chunk
  absl::flat_hash_map<uint32_t, ChunkSPtr> chunk_map_;

  // task id -> commit task
  absl::flat_hash_map<uint64_t, CommitTaskSPtr> commit_task_map_;
  absl::flat_hash_set<uint32_t> committing_chunk_index_set_;

  std::atomic<uint64_t> last_commit_ms_{0};

  std::atomic<uint64_t> last_active_s_{0};
};

// all file chunk cache
class ChunkCache {
 public:
  ChunkCache() = default;
  ~ChunkCache() = default;

  ChunkSetSPtr GetOrCreateChunkSet(Ino ino);

  bool HasUncommitedSlice();

  size_t Size();

  void Clear();

  void CleanExpired(uint64_t expire_s);

  bool Dump(Json::Value& value, bool is_summary = false);

 private:
  using Map = absl::flat_hash_map<Ino, ChunkSetSPtr>;

  constexpr static size_t kShardNum = 32;
  utils::Shards<Map, kShardNum> shard_map_;
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_CHUNK_H_
