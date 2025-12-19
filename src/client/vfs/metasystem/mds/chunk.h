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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_CHUNK_H_
#define DINGOFS_SRC_CLIENT_VFS_META_CHUNK_H_

#include <absl/container/inlined_vector.h>
#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "client/vfs/metasystem/mds/helper.h"
#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "mds/common/synchronization.h"
#include "mds/common/type.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

using mds::ChunkDescriptor;
using mds::ChunkEntry;

class Chunk;
using ChunkSPtr = std::shared_ptr<Chunk>;

class ChunkSet;
using ChunkSetSPtr = std::shared_ptr<ChunkSet>;

class WriteMemo {
 public:
  WriteMemo() = default;
  ~WriteMemo() = default;

  void AddRange(uint64_t offset, uint64_t size);
  uint64_t GetLength();
  uint64_t LastTimeNs() const { return last_time_ns_; }

  // output json format string
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  struct Range {
    uint64_t start{0};
    uint64_t end{0};  // [start, end)
  };

  std::vector<Range> ranges_;
  uint64_t last_time_ns_{0};
};

class Chunk {
 public:
  Chunk(Ino ino, uint32_t index) : ino_(ino), index_(index) {}
  Chunk(Ino ino, const ChunkEntry& chunk) : ino_(ino), index_(chunk.index()) {
    Put(chunk);
  }

  ~Chunk() = default;

  static ChunkSPtr New(Ino ino, uint32_t index) {
    return std::make_shared<Chunk>(ino, index);
  }

  static ChunkSPtr New(Ino ino, const ChunkEntry& chunk) {
    return std::make_shared<Chunk>(ino, chunk);
  }

  void Put(const ChunkEntry& chunk);
  void AppendSlice(const std::vector<Slice>& slices);

  bool IsCompleted();

  bool HasStage();
  bool HasCommitting();
  std::vector<Slice> CommitSlice();
  void MarkCommited(uint64_t version);

  uint64_t GetVersion();
  std::vector<Slice> GetAllSlice();

  // output json format string
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  utils::RWLock lock_;

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

class CommitTask {
 public:
  struct DeltaSlice {
    uint32_t chunk_index{0};
    std::vector<Slice> slices;
  };

  CommitTask(uint64_t task_id, std::vector<DeltaSlice>&& delta_slices)
      : task_id_(task_id), delta_slices_(std::move(delta_slices)), cond(0) {}

  uint64_t TaskID() const { return task_id_; }
  const std::vector<DeltaSlice>& DeltaSlices() const { return delta_slices_; }

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
      cond.Reset();

      return true;
    }
  }

  uint32_t Retries() const { return retries_.load(std::memory_order_relaxed); }

  void Wait() { cond.IncreaseWait(); }

  void Signal() { cond.DecreaseSignal(); }

  // output json format string
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

  enum class State : uint8_t {
    INIT = 0,
    RUNNING = 1,
    DONE = 2,
  };

 private:
  utils::RWLock lock_;

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
  ChunkSet(Ino ino) : ino_(ino) {}
  ~ChunkSet() = default;

  static ChunkSetSPtr New(Ino ino) { return std::make_shared<ChunkSet>(ino); }

  void AddWriteMemo(uint64_t offset, uint64_t size);
  uint64_t GetLength();
  uint64_t GetLastWriteTimeNs();

  void Append(uint32_t index, const std::vector<Slice>& slices);
  void Put(const std::vector<ChunkEntry>& chunks);

  bool HasStage();
  bool HasCommitting();

  uint32_t TryCommitSlice(bool is_force);
  void MarkCommited(const std::vector<ChunkDescriptor>& chunk_descriptors);

  bool HasCommitTask();
  void DeleteCommitTask(uint64_t task_id);
  std::vector<CommitTaskSPtr> ListCommitTask();

  bool Exist(uint32_t index);
  ChunkSPtr Get(uint32_t index);

  uint64_t GetVersion(uint32_t index);
  std::vector<std::pair<uint32_t, uint64_t>> GetAllVersion();

  // output json format string
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  CommitTaskSPtr CreateCommitTask(
      std::vector<CommitTask::DeltaSlice>&& delta_slices);

  const Ino ino_;

  utils::RWLock lock_;

  WriteMemo write_memo_;

  // chunk index -> chunk
  absl::flat_hash_map<uint32_t, ChunkSPtr> chunk_map_;

  std::atomic<uint64_t> id_generator_{10000};
  std::list<CommitTaskSPtr> commit_task_list_;

  std::atomic<uint64_t> last_commit_time_ms_{0};
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_CHUNK_H_
