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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_COMPACT_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_COMPACT_H_

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <cstdint>
#include <string>

#include "client/vfs/compaction/compactor.h"
#include "client/vfs/metasystem/mds/chunk.h"
#include "client/vfs/metasystem/mds/executor.h"
#include "client/vfs/metasystem/mds/inode_cache.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "mds/common/runnable.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

using WorkerSetUPtr = mds::WorkerSetUPtr;
using TaskRunnable = mds::TaskRunnable;
using TaskRunnablePtr = mds::TaskRunnablePtr;

class CompactChunkTask;
using CompactChunkTaskPtr = std::shared_ptr<CompactChunkTask>;

class CompactProcessor;

class CompactChunkTask : public TaskRunnable {
 public:
  CompactChunkTask(Ino ino, InodeSPtr& inode, ChunkSPtr& chunk,
                   CompactProcessor& compact_processor)
      : ino_(ino),
        inode_(inode),
        chunk_(chunk),
        compact_processor_(compact_processor) {}
  ~CompactChunkTask() override = default;

  static CompactChunkTaskPtr New(Ino ino, InodeSPtr& inode, ChunkSPtr& chunk,

                                 CompactProcessor& compact_processor) {
    return std::make_shared<CompactChunkTask>(ino, inode, chunk,
                                              compact_processor);
  }

  std::string Type() override { return "COMPACT_CHUNK"; }

  void Run() override;

  void Wait() { cond_.Wait(); }

  void Signal() { cond_.DecreaseSignal(); }

  Status GetStatus() { return status_; }

 private:
  bool IsDeleted() { return inode_ != nullptr && inode_->IsDeleted(); }
  void TryCleanupUncommittedSlices(const std::vector<Slice>& old_slices,
                                   const std::vector<Slice>& new_slices);

  Status Compact();

  Ino ino_;
  InodeSPtr inode_;
  ChunkSPtr chunk_;

  CompactProcessor& compact_processor_;

  Status status_;
  mds::BthreadCond cond_{1};
};

class CompactProcessor {
 public:
  CompactProcessor(MDSClient& mds_client, Compactor& compactor,
                   Executor& executor)
      : mds_client_(mds_client), compactor_(compactor), executor_(executor) {}
  ~CompactProcessor() = default;

  // no copy and move
  CompactProcessor(const CompactProcessor&) = delete;
  CompactProcessor& operator=(const CompactProcessor&) = delete;
  CompactProcessor(CompactProcessor&&) = delete;
  CompactProcessor& operator=(CompactProcessor&&) = delete;

  void Stop();

  Status Execute(Ino ino, InodeSPtr inode, ChunkSPtr& chunk,
                 bool is_async = true);

  bool IsStopped() { return is_stopped_.load(); }

  uint64_t GetCompactedVersion(Ino ino, uint32_t chunk_index);
  void UpdateComapctedVersion(Ino ino, uint32_t chunk_index, uint64_t version);

  void CleanExpired(uint64_t expire_time_s);

 private:
  friend class CompactChunkTask;

  MDSClient& GetMDSClient() { return mds_client_; }
  Compactor& GetCompactor() { return compactor_; }

  std::atomic<bool> is_stopped_{false};

  MDSClient& mds_client_;
  Compactor& compactor_;
  Executor& executor_;

  struct Value {
    uint64_t version;
    uint64_t last_active_time_s;
  };
  utils::RWLock lock_;
  // key: ino + chunk_index, value: compacted version
  absl::flat_hash_map<std::string, Value> compacted_version_memo_;
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_COMPACT_H_
