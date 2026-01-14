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

#include <cstdint>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "client/vfs/metasystem/mds/chunk.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "client/vfs/vfs_meta.h"
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

class CompactChunkTask : public TaskRunnable {
 public:
  CompactChunkTask(Ino ino, ChunkSPtr& chunk, MDSClient& mds_client)
      : ino_(ino), chunk_(chunk), mds_client_(mds_client) {}
  ~CompactChunkTask() override = default;

  static CompactChunkTaskPtr New(Ino ino, ChunkSPtr& chunk,
                                 MDSClient& mds_client) {
    return std::make_shared<CompactChunkTask>(ino, chunk, mds_client);
  }

  std::string Type() override { return "COMPACT_CHUNK"; }

  std::string Key() override {
    return fmt::format("COMPACT_CHUNK_{}_{}", ino_, chunk_->GetIndex());
  }

  void Run() override;

 private:
  void CompactCompletelyOverlap();
  Status Compact();

  Ino ino_;
  ChunkSPtr chunk_;

  MDSClient& mds_client_;
};

class CompactProcessor {
 public:
  CompactProcessor() = default;
  ~CompactProcessor() = default;

  // no copy and move
  CompactProcessor(const CompactProcessor&) = delete;
  CompactProcessor& operator=(const CompactProcessor&) = delete;
  CompactProcessor(CompactProcessor&&) = delete;
  CompactProcessor& operator=(CompactProcessor&&) = delete;

  bool Init();
  void Stop();

  void Execute(TaskRunnablePtr task);

 public:
  bool IsExistTask(const std::string& key);
  void RememberTask(const std::string& key);
  void ForgetTask(const std::string& key);

  WorkerSetUPtr worker_set_;

  mutable utils::RWLock lock_;
  // task key
  absl::flat_hash_set<std::string> doing_tasks_;
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_COMPACT_H_
