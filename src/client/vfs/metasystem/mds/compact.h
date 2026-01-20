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

#include <string>

#include "client/vfs/compaction/compactor.h"
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
  CompactChunkTask(Ino ino, ChunkSPtr& chunk, MDSClient& mds_client,
                   Compactor& compactor)
      : ino_(ino),
        chunk_(chunk),
        mds_client_(mds_client),
        compactor_(compactor) {}
  ~CompactChunkTask() override = default;

  static CompactChunkTaskPtr New(Ino ino, ChunkSPtr& chunk,
                                 MDSClient& mds_client, Compactor& compactor) {
    return std::make_shared<CompactChunkTask>(ino, chunk, mds_client,
                                              compactor);
  }

  std::string Type() override { return "COMPACT_CHUNK"; }

  void Run() override;

  void Wait() { cond_.Wait(); }

  void Signal() { cond_.DecreaseSignal(); }

  Status GetStatus() { return status_; }

 private:
  Status Compact();

  Ino ino_;
  ChunkSPtr chunk_;

  MDSClient& mds_client_;
  Compactor& compactor_;

  Status status_;
  mds::BthreadCond cond_{1};
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

  Status LaunchCompact(Ino ino, ChunkSPtr& chunk, MDSClient& mds_client,
                       Compactor& compactor, bool is_async = true);

 public:
  WorkerSetUPtr worker_set_;
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_COMPACT_H_
