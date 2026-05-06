// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDS_CLIENT_TRASH_RESTORE_H_
#define DINGOFS_MDS_CLIENT_TRASH_RESTORE_H_

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_set>
#include <vector>

#include "dingofs/mds.pb.h"
#include "mds/client/mds.h"
#include "mds/common/type.h"

namespace dingofs {
namespace mds {
namespace client {

// Implements the `juicefs restore META HOUR ...` workflow against a DingoFS
// MDS. For each hour bucket, list all trash dentries, optionally pre-scan to
// build the set of trashed directories, then drive a worker pool that calls
// RestoreFromTrash for each entry.
//
// Modes:
//   - put_back = false (default): only entries whose original parent is itself
//     a trashed directory in the same hour bucket are restored; they are
//     grafted onto that trashed parent (tree-rebuild inside the bucket).
//   - put_back = true: every entry is restored to its original parent in the
//     live filesystem. Entries whose parent no longer exists or whose name is
//     already taken count as failures.
class TrashRestore {
 public:
  struct Options {
    uint32_t fs_id{0};
    std::vector<std::string> hours;
    bool put_back{false};
    uint32_t threads{10};
  };

  TrashRestore() = default;
  ~TrashRestore() = default;

  // Connect to MDS and validate caller is root. Returns false on any failure
  // (non-root euid, mds_addr empty, init fail). Errors are logged.
  bool Init(const std::string& mds_addr, const Options& options);

  // Run restore for each hour in options.hours, in order. Per-hour errors do
  // not abort remaining hours.
  void Run();

 private:
  void DoRestoreHour(const std::string& hour);
  void WorkerLoop(Ino bucket_ino);

  Options options_;
  std::unique_ptr<MDSClient> client_;

  // Per-hour state. Reset at the top of each DoRestoreHour.
  std::mutex mu_;
  std::condition_variable cv_;
  std::queue<pb::mds::Dentry> queue_;
  bool queue_closed_{false};
  // Populated only when options_.put_back == false: the set of directory
  // inodes that live inside the current hour bucket. A flat trash entry is
  // restored only if its parsed original parent is in this set.
  std::unordered_set<Ino> trashed_dir_inos_;

  std::atomic<int64_t> restored_{0};
  std::atomic<int64_t> skipped_{0};
  std::atomic<int64_t> failed_{0};
};

}  // namespace client
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_CLIENT_TRASH_RESTORE_H_
