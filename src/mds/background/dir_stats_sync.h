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

#ifndef DINGOFS_MDS_BACKGROUND_DIR_STATS_SYNC_H_
#define DINGOFS_MDS_BACKGROUND_DIR_STATS_SYNC_H_

#include <atomic>
#include <memory>

#include "mds/filesystem/filesystem.h"

namespace dingofs {
namespace mds {

class DirStatsSynchronizer;
using DirStatsSynchronizerSPtr = std::shared_ptr<DirStatsSynchronizer>;

// Periodic per-filesystem dir-stats sync: flushes accumulated per-directory
// dir-stat delta back to store. Flush-only (never loads). Driven by a
// background crontab tick.
class DirStatsSynchronizer {
 public:
  DirStatsSynchronizer(FileSystemSetSPtr fs_set) : fs_set_(fs_set) {};
  ~DirStatsSynchronizer() = default;

  static DirStatsSynchronizerSPtr New(FileSystemSetSPtr fs_set) { return std::make_shared<DirStatsSynchronizer>(fs_set); }

  void Run();

 private:
  // for each filesystem: flush dir-stats delta
  void SyncDirStats();

  std::atomic<bool> is_running_{false};

  FileSystemSetSPtr fs_set_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_BACKGROUND_DIR_STATS_SYNC_H_
