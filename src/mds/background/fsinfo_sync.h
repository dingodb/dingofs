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

#ifndef DINGOFS_MDS_BACKGROUND_FSINFO_SYNC_H_
#define DINGOFS_MDS_BACKGROUND_FSINFO_SYNC_H_

#include "mds/filesystem/filesystem.h"

namespace dingofs {
namespace mds {

class FsInfoSync;
using FsInfoSyncSPtr = std::shared_ptr<FsInfoSync>;

class FsInfoSync {
 public:
  FsInfoSync(FileSystemSetSPtr file_system_set) : file_system_set_(file_system_set) {};
  ~FsInfoSync() = default;

  static FsInfoSyncSPtr New(FileSystemSetSPtr fs_set) { return std::make_shared<FsInfoSync>(fs_set); }

  void Run();

 private:
  void SyncFsInfo();

  std::atomic<bool> is_running_{false};

  FileSystemSetSPtr file_system_set_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_BACKGROUND_FSINFO_SYNC_H_