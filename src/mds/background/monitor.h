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

#ifndef DINGOFS_MDS_BACKGROUND_MDS_MONITOR_H_
#define DINGOFS_MDS_BACKGROUND_MDS_MONITOR_H_

#include <atomic>
#include <memory>

#include "mds/common/distribution_lock.h"
#include "mds/common/status.h"
#include "mds/filesystem/filesystem.h"

namespace dingofs {
namespace mds {

class Monitor;
using MonitorSPtr = std::shared_ptr<Monitor>;

class Monitor {
 public:
  Monitor(FileSystemSetSPtr fs_set, DistributionLockSPtr dist_lock, notify::NotifyBuddySPtr notify_buddy)
      : fs_set_(fs_set), dist_lock_(dist_lock), notify_buddy_(notify_buddy) {}
  ~Monitor() = default;

  static MonitorSPtr New(FileSystemSetSPtr fs_set, DistributionLockSPtr dist_lock,
                         notify::NotifyBuddySPtr notify_buddy) {
    return std::make_shared<Monitor>(fs_set, dist_lock, notify_buddy);
  }

  bool Init();
  void Destroy();

  void Run();

 private:
  Status MonitorMDS();
  Status ProcessFaultMDS(std::vector<MDSMeta>& mdses);
  void NotifyRefreshFs(const MDSMeta& mds, const FsInfoEntry& fs_info);
  void NotifyRefreshFs(const std::vector<MDSMeta>& mdses, const FsInfoEntry& fs_info);

  Status MonitorClient();

  std::atomic<bool> is_running_{false};

  FileSystemSetSPtr fs_set_;

  DistributionLockSPtr dist_lock_;

  // notify buddy
  notify::NotifyBuddySPtr notify_buddy_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_BACKGROUND_MDS_MONITOR_H_