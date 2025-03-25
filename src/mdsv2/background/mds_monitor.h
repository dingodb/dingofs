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

#ifndef DINGOFS_MDV2_MDS_MONITOR_H_
#define DINGOFS_MDV2_MDS_MONITOR_H_

#include <atomic>
#include <memory>

#include "mdsv2/common/distribution_lock.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/filesystem.h"

namespace dingofs {
namespace mdsv2 {

class MDSMonitor;
using MDSMonitorPtr = std::shared_ptr<MDSMonitor>;

class MDSMonitor {
 public:
  MDSMonitor(FileSystemSetPtr fs_set, DistributionLockPtr dist_lock) : fs_set_(fs_set), dist_lock_(dist_lock) {}
  ~MDSMonitor() = default;

  static MDSMonitorPtr New(FileSystemSetPtr fs_set, DistributionLockPtr dist_lock) {
    return std::make_shared<MDSMonitor>(fs_set, dist_lock);
  }

  bool Init();
  void Destroy();

  void Run();

 private:
  Status MonitorMDS();
  Status ProcessFaultMDS(std::vector<MDSMeta>& mdses);
  static void NotifyRefreshFs(const MDSMeta& mds, const std::string& fs_name);
  static void NotifyRefreshFs(const std::vector<MDSMeta>& mdses, const std::string& fs_name);

  std::atomic<bool> is_running_{false};

  FileSystemSetPtr fs_set_;

  DistributionLockPtr dist_lock_;
};

using MDSMonitorPtr = std::shared_ptr<MDSMonitor>;

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_MDS_MONITOR_H_