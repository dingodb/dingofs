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
#include <utility>

#include "mdsv2/common/distribution_lock.h"
#include "mdsv2/coordinator/coordinator_client.h"
#include "mdsv2/filesystem/filesystem.h"

namespace dingofs {
namespace mdsv2 {

class MDSMonitor;
using MDSMonitorPtr = std::shared_ptr<MDSMonitor>;

class MDSMonitor {
 public:
  MDSMonitor(std::unique_ptr<DistributionLock> dist_lock) : dist_lock_(std::move(dist_lock)) {}
  ~MDSMonitor() = default;

  static MDSMonitorPtr New(std::unique_ptr<DistributionLock> dist_lock) {
    return std::make_shared<MDSMonitor>(std::move(dist_lock));
  }

  bool Init();
  void Destroy();

  void MonitorMDS();

 private:
  std::atomic<bool> is_running_{false};

  CoordinatorClientPtr coordinator_client_;

  FileSystemSetPtr fs_set_;

  std::unique_ptr<DistributionLock> dist_lock_;
};

using MDSMonitorPtr = std::shared_ptr<MDSMonitor>;

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_MDS_MONITOR_H_