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

#include "mds/background/quota_sync.h"

namespace dingofs {
namespace mds {

void QuotaSynchronizer::Run() {
  bool running = false;
  if (!is_running_.compare_exchange_strong(running, true)) {
    return;
  }
  DEFER(is_running_.store(false));

  SyncFsQuota();
}

void QuotaSynchronizer::SyncFsQuota() {
  auto fses = fs_set_->GetAllFileSystem();
  for (auto& fs : fses) {
    auto& quota_manager = fs->GetQuotaManager();

    // load fs and dir quota
    quota_manager.LoadQuota();

    // flush fs and dir usage
    quota_manager.FlushUsage();
  }
}

}  // namespace mds
}  // namespace dingofs