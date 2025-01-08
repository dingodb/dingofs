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

#include "dingofs/src/client/filesystemv2/mds_discovery.h"

#include "bthread/mutex.h"
#include "dingofs/src/mdsv2/common/status.h"
#include "fmt/core.h"

namespace dingofs {
namespace client {
namespace filesystem {

MDSDiscovery::MDSDiscovery(mdsv2::CoordinatorClientPtr coordinator_client)
    : coordinator_client_(coordinator_client) {
  bthread_mutex_init(&mutex_, nullptr);
}

MDSDiscovery::~MDSDiscovery() { bthread_mutex_destroy(&mutex_); }

bool MDSDiscovery::Init() { return UpdateMDSList(); }

void MDSDiscovery::Destroy() {}

bool MDSDiscovery::GetMDS(int64_t mds_id, mdsv2::MDSMeta& mds_meta) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = mdses_.find(mds_id);
  if (it == mdses_.end()) {
    return false;
  }

  mds_meta = it->second;

  return true;
}

bool MDSDiscovery::UpdateMDSList() {
  std::vector<mdsv2::MDSMeta> mdses;
  auto status = coordinator_client_->GetMDSList(mdses);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("get mds list fail, error: {}.",
                              status.error_str());
    return false;
  }

  {
    BAIDU_SCOPED_LOCK(mutex_);

    for (const auto& mds : mdses) {
      CHECK(mds.ID() != 0) << "mds id is 0.";
      mdses_[mds.ID()] = mds;
    }
  }

  return true;
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs