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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_MDS_DISCOVERY_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_MDS_DISCOVERY_H_

#include <map>
#include <memory>
#include <vector>

#include "bthread/types.h"
#include "mdsv2/common/status.h"
#include "mdsv2/coordinator/coordinator_client.h"
#include "mdsv2/mds/mds_meta.h"

namespace dingofs {
namespace client {
namespace filesystem {

class MDSDiscovery;
using MDSDiscoveryPtr = std::shared_ptr<MDSDiscovery>;

class MDSDiscovery {
 public:
  MDSDiscovery(mdsv2::CoordinatorClientPtr coordinator_client);
  ~MDSDiscovery() = default;

  static MDSDiscoveryPtr New(mdsv2::CoordinatorClientPtr coordinator_client) {
    return std::make_shared<MDSDiscovery>(coordinator_client);
  }

  bool Init();
  void Destroy();

  bool GetMDS(int64_t mds_id, mdsv2::MDSMeta& mds_meta);
  bool PickFirstMDS(mdsv2::MDSMeta& mds_meta);
  std::vector<mdsv2::MDSMeta> GetAllMDS();

 private:
  bool UpdateMDSList();

  utils::RWLock lock_;
  std::map<int64_t, mdsv2::MDSMeta> mdses_;

  mdsv2::CoordinatorClientPtr coordinator_client_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_MDS_DISCOVERY_H_