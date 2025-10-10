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

#include "client/vfs/metasystem/mds/rpc.h"
#include "common/status.h"
#include "mds/mds/mds_meta.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class MDSDiscovery;
using MDSDiscoverySPtr = std::shared_ptr<MDSDiscovery>;

class MDSDiscovery {
 public:
  MDSDiscovery(RPCPtr rpc) : rpc_(rpc) {};
  ~MDSDiscovery() = default;

  static MDSDiscoverySPtr New(RPCPtr rpc) {
    return std::make_shared<MDSDiscovery>(rpc);
  }

  bool Init();
  void Destroy();

  bool GetMDS(int64_t mds_id, mds::MDSMeta& mds_meta);
  void PickFirstMDS(mds::MDSMeta& mds_meta);
  std::vector<mds::MDSMeta> GetAllMDS();
  std::vector<mds::MDSMeta> GetMDSByState(mds::MDSMeta::State state);
  std::vector<mds::MDSMeta> GetNormalMDS(bool force = true);

  void SetAbnormalMDS(int64_t mds_id);
  bool RefreshFullyMDSList();

 private:
  Status GetMDSList(std::vector<mds::MDSMeta>& mdses);

  utils::RWLock lock_;
  // mds_id -> MDSMeta
  std::map<int64_t, mds::MDSMeta> mdses_;

  RPCPtr rpc_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_MDS_DISCOVERY_H_