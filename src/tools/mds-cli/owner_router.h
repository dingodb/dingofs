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

#ifndef DINGOFS_MDS_CLIENT_OWNER_ROUTER_H_
#define DINGOFS_MDS_CLIENT_OWNER_ROUTER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "dingofs/mds.pb.h"
#include "mds/common/type.h"
#include "tools/mds-cli/mds.h"

namespace dingofs {
namespace mds {
namespace client {

// Routes an inode to the MDS that owns its partition under the fs partition
// policy (same parent-hash scheme the FUSE client uses), and hands back a
// client connected to that MDS.
//
// Why this matters: requests whose correctness depends on the owner's in-memory
// state -- a fresh partition cache, or an unflushed dir-stat delta that lives
// only on the owner -- must be sent to the owner, not to an arbitrary MDS.
// RestoreFromTrash (trash restore) and GetDirStat (single-directory stat) both
// need this.
//
// Read-only after Init, so concurrent ClientForIno() calls need no locking.
class OwnerRouter {
 public:
  OwnerRouter() = default;
  ~OwnerRouter() = default;

  OwnerRouter(const OwnerRouter&) = delete;
  OwnerRouter& operator=(const OwnerRouter&) = delete;

  // Fetch the fs partition policy (via bootstrap.GetFs) and the MDS list (via
  // bootstrap.GetMdsList), then build one MDSClient per owner MDS. `bootstrap`
  // is an already-Init'd client to any reachable MDS, used only for these two
  // setup RPCs. Returns false on failure (errors are logged / printed).
  bool Init(uint32_t fs_id, MDSClient& bootstrap);

  // Client connected to the MDS that owns `ino`'s partition (ino is used as the
  // parent-hash key, i.e. the directory whose children/stat we want). Returns
  // nullptr when no route is known or the owner's client is unavailable; the
  // caller should handle that instead of misrouting to another MDS.
  MDSClient* ClientForIno(Ino ino);

 private:
  uint32_t fs_id_{0};
  pb::mds::PartitionPolicy partition_policy_;
  std::unordered_map<uint32_t, uint64_t> bucket_to_mds_;                  // parent-hash only
  std::unordered_map<uint64_t, std::unique_ptr<MDSClient>> mds_clients_;  // mds_id -> client
};

}  // namespace client
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_CLIENT_OWNER_ROUTER_H_
