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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_DISCOVERY_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_DISCOVERY_H_

#include <atomic>
#include <cstddef>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "client/vfs/metasystem/mds/rpc.h"
#include "common/status.h"
#include "mds/mds/mds_meta.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

class MDSDiscovery {
 public:
  MDSDiscovery(RPC& rpc) : rpc_(rpc) {};
  ~MDSDiscovery() = default;

  bool Init();
  void Stop();

  bool GetMDS(int64_t mds_id, mds::MDSMeta& mds_meta);
  void PickFirstMDS(mds::MDSMeta& mds_meta);
  std::vector<mds::MDSMeta> GetAllMDS();
  std::vector<mds::MDSMeta> GetMDSByState(mds::MDSMeta::State state);
  std::vector<mds::MDSMeta> GetNormalMDS(bool force = true);

  void SetAbnormalMDS(int64_t mds_id);
  bool RefreshFullyMDSList();

  size_t Size();
  size_t Bytes();

  bool Dump(Json::Value& value);

 private:
  void IncActiveCount() {
    active_count_.fetch_add(1, std::memory_order_release);
  }
  void DecActiveCount() {
    active_count_.fetch_sub(1, std::memory_order_release);
  }
  uint32_t ActiveCount() {
    return active_count_.load(std::memory_order_acquire);
  }

  bool IsStop() { return stopped_.load(std::memory_order_acquire); }

  Status GetMDSList(std::vector<mds::MDSMeta>& mdses);

  utils::RWLock lock_;
  // mds_id -> MDSMeta
  absl::flat_hash_map<int64_t, mds::MDSMeta> mdses_;

  RPC& rpc_;

  std::atomic<uint32_t> active_count_{0};
  std::atomic<bool> stopped_{false};
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_DISCOVERY_H_