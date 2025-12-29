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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_ROUTER_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_ROUTER_H_

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "client/vfs/metasystem/mds/mds_discovery.h"
#include "client/vfs/metasystem/mds/parent_memo.h"
#include "dingofs/mds.pb.h"
#include "mds/mds/mds_meta.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

class MDSRouter {
 public:
  virtual ~MDSRouter() = default;

  virtual bool Init(const pb::mds::PartitionPolicy& partition_policy) = 0;

  virtual bool GetMDSByParent(Ino parent, mds::MDSMeta& mds_meta) = 0;
  virtual bool GetMDS(Ino ino, mds::MDSMeta& mds_meta) = 0;
  virtual bool GetRandomlyMDS(mds::MDSMeta& mds_meta) = 0;

  virtual bool UpdateRouter(
      const pb::mds::PartitionPolicy& partition_policy) = 0;

  virtual bool Dump(Json::Value& value) = 0;
};

using MDSRouterPtr = std::shared_ptr<MDSRouter>;

class MonoMDSRouter;
using MonoMDSRouterPtr = std::shared_ptr<MonoMDSRouter>;

class MonoMDSRouter : public MDSRouter {
 public:
  MonoMDSRouter(MDSDiscoverySPtr mds_discovery)
      : mds_discovery_(mds_discovery) {};
  ~MonoMDSRouter() override = default;

  static MonoMDSRouterPtr New(MDSDiscoverySPtr mds_discovery) {
    return std::make_shared<MonoMDSRouter>(mds_discovery);
  }

  bool Init(const pb::mds::PartitionPolicy& partition_policy) override;

  bool GetMDSByParent(Ino parent, mds::MDSMeta& mds_meta) override;

  bool GetMDS(Ino ino, mds::MDSMeta& mds_meta) override;
  bool GetRandomlyMDS(mds::MDSMeta& mds_meta) override;

  bool UpdateRouter(const pb::mds::PartitionPolicy& partition_policy) override;

  bool Dump(Json::Value& value) override;

 private:
  bool UpdateMds(int64_t mds_id);

  utils::RWLock lock_;
  mds::MDSMeta mds_meta_;

  MDSDiscoverySPtr mds_discovery_;
};

class ParentHashMDSRouter;
using ParentHashMDSRouterPtr = std::shared_ptr<ParentHashMDSRouter>;

class ParentHashMDSRouter : public MDSRouter {
 public:
  ParentHashMDSRouter(MDSDiscoverySPtr mds_discovery,
                      ParentMemoSPtr parent_memo)
      : mds_discovery_(mds_discovery), parent_memo_(parent_memo) {}
  ~ParentHashMDSRouter() override = default;

  static ParentHashMDSRouterPtr New(MDSDiscoverySPtr mds_discovery,
                                    ParentMemoSPtr parent_memo) {
    return std::make_shared<ParentHashMDSRouter>(mds_discovery, parent_memo);
  }

  bool Init(const pb::mds::PartitionPolicy& partition_policy) override;

  bool GetMDSByParent(Ino parent, mds::MDSMeta& mds_meta) override;

  bool GetMDS(Ino ino, mds::MDSMeta& mds_meta) override;

  bool GetRandomlyMDS(mds::MDSMeta& mds_meta) override;

  bool UpdateRouter(const pb::mds::PartitionPolicy& partition_policy) override;

  bool Dump(Json::Value& value) override;

 private:
  void UpdateMDSes(const pb::mds::HashPartition& hash_partition);

  MDSDiscoverySPtr mds_discovery_;
  ParentMemoSPtr parent_memo_;

  utils::RWLock lock_;
  pb::mds::HashPartition hash_partition_;
  // bucket_id -> mds_meta
  std::unordered_map<int64_t, mds::MDSMeta> mds_map_;
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_ROUTER_H_
