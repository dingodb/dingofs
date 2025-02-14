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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_MDS_ROUTER_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_MDS_ROUTER_H_

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "client/vfs/meta/v2/mds_discovery.h"
#include "client/vfs/meta/v2/parent_cache.h"
#include "dingofs/mdsv2.pb.h"
#include "glog/logging.h"
#include "mdsv2/mds/mds_meta.h"

namespace dingofs {
namespace client {
namespace filesystem {

class MDSRouter {
 public:
  virtual ~MDSRouter() = default;

  virtual bool Init() = 0;

  virtual mdsv2::MDSMeta GetMDSByParentIno(int64_t parent_ino) = 0;
  virtual mdsv2::MDSMeta GetMDSByIno(int64_t ino) = 0;

  virtual void UpdateMDS(const mdsv2::MDSMeta&) {
    CHECK(false) << "Not implemented.";
  }
};

using MDSRouterPtr = std::shared_ptr<MDSRouter>;

class MonoMDSRouter;
using MonoMDSRouterPtr = std::shared_ptr<MonoMDSRouter>;

class MonoMDSRouter : public MDSRouter {
 public:
  MonoMDSRouter(const mdsv2::MDSMeta& mds_meta) : mds_meta_(mds_meta){};
  ~MonoMDSRouter() override = default;

  static MonoMDSRouterPtr New(const mdsv2::MDSMeta& mds_meta) {
    return std::make_shared<MonoMDSRouter>(mds_meta);
  }

  bool Init() override;

  mdsv2::MDSMeta GetMDSByParentIno(int64_t parent_ino) override;

  mdsv2::MDSMeta GetMDSByIno(int64_t ino) override;

  void UpdateMDS(const mdsv2::MDSMeta&) override;

 private:
  utils::RWLock lock_;
  mdsv2::MDSMeta mds_meta_;
};

class ParentHashMDSRouter;
using ParentHashMDSRouterPtr = std::shared_ptr<ParentHashMDSRouter>;

class ParentHashMDSRouter : public MDSRouter {
 public:
  ParentHashMDSRouter(const pb::mdsv2::HashPartition& hash_partition,
                      MDSDiscoveryPtr mds_discovery,
                      ParentCachePtr parent_cache)
      : hash_partition_(hash_partition),
        mds_discovery_(mds_discovery),
        parent_cache_(parent_cache) {}
  ~ParentHashMDSRouter() override = default;

  static ParentHashMDSRouterPtr New(
      const pb::mdsv2::HashPartition& hash_partition,
      MDSDiscoveryPtr mds_discovery, ParentCachePtr parent_cache) {
    return std::make_shared<ParentHashMDSRouter>(hash_partition, mds_discovery,
                                                 parent_cache);
  }

  bool Init() override;

  mdsv2::MDSMeta GetMDSByParentIno(int64_t parent_ino) override;

  mdsv2::MDSMeta GetMDSByIno(int64_t ino) override;

 private:
  pb::mdsv2::HashPartition hash_partition_;
  MDSDiscoveryPtr mds_discovery_;

  ParentCachePtr parent_cache_;

  utils::RWLock lock_;
  // bucket_id -> mds_meta
  std::unordered_map<int64_t, mdsv2::MDSMeta> mdses_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_MDS_ROUTER_H_
