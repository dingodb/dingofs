/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DINGOFS_CLIENT_VFS_VFS_HUB_H_
#define DINGOFS_CLIENT_VFS_VFS_HUB_H_

#include <atomic>
#include <memory>

#include "cache/blockcache/block_cache.h"
#include "client/common/config.h"
#include "client/vfs/handle/handle_manager.h"
#include "client/vfs/meta/meta_system.h"
#include "client/vfs/vfs.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "blockaccess/block_accesser.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub {
 public:
  VFSHub() = default;

  virtual ~VFSHub() = default;

  virtual Status Start(const VFSConfig& vfs_conf,
                       const common::ClientOption& client_option) = 0;

  virtual Status Stop() = 0;

  virtual MetaSystem* GetMetaSystem() = 0;

  virtual HandleManager* GetHandleManager() = 0;

  virtual cache::blockcache::BlockCache* GetBlockCache() = 0;

  virtual blockaccess::BlockAccesser* GetBlockAccesser() = 0;

  virtual FsInfo GetFsInfo() = 0;
};

class VFSHubImpl : public VFSHub {
 public:
  VFSHubImpl() = default;

  ~VFSHubImpl() override = default;

  Status Start(const VFSConfig& vfs_conf,
               const common::ClientOption& client_option) override;

  Status Stop() override;

  MetaSystem* GetMetaSystem() override;

  HandleManager* GetHandleManager() override;

  cache::blockcache::BlockCache* GetBlockCache() override;

  blockaccess::BlockAccesser* GetBlockAccesser() override;

  FsInfo GetFsInfo() override;

 private:
  std::atomic_bool started_{false};

  common::ClientOption client_option_;
  FsInfo fs_info_;
  S3Info s3_info_;
  std::unique_ptr<MetaSystem> meta_system_;
  std::unique_ptr<HandleManager> handle_manager_;
  std::unique_ptr<blockaccess::BlockAccesser> block_accesser_;
  std::unique_ptr<cache::blockcache::BlockCache> block_cache_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_VFS_HUB_H_