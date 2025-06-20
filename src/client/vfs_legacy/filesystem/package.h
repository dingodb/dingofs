/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Dingofs
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEM_PACKAGE_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEM_PACKAGE_H_

#include <memory>

#include "client/vfs_legacy/dentry_cache_manager.h"
#include "client/vfs_legacy/inode_cache_manager.h"

namespace dingofs {
namespace client {
namespace filesystem {

struct ExternalMember {  // external member depended by FileSystem
  ExternalMember() = delete;
  ExternalMember(
      std::shared_ptr<DentryCacheManager> p_dentry_manager,
      std::shared_ptr<InodeCacheManager> p_inode_manager,
      std::shared_ptr<stub::rpcclient::MetaServerClient> p_meta_client,
      std::shared_ptr<stub::rpcclient::MdsClient> p_mds_client)
      : dentryManager(std::move(p_dentry_manager)),
        inodeManager(std::move(p_inode_manager)),
        meta_client(std::move(p_meta_client)),
        mds_client(std::move(p_mds_client)) {}

  std::shared_ptr<DentryCacheManager> dentryManager;
  std::shared_ptr<InodeCacheManager> inodeManager;
  std::shared_ptr<stub::rpcclient::MetaServerClient> meta_client;
  std::shared_ptr<stub::rpcclient::MdsClient> mds_client;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEM_PACKAGE_H_
