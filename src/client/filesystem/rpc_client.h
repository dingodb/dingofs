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
 * Project: Curve
 * Created Date: 2023-03-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEM_RPC_CLIENT_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEM_RPC_CLIENT_H_

#include <memory>
#include <string>

#include "dingofs/metaserver.pb.h"
#include "client/common/config.h"
#include "client/filesystem/dir_cache.h"
#include "client/filesystem/meta.h"
#include "client/filesystem/package.h"

namespace dingofs {
namespace client {
namespace filesystem {

class RPCClient {
 public:
  RPCClient(common::RPCOption option, ExternalMember member);

  DINGOFS_ERROR GetAttr(Ino ino, pb::metaserver::InodeAttr* attr);

  DINGOFS_ERROR Lookup(Ino parent, const std::string& name, EntryOut* entryOut);

  DINGOFS_ERROR ReadDir(Ino ino, std::shared_ptr<DirEntryList>* entries);

  DINGOFS_ERROR Open(Ino ino, std::shared_ptr<InodeWrapper>* inode);

 private:
  common::RPCOption option_;
  std::shared_ptr<InodeCacheManager> inodeManager_;
  std::shared_ptr<DentryCacheManager> dentryManager_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEM_RPC_CLIENT_H_
