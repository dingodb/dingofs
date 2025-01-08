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

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEMV2_MDS_CLIENT_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEMV2_MDS_CLIENT_H_

#include <memory>

#include "dingofs/src/client/common/dynamic_config.h"
#include "dingofs/src/client/filesystem/meta.h"
#include "dingofs/src/client/filesystemv2/rpc.h"
#include "dingofs/src/mdsv2/common/status.h"

namespace dingofs {
namespace client {
namespace filesystem {

class MDSClient {
 public:
  MDSClient() = default;
  virtual ~MDSClient() = default;

  bool Init();
  void Destory();

  Status Lookup(uint32_t fs_id, uint64_t parent_ino, const std::string& name,
                EntryOut* entry_out);

  Status MkNod(uint32_t fs_id, uint64_t parent_ino, const std::string& name,
               uint32_t uid, uint32_t gid, mode_t mode, dev_t rdev,
               EntryOut* entry_out);
  Status MkDir(uint32_t fs_id, uint64_t parent_ino, const std::string& name,
               uint32_t uid, uint32_t gid, mode_t mode, dev_t rdev,
               EntryOut* entry_out);
  Status RmDir(uint32_t fs_id, uint64_t parent_ino, const std::string& name);

  Status Link(uint32_t fs_id, uint64_t parent_ino, uint64_t ino,
              const std::string& name);
  Status UnLink(uint32_t fs_id, uint64_t parent_ino, const std::string& name);
  Status Symlink(uint32_t fs_id, uint64_t parent_ino, const std::string& name,
                 uint32_t uid, uint32_t gid, const std::string& symlink,
                 EntryOut* entry_out);
  Status ReadLink(uint32_t fs_id, uint64_t ino, std::string& symlink);

  Status GetAttr(uint32_t fs_id, uint64_t ino, EntryOut* entry_out);
  Status SetAttr(uint32_t fs_id, uint64_t ino, struct stat* attr, int to_set,
                 AttrOut* attr_out);
  Status GetXAttr(uint32_t fs_id, uint64_t ino, const std::string& name,
                  std::string& value);
  Status SetXAttr(uint32_t fs_id, uint64_t ino, const std::string& name,
                  const std::string& value);
  Status ListXAttr(uint32_t fs_id, uint64_t ino,
                   std::map<std::string, std::string>& xattrs);

  Status Rename();

 private:
  RPCPtr rpc_;
};
using MDSClientPtr = std::shared_ptr<MDSClient>;

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEMV2_MDS_CLIENT_H_