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

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEMV2_FILESYSTEM_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEMV2_FILESYSTEM_H_

#include <cstdint>
#include <string>

#include "dingofs/proto/mdsv2.pb.h"
#include "dingofs/src/client/filesystem/error.h"
#include "dingofs/src/client/filesystem/meta.h"
#include "dingofs/src/client/filesystemv2/mds_client.h"
#include "dingofs/src/mdsv2/common/status.h"

namespace dingofs {
namespace client {
namespace filesystem {

class MDSV2FileSystem {
 public:
  MDSV2FileSystem() = default;
  virtual ~MDSV2FileSystem() = default;

  bool Init();
  void UnInit();

  Status Lookup(uint64_t parent_ino, const std::string& name,
                EntryOut* entry_out);

  Status MkNod(uint64_t parent_ino, const std::string& name, uint32_t uid,
               uint32_t gid, mode_t mode, dev_t rdev, EntryOut* entry_out);
  Status MkDir(uint64_t parent_ino, const std::string& name, uint32_t uid,
               uint32_t gid, mode_t mode, dev_t rdev, EntryOut* entry_out);
  Status RmDir(uint64_t parent_ino, const std::string& name);

  Status Link(uint64_t parent_ino, uint64_t ino, const std::string& name);
  Status UnLink(uint64_t parent_ino, const std::string& name);
  Status Symlink(uint64_t parent_ino, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& symlink, EntryOut* entry_out);
  Status ReadLink(uint64_t ino, std::string& symlink);

  Status GetAttr(uint64_t ino, EntryOut* entry_out);
  Status SetAttr(uint64_t ino, struct stat* attr, int to_set,
                 AttrOut* attr_out);
  DINGOFS_ERROR GetXAttr(uint64_t ino, const std::string& name, size_t size,
                         std::string& value);
  Status SetXAttr(uint64_t ino, const std::string& name,
                  const std::string& value);
  DINGOFS_ERROR ListXAttr(uint64_t ino, size_t size, std::string& out_names);

  Status Rename();

 private:
  pb::mdsv2::FsInfo fs_info_;

  MDSClientPtr mds_client_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEMV2_FILESYSTEM_H_
