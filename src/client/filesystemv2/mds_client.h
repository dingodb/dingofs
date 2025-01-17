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

#include <absl/status/status.h>

#include <cstdint>
#include <memory>
#include <string>

#include "client/common/dynamic_config.h"
#include "client/filesystem/meta.h"
#include "client/filesystemv2/rpc.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"

namespace dingofs {
namespace client {
namespace filesystem {

class MDSClient;
using MDSClientPtr = std::shared_ptr<MDSClient>;

class MDSClient {
 public:
  MDSClient(RPCPtr rpc) : rpc_(rpc) {}
  virtual ~MDSClient() = default;

  static MDSClientPtr New(RPCPtr rpc) {
    return std::make_shared<MDSClient>(rpc);
  }

  bool Init();
  void Destory();

  void SetFsId(uint32_t fs_id) { fs_id_ = fs_id; }

  bool SetEndpoint(const std::string& ip, int port, bool is_default);

  Status GetFsInfo(const std::string& name, pb::mdsv2::FsInfo& fs_info);
  Status MountFs(const std::string& name,
                 const pb::mdsv2::MountPoint& mount_point);
  Status UmountFs(const std::string& name,
                  const pb::mdsv2::MountPoint& mount_point);

  Status Lookup(uint64_t parent_ino, const std::string& name,
                EntryOut& entry_out);

  Status MkNod(uint64_t parent_ino, const std::string& name, uint32_t uid,
               uint32_t gid, mode_t mode, dev_t rdev, EntryOut& entry_out);
  Status MkDir(uint64_t parent_ino, const std::string& name, uint32_t uid,
               uint32_t gid, mode_t mode, dev_t rdev, EntryOut& entry_out);
  Status RmDir(uint64_t parent_ino, const std::string& name);

  Status ReadDir(uint64_t ino, std::string& last_name, uint32_t limit,
                 bool with_attr,
                 std::vector<pb::mdsv2::ReadDirResponse::Entry>& entries);

  Status Open(uint64_t ino);
  Status Release(uint64_t ino);

  Status Link(uint64_t parent_ino, uint64_t ino, const std::string& name,
              EntryOut& entry_out);
  Status UnLink(uint64_t parent_ino, const std::string& name);
  Status Symlink(uint64_t parent_ino, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& symlink, EntryOut& entry_out);
  Status ReadLink(uint64_t ino, std::string& symlink);

  Status GetAttr(uint64_t ino, AttrOut& entry_out);
  Status SetAttr(uint64_t ino, struct stat* attr, int to_set,
                 AttrOut& attr_out);
  Status GetXAttr(uint64_t ino, const std::string& name, std::string& value);
  Status SetXAttr(uint64_t ino, const std::string& name,
                  const std::string& value);
  Status ListXAttr(uint64_t ino, std::map<std::string, std::string>& xattrs);

  Status Rename();

 private:
  uint32_t fs_id_;

  RPCPtr rpc_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEMV2_MDS_CLIENT_H_