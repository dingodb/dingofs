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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_FILESYSTEM_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_FILESYSTEM_H_

#include <cstdint>
#include <memory>
#include <string>

#include "client/vfs/meta/meta_system.h"
#include "client/vfs/meta/v2/dir_reader.h"
#include "client/vfs/meta/v2/mds_client.h"
#include "client/vfs/meta/v2/mds_discovery.h"
#include "client/vfs/vfs_meta.h"
#include "dingofs/mdsv2.pb.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class MDSV2FileSystem;
using MDSV2FileSystemPtr = std::shared_ptr<MDSV2FileSystem>;

class MDSV2FileSystem : public vfs::MetaSystem {
 public:
  MDSV2FileSystem(pb::mdsv2::FsInfo fs_info, const std::string& mount_path,
                  MDSDiscoveryPtr mds_discovery, MDSClientPtr mds_client);
  ~MDSV2FileSystem() override;

  static MDSV2FileSystemPtr New(pb::mdsv2::FsInfo fs_info,
                                const std::string& mount_path,
                                MDSDiscoveryPtr mds_discovery,
                                MDSClientPtr mds_client) {
    return std::make_shared<MDSV2FileSystem>(fs_info, mount_path, mds_discovery,
                                             mds_client);
  }

  Status Init() override;

  void UnInit() override;

  pb::mdsv2::FsInfo GetFsInfo() { return fs_info_; }

  Status StatFs(Ino ino, FsStat* fs_stat) override { return Status::OK(); };

  Status Lookup(Ino parent_ino, const std::string& name,
                Attr* out_attr) override;

  Status Create(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
                uint32_t mode, int flags, Attr* attr) override;

  Status MkNod(Ino parent_ino, const std::string& name, uint32_t uid,
               uint32_t gid, uint32_t mode, uint64_t rdev, Attr* attr) override;

  Status Open(Ino ino, int flags) override;
  Status Close(Ino ino) override;

  // Status Read(uint64_t ino, off_t off, size_t size, char* buf, size_t&
  // rsize); Status Write(uint64_t ino, off_t off, const char* buf, size_t size,
  //              size_t& wsize);

  Status ReadSlice(Ino ino, uint64_t index,
                   std::vector<Slice>* slices) override;
  Status NewSliceId(uint64_t* id) override;
  Status WriteSlice(Ino ino, uint64_t index,
                    const std::vector<Slice>& slices) override;

  Status MkDir(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, Attr* attr) override;
  Status RmDir(Ino parent, const std::string& name) override;

  Status OpenDir(Ino ino) override;

  // NOTE: caller own dir and the DirHandler should be deleted by caller
  Status NewDirHandler(Ino ino, bool with_attr, DirHandler** handler) override;

  Status Link(Ino ino, Ino new_parent, const std::string& new_name,
              Attr* attr) override;
  Status Unlink(Ino parent, const std::string& name) override;

  Status Symlink(Ino parent, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& link, Attr* attr) override;
  Status ReadLink(Ino ino, std::string* link) override;

  Status GetAttr(Ino ino, Attr* attr) override;
  Status SetAttr(Ino ino, int set, const Attr& attr, Attr* out_attr) override;
  Status GetXattr(Ino ino, const std::string& name,
                  std::string* value) override;
  Status SetXattr(Ino ino, const std::string& name, const std::string& value,
                  int flags) override;
  Status ListXattr(Ino ino, std::vector<std::string>* xattrs) override;

  Status Rename(Ino old_parent, const std::string& old_name, Ino new_parent,
                const std::string& new_name) override;

 private:
  bool SetRandomEndpoint();
  bool SetEndpoints();
  bool MountFs();
  bool UnmountFs();

  const std::string name_;
  const std::string mount_path_;

  pb::mdsv2::FsInfo fs_info_;

  MDSDiscoveryPtr mds_discovery_;

  MDSClientPtr mds_client_;

  DirReader dir_reader_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_FILESYSTEM_H_
