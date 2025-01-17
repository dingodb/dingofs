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

#include "client/filesystem/error.h"
#include "client/filesystem/meta.h"
#include "client/filesystemv2/dir_reader.h"
#include "client/filesystemv2/mds_client.h"
#include "client/filesystemv2/mds_discovery.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"

namespace dingofs {
namespace client {
namespace filesystem {

class MDSV2FileSystem {
 public:
  MDSV2FileSystem(const std::string& name, const std::string& mount_path,
                  MDSDiscoveryPtr mds_discovery, MDSClientPtr mds_client);
  virtual ~MDSV2FileSystem();

  using PBInode = pb::mdsv2::Inode;
  using PBDentry = pb::mdsv2::Dentry;

  using ReadDirHandler = std::function<bool(const std::string&, uint64_t)>;
  using ReadDirPlusHandler =
      std::function<bool(const std::string&, const PBInode&)>;

  bool Init();
  void UnInit();

  pb::mdsv2::FsInfo GetFsInfo() { return fs_info_; }

  Status Lookup(uint64_t parent_ino, const std::string& name,
                EntryOut& entry_out);

  Status MkNod(uint64_t parent_ino, const std::string& name, uint32_t uid,
               uint32_t gid, mode_t mode, dev_t rdev, EntryOut& entry_out);
  Status Open(uint64_t ino);
  Status Release(uint64_t ino);
  Status Read(uint64_t ino, off_t off, size_t size, char* buf, size_t& rsize);
  Status Write(uint64_t ino, off_t off, const char* buf, size_t size,
               size_t& wsize);
  // just sync data
  Status Flush(uint64_t ino);
  // sync data and metadata, if data_sync=1 then just sync data
  Status Fsync(uint64_t ino, int data_sync);

  Status MkDir(uint64_t parent_ino, const std::string& name, uint32_t uid,
               uint32_t gid, mode_t mode, dev_t rdev, EntryOut& entry_out);
  Status RmDir(uint64_t parent_ino, const std::string& name);
  Status OpenDir(uint64_t ino, uint64_t& fh);
  Status ReadDir(uint64_t fh, uint64_t ino, ReadDirHandler handler);
  Status ReadDirPlus(uint64_t fh, uint64_t ino, ReadDirPlusHandler handler);
  Status ReleaseDir(uint64_t ino, uint64_t fh);

  Status Link(uint64_t ino, uint64_t new_parent_ino,
              const std::string& new_name, EntryOut& entry_out);
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
  Status ListXAttr(uint64_t ino, size_t size, std::string& out_names);

  Status Rename(uint64_t parent_ino, const std::string& name,
                uint64_t new_parent_ino, const std::string& new_name);

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

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEMV2_FILESYSTEM_H_
