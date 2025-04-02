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

#ifndef DINGOFS_CLIENT_VFS_H_
#define DINGOFS_CLIENT_VFS_H_

#include <cstdint>
#include <functional>
#include <string>

#include "client/common/config.h"
#include "client/common/status.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

struct VFSConfig {
  std::string mount_point;
  std::string fs_name;
  std::string config_path;
  std::string fs_type;  // vfs_old/vfs_v1/vfs_v2/vfs_dummy
};

using ReadDirHandler = std::function<bool(const DirEntry& dir_entry)>;

// NOT: all return value should sys error code in <errno.h>
class VFS {
 public:
  VFS() = default;

  virtual ~VFS() = default;

  virtual Status Start(const VFSConfig& vfs_con) = 0;

  virtual Status Stop() = 0;

  virtual Status Lookup(Ino parent, const std::string& name, Attr* attr) = 0;

  virtual Status GetAttr(Ino ino, Attr* attr) = 0;

  virtual Status SetAttr(Ino ino, int set, const Attr& in_attr,
                         Attr* out_attr) = 0;

  virtual Status ReadLink(Ino ino, std::string* link) = 0;

  virtual Status MkNod(Ino parent, const std::string& name, uint32_t uid,
                       uint32_t gid, uint32_t mode, uint64_t dev,
                       Attr* attr) = 0;

  virtual Status Unlink(Ino parent, const std::string& name) = 0;

  /**
   * Create a symlink in parent directory
   * @param parent
   * @param name to be created
   * @param link the content of the symlink
   * @param attr output
   */
  virtual Status Symlink(Ino parent, const std::string& name, uint32_t uid,
                         uint32_t gid, const std::string& link, Attr* attr) = 0;

  virtual Status Rename(Ino old_parent, const std::string& old_name,
                        Ino new_parent, const std::string& new_name) = 0;

  virtual Status Link(Ino ino, Ino new_parent, const std::string& new_name,
                      Attr* attr) = 0;

  virtual Status Open(Ino ino, int flags, uint64_t* fh) = 0;

  virtual Status Create(Ino parent, const std::string& name, uint32_t uid,
                        uint32_t gid, uint32_t mode, int flags, uint64_t* fh,
                        Attr* attr) = 0;

  virtual Status Read(Ino ino, char* buf, uint64_t size, uint64_t offset,
                      uint64_t fh, uint64_t* out_rsize) = 0;

  virtual Status Write(Ino ino, const char* buf, uint64_t size, uint64_t offset,
                       uint64_t fh, uint64_t* out_wsize) = 0;

  virtual Status Flush(Ino ino, uint64_t fh) = 0;

  virtual Status Release(Ino ino, uint64_t fh) = 0;

  virtual Status Fsync(Ino ino, int datasync, uint64_t fh) = 0;

  virtual Status SetXattr(Ino ino, const std::string& name,
                          const std::string& value, int flags) = 0;

  virtual Status GetXattr(Ino ino, const std::string& name,
                          std::string* value) = 0;

  virtual Status ListXattr(Ino ino, std::vector<std::string>* xattrs) = 0;

  virtual Status MkDir(Ino parent, const std::string& name, uint32_t uid,
                       uint32_t gid, uint32_t mode, Attr* attr) = 0;

  virtual Status OpenDir(Ino ino, uint64_t* fh) = 0;

  virtual Status ReadDir(Ino ino, uint64_t fh, uint64_t offset, bool with_attr,
                         ReadDirHandler handler) = 0;

  virtual Status ReleaseDir(Ino ino, uint64_t fh) = 0;

  virtual Status RmDir(Ino parent, const std::string& name) = 0;

  virtual Status StatFs(Ino ino, FsStat* fs_stat) = 0;

  virtual uint64_t GetFsId() = 0;

  virtual double GetAttrTimeout(const FileType& type) = 0;

  virtual double GetEntryTimeout(const FileType& type) = 0;

  virtual uint64_t GetMaxNameLength() = 0;

  // TODO: refactor this interface
  // used for fuse
  virtual common::FuseOption GetFuseOption() = 0;

};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_H_