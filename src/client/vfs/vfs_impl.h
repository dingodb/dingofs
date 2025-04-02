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

#ifndef DINGOFS_CLIENT_VFS_IMPL_H_
#define DINGOFS_CLIENT_VFS_IMPL_H_

#include <atomic>
#include <memory>

#include "client/common/config.h"
#include "client/vfs/handle_manager.h"
#include "client/vfs/meta/meta_system.h"
#include "client/vfs/vfs.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSImpl : public VFS {
 public:
  VFSImpl(const common::ClientOption& fuse_client_option)
      : fuse_client_option_(fuse_client_option) {};

  ~VFSImpl() override = default;

  Status Start(const VFSConfig& vfs_conf) override;

  Status Stop() override;

  double GetAttrTimeout(const FileType& type) override;

  double GetEntryTimeout(const FileType& type) override;

  Status Lookup(Ino parent, const std::string& name, Attr* attr) override;

  Status GetAttr(Ino ino, Attr* attr) override;

  Status SetAttr(Ino ino, int set, const Attr& in_attr,
                 Attr* out_attr) override;

  Status ReadLink(Ino ino, std::string* link) override;

  Status MkNod(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, uint64_t dev, Attr* attr) override;

  Status Unlink(Ino parent, const std::string& name) override;

  Status Symlink(Ino parent, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& link, Attr* attr) override;

  Status Rename(Ino old_parent, const std::string& old_name, Ino new_parent,
                const std::string& new_name) override;

  Status Link(Ino ino, Ino new_parent, const std::string& new_name,
              Attr* attr) override;

  Status Open(Ino ino, int flags, uint64_t* fh) override;

  Status Create(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
                uint32_t mode, int flags, uint64_t* fh, Attr* attr) override;

  Status Read(Ino ino, char* buf, uint64_t size, uint64_t offset, uint64_t fh,
              uint64_t* out_rsize) override;

  Status Write(Ino ino, const char* buf, uint64_t size, uint64_t offset,
               uint64_t fh, uint64_t* out_wsize) override;

  Status Flush(Ino ino, uint64_t fh) override;

  Status Release(Ino ino, uint64_t fh) override;

  Status Fsync(Ino ino, int datasync, uint64_t fh) override;

  Status SetXattr(Ino ino, const std::string& name, const std::string& value,
                  int flags) override;

  Status GetXattr(Ino ino, const std::string& name,
                  std::string* value) override;

  Status ListXattr(Ino ino, std::vector<std::string>* xattrs) override;

  Status MkDir(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, Attr* attr) override;

  Status OpenDir(Ino ino, uint64_t* fh) override;

  Status ReadDir(Ino ino, uint64_t fh, uint64_t offset, bool with_attr,
                 ReadDirHandler handler) override;

  Status ReleaseDir(Ino ino, uint64_t fh) override;

  Status RmDir(Ino parent, const std::string& name) override;

  Status StatFs(Ino ino, FsStat* fs_stat) override;

  uint64_t GetFsId() override;

  uint64_t GetMaxNameLength() override;

  common::FuseOption GetFuseOption() override {
    return fuse_client_option_.fuse_option;
  }

 private:
  std::atomic_bool started_{false};

  common::ClientOption fuse_client_option_;

  std::unique_ptr<MetaSystem> meta_system_;
  std::unique_ptr<HandleManager> handle_manager_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_IMPL_H_