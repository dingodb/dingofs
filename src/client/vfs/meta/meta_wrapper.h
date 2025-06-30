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

#ifndef DINGODB_CLIENT_VFS_META_WRAPPER_H
#define DINGODB_CLIENT_VFS_META_WRAPPER_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "client/vfs/handle/dir_iterator.h"
#include "client/vfs/meta/meta_system.h"

namespace dingofs {
namespace client {
namespace vfs {

class MetaWrapper : public MetaSystem {
 public:
  MetaWrapper(std::unique_ptr<MetaSystem> meta_system)
      : target_(std::move(meta_system)) {};

  ~MetaWrapper() override = default;

  Status Init() override;

  void UnInit() override;

  Status Lookup(Ino parent, const std::string& name, Attr* attr) override;

  Status MkNod(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, uint64_t rdev, Attr* attr) override;

  Status Open(Ino ino, int flags, uint64_t fh) override;

  Status Create(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
                uint32_t mode, int flags, Attr* attr, uint64_t fh) override;

  Status Close(Ino ino, uint64_t fh) override;

  Status MkDir(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, Attr* attr) override;

  Status RmDir(Ino parent, const std::string& name) override;

  Status OpenDir(Ino ino) override;

  // NOTE: caller own dir and the DirHandler should be deleted by caller
  DirIterator* NewDirIterator(Ino ino) override;

  Status Link(Ino ino, Ino new_parent, const std::string& new_name,
              Attr* attr) override;

  Status Unlink(Ino parent, const std::string& name) override;

  Status Symlink(Ino parent, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& link, Attr* att) override;

  Status ReadLink(Ino ino, std::string* link) override;

  Status GetAttr(Ino ino, Attr* attr) override;

  Status SetAttr(Ino ino, int set, const Attr& in_attr,
                 Attr* out_attr) override;

  Status GetXattr(Ino ino, const std::string& name,
                  std::string* value) override;

  Status SetXattr(Ino ino, const std::string& name, const std::string& value,
                  int flags) override;

  Status ListXattr(Ino ino, std::vector<std::string>* xattrs) override;

  Status Rename(Ino old_parent, const std::string& old_name, Ino new_parent,
                const std::string& new_name) override;

  Status ReadSlice(Ino ino, uint64_t index,
                   std::vector<Slice>* slices) override;

  Status NewSliceId(Ino ino, uint64_t* id) override;

  Status WriteSlice(Ino ino, uint64_t index,
                    const std::vector<Slice>& slices) override;

  Status StatFs(Ino ino, FsStat* fs_stat) override;

  Status GetFsInfo(FsInfo* fs_info) override;

  // TODO : need implemented
  bool Dump(const std::string& path) override { return true; };

  // TODO : need implemented
  bool Load(const std::string& path) override { return true; };

 private:
  std::unique_ptr<MetaSystem> target_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_META_WRAPPER_H