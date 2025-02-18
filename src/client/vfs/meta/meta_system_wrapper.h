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

#ifndef DINGODB_CLIENT_VFS_META_SYSTEM_WRAPPER_H
#define DINGODB_CLIENT_VFS_META_SYSTEM_WRAPPER_H

#include <map>

#include "client/common/status.h"
#include "client/vfs/meta/meta_system.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

class MetaSystemWrapper {
 public:
  MetaSystemWrapper() = default;

  ~MetaSystemWrapper() = default;

  Status Init();

  void UnInit();

  Status Lookup(Ino parent, const std::string& name, Attr* attr);

  // create a regular file in parent directory
  Status MkNod(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, uint64_t rdev, Attr* attr);

  Status Open(Ino ino, int flags, Attr* attr);

  Status Create(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
                uint32_t mode, int flags, Attr* attr);

  Status Close(Ino ino);

  /**
   * Read the slices of a file meta
   * @param ino the file to be read
   * @param index the chunk index
   * @param slices output
   */
  Status ReadSlice(Ino ino, uint64_t index, std::vector<Slice>* slices);

  Status NewSliceId(uint64_t* id);

  /**
   * Write the slices of a file meta
   * @param ino the file to be written
   * @param index the chunk index
   * @param slices the slices to be written
   */
  Status WriteSlice(Ino ino, uint64_t index, const std::vector<Slice>& slices);

  Status Unlink(Ino parent, const std::string& name);

  Status Rename(Ino old_parent, const std::string& old_name, Ino new_parent,
                const std::string& new_name);

  /**
   * Hard link a file to a new parent directory
   * @param ino the file to be linked
   * @param new_parent the new parent directory
   * @param new_name the new name of the file
   * @param attr output
   */
  Status Link(Ino ino, Ino new_parent, const std::string& new_name, Attr* attr);

  /**
   * Create a symlink in parent directory
   * @param parent
   * @param name to be created
   # @param uid the owner of the symlink
   * @param gid the group of the symlink
   * @param link the content of the symlink
   * @param attr output
   */
  Status Symlink(Ino parent, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& link, Attr* attr);

  Status ReadLink(Ino ino, std::string* link);

  Status GetAttr(Ino ino, Attr* attr);

  Status SetAttr(Ino ino, int set, const Attr& in_attr, Attr* out_attr);

  Status SetXattr(Ino ino, const std::string& name, const std::string& value,
                  int flags);

  Status GetXattr(Ino ino, const std::string& name, std::string* value);

  Status ListXattr(Ino ino, std::map<std::string, std::string>* xattrs);

  Status MkDir(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, uint64_t rdev, Attr* attr);

  Status RmDir(Ino parent, const std::string& name);

  Status OpenDir(Ino ino);

  Status NewDirHandler(Ino ino, bool with_attr, DirHandler** handler);

  Status ReleaseDir(Ino ino);

  Status StatFs(Ino ino, FsStat* fs_stat);

 private:
  std::unique_ptr<MetaSystem> meta_system_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_META_SYSTEM_WRAPPER_H
