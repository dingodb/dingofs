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

#ifndef DINGODB_CLIENT_VFS_META_META_SYSTEM_H
#define DINGODB_CLIENT_VFS_META_META_SYSTEM_H

#include <cstdint>
#include <string>
#include <vector>

#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "json/value.h"

namespace dingofs {
namespace client {
namespace vfs {

struct DumpOption {
  Ino ino{0};
  bool dir_iterator{false};
  bool file_session{false};
  bool handler{false};
  bool parent_memo{false};
  bool modify_time_memo{false};
  bool chunk_memo{false};
  bool mds_router{false};
  bool inode_cache{false};
  bool rpc{false};
  bool mds_discovery{false};
};

class MetaSystem {
 public:
  MetaSystem() = default;

  virtual ~MetaSystem() = default;

  virtual Status Init(bool upgrade) = 0;

  virtual void Stop(bool upgrade) = 0;

  virtual bool Dump(ContextSPtr ctx, Json::Value& value) = 0;

  virtual bool Dump(const DumpOption& options, Json::Value& value) = 0;

  virtual bool Load(ContextSPtr ctx, const Json::Value& value) = 0;

  virtual Status Lookup(ContextSPtr ctx, Ino parent, const std::string& name,
                        Attr* attr) = 0;

  // create a regular file in parent directory
  virtual Status MkNod(ContextSPtr ctx, Ino parent, const std::string& name,
                       uint32_t uid, uint32_t gid, uint32_t mode, uint64_t rdev,
                       Attr* attr) = 0;

  virtual Status Open(ContextSPtr ctx, Ino ino, int flags, uint64_t fh) = 0;

  virtual Status Create(ContextSPtr ctx, Ino parent, const std::string& name,
                        uint32_t uid, uint32_t gid, uint32_t mode, int flags,
                        Attr* attr, uint64_t fh) = 0;

  virtual Status Flush(ContextSPtr ctx, Ino ino, uint64_t fh) = 0;

  virtual Status Close(ContextSPtr ctx, Ino ino, uint64_t fh) = 0;

  /**
   * Read the slices of a file meta
   * @param ino the file to be read
   * @param index the chunk index
   * @param slices output
   */
  virtual Status ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                           uint64_t fh, std::vector<Slice>* slices,
                           uint64_t& version) = 0;

  virtual Status NewSliceId(ContextSPtr ctx, Ino ino, uint64_t* id) = 0;

  /**
   * Write the slices of a file meta
   * @param ino the file to be written
   * @param index the chunk index
   * @param slices the slices to be written
   */
  virtual Status WriteSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                            uint64_t fh, const std::vector<Slice>& slices) = 0;

  virtual Status AsyncWriteSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                                 uint64_t fh, const std::vector<Slice>& slices,
                                 DoneClosure done) = 0;
  // metasystem record write
  virtual Status Write(ContextSPtr ctx, Ino ino, uint64_t offset, uint64_t size,
                       uint64_t fh) = 0;

  /**
   * Hard link a file to a new parent directory
   * @param ino the file to be linked
   * @param new_parent the new parent directory
   * @param new_name the new name of the file
   * @param attr output
   */
  virtual Status Link(ContextSPtr ctx, Ino ino, Ino new_parent,
                      const std::string& new_name, Attr* attr) = 0;

  virtual Status Unlink(ContextSPtr ctx, Ino parent,
                        const std::string& name) = 0;

  /**
   * Create a symlink in parent directory
   * @param parent
   * @param name to be created
   * @param link the content of the symlink
   * @param attr output
   */
  virtual Status Symlink(ContextSPtr ctx, Ino parent, const std::string& name,
                         uint32_t uid, uint32_t gid, const std::string& link,
                         Attr* attr) = 0;

  virtual Status ReadLink(ContextSPtr ctx, Ino ino, std::string* link) = 0;

  virtual Status GetAttr(ContextSPtr ctx, Ino ino, Attr* attr) = 0;

  virtual Status SetAttr(ContextSPtr ctx, Ino ino, int set, const Attr& in_attr,
                         Attr* out_attr) = 0;

  virtual Status SetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                          const std::string& value, int flags) = 0;

  virtual Status GetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                          std::string* value) = 0;

  virtual Status RemoveXattr(ContextSPtr ctx, Ino ino,
                             const std::string& name) = 0;

  virtual Status ListXattr(ContextSPtr ctx, Ino ino,
                           std::vector<std::string>* xattrs) = 0;

  // create a directory in parent directory
  virtual Status MkDir(ContextSPtr ctx, Ino parent, const std::string& name,
                       uint32_t uid, uint32_t gid, uint32_t mode,
                       Attr* attr) = 0;

  virtual Status RmDir(ContextSPtr ctx, Ino parent,
                       const std::string& name) = 0;

  virtual Status OpenDir(ContextSPtr ctx, Ino ino, uint64_t fh) = 0;

  virtual Status ReadDir(ContextSPtr ctx, Ino ino, uint64_t fh, uint64_t offset,
                         bool with_attr, ReadDirHandler handler) = 0;

  virtual Status ReleaseDir(ContextSPtr ctx, Ino ino, uint64_t fh) = 0;

  virtual Status StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) = 0;

  virtual Status Rename(ContextSPtr ctx, Ino old_parent,
                        const std::string& old_name, Ino new_parent,
                        const std::string& new_name) = 0;

  virtual Status GetFsInfo(ContextSPtr ctx, FsInfo* fs_info) = 0;

  virtual bool GetDescription(Json::Value& value) = 0;
};

using MetaSystemPtr = std::shared_ptr<MetaSystem>;
using MetaSystemUPtr = std::unique_ptr<MetaSystem>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_META_META_SYSTEM_H