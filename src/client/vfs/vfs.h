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

#include <json/value.h>

#include <cstdint>
#include <string>

#include "client/common/client_option.h"
#include "client/vfs/data_buffer.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "common/trace/itracer.h"
#include "common/types.h"

namespace dingofs {
namespace client {
namespace vfs {

struct VFSConfig {
  std::string mds_addrs;
  std::string mount_point;
  std::string fs_name;
  client::MetaSystemType metasystem_type;  // mds,memory,local
  std::string storage_info;
};

// NOT: all return value should sys error code in <errno.h>
class VFS {
 public:
  VFS() = default;

  virtual ~VFS() = default;

  virtual Status Start(const VFSConfig& vfs_conf, bool upgrade) = 0;

  virtual Status Stop(bool upgrade) = 0;

  virtual bool Dump(ContextSPtr ctx, Json::Value& value) = 0;

  virtual bool Load(ContextSPtr ctx, const Json::Value& value) = 0;

  virtual Status Lookup(ContextSPtr ctx, Ino parent, const std::string& name,
                        Attr* attr) = 0;

  virtual Status GetAttr(ContextSPtr ctx, Ino ino, Attr* attr) = 0;

  virtual Status SetAttr(ContextSPtr ctx, Ino ino, int set, const Attr& in_attr,
                         Attr* out_attr) = 0;

  virtual Status ReadLink(ContextSPtr ctx, Ino ino, std::string* link) = 0;

  virtual Status MkNod(ContextSPtr ctx, Ino parent, const std::string& name,
                       uint32_t uid, uint32_t gid, uint32_t mode, uint64_t dev,
                       Attr* attr) = 0;

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

  virtual Status Rename(ContextSPtr ctx, Ino old_parent,
                        const std::string& old_name, Ino new_parent,
                        const std::string& new_name) = 0;

  virtual Status Link(ContextSPtr ctx, Ino ino, Ino new_parent,
                      const std::string& new_name, Attr* attr) = 0;

  virtual Status Open(ContextSPtr ctx, Ino ino, int flags, uint64_t* fh) = 0;

  virtual Status Create(ContextSPtr ctx, Ino parent, const std::string& name,
                        uint32_t uid, uint32_t gid, uint32_t mode, int flags,
                        uint64_t* fh, Attr* attr) = 0;

  virtual Status Read(ContextSPtr ctx, Ino ino, DataBuffer* data_buffer,
                      uint64_t size, uint64_t offset, uint64_t fh,
                      uint64_t* out_rsize) = 0;

  virtual Status Write(ContextSPtr ctx, Ino ino, const char* buf, uint64_t size,
                       uint64_t offset, uint64_t fh, uint64_t* out_wsize) = 0;

  virtual Status Flush(ContextSPtr ctx, Ino ino, uint64_t fh) = 0;

  virtual Status Release(ContextSPtr ctx, Ino ino, uint64_t fh) = 0;

  virtual Status Fsync(ContextSPtr ctx, Ino ino, int datasync, uint64_t fh) = 0;

  virtual Status SetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                          const std::string& value, int flags) = 0;

  virtual Status GetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                          std::string* value) = 0;

  virtual Status RemoveXattr(ContextSPtr ctx, Ino ino,
                             const std::string& name) = 0;

  virtual Status ListXattr(ContextSPtr ctx, Ino ino,
                           std::vector<std::string>* xattrs) = 0;

  virtual Status MkDir(ContextSPtr ctx, Ino parent, const std::string& name,
                       uint32_t uid, uint32_t gid, uint32_t mode,
                       Attr* attr) = 0;

  virtual Status OpenDir(ContextSPtr ctx, Ino ino, uint64_t* fh) = 0;

  virtual Status ReadDir(ContextSPtr ctx, Ino ino, uint64_t fh, uint64_t offset,
                         bool with_attr, ReadDirHandler handler) = 0;

  virtual Status ReleaseDir(ContextSPtr ctx, Ino ino, uint64_t fh) = 0;

  virtual Status RmDir(ContextSPtr ctx, Ino parent,
                       const std::string& name) = 0;

  virtual Status StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) = 0;

  virtual Status Ioctl(ContextSPtr ctx, Ino ino, uint32_t uid, unsigned int cmd,
                       unsigned flags, const void* in_buf, size_t in_bufsz,
                       char* out_buf, size_t out_bufsz) = 0;

  virtual uint64_t GetFsId() = 0;

  virtual double GetAttrTimeout(const FileType& type) = 0;

  virtual double GetEntryTimeout(const FileType& type) = 0;

  virtual uint64_t GetMaxNameLength() = 0;

  // TODO: refactor this interface
  // used for fuse
  virtual FuseOption GetFuseOption() = 0;

  virtual ITracer* GetTracer() = 0;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_H_