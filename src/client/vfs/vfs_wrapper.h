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

#ifndef DINGOFS_CLIENT_VFS_WRAPPER_H_
#define DINGOFS_CLIENT_VFS_WRAPPER_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "client/vfs/data_buffer.h"
#include "client/vfs/vfs.h"
#include "common/metrics/client/client.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSWrapper {
 public:
  VFSWrapper() = default;

  ~VFSWrapper() = default;

  Status Start(const char* argv0, const VFSConfig& vfs_conf);

  Status Stop();

  // for fuse op init, may remove in future
  void Init();

  // for fuse op destroy, may remove in future
  void Destory();

  double GetAttrTimeout(const FileType& type);

  double GetEntryTimeout(const FileType& type);

  Status Lookup(Ino parent, const std::string& name, Attr* attr);

  Status GetAttr(Ino ino, Attr* attr);

  Status SetAttr(Ino ino, int set, const Attr& in_attr, Attr* out_attr);

  Status ReadLink(Ino ino, std::string* link);

  Status MkNod(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, uint64_t dev, Attr* attr);

  Status Unlink(Ino parent, const std::string& name);

  /**
   * Create a symlink in parent directory
   * @param parent
   * @param name to be created
   * @param link the content of the symlink
   * @param attr output
   */
  Status Symlink(Ino parent, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& link, Attr* attr);

  Status Rename(Ino old_parent, const std::string& old_name, Ino new_parent,
                const std::string& new_name);

  Status Link(Ino ino, Ino new_parent, const std::string& new_name, Attr* attr);

  Status Open(Ino ino, int flags, uint64_t* fh);

  Status Create(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
                uint32_t mode, int flags, uint64_t* fh, Attr* attr);

  Status Read(Ino ino, DataBuffer* data_buffer, uint64_t size, uint64_t offset,
              uint64_t fh, uint64_t* out_rsize);

  Status Write(Ino ino, const char* buf, uint64_t size, uint64_t offset,
               uint64_t fh, uint64_t* out_wsize);

  Status Flush(Ino ino, uint64_t fh);

  Status Release(Ino ino, uint64_t fh);

  Status Fsync(Ino ino, int datasync, uint64_t fh);

  Status SetXattr(Ino ino, const std::string& name, const std::string& value,
                  int flags);

  Status GetXattr(Ino ino, const std::string& name, std::string* value);

  Status RemoveXattr(Ino ino, const std::string& name);

  Status ListXattr(Ino ino, std::vector<std::string>* xattrs);

  Status MkDir(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, Attr* attr);

  Status OpenDir(Ino ino, uint64_t* fh, bool& need_cache);

  Status ReadDir(Ino ino, uint64_t fh, uint64_t offset, bool with_attr,
                 ReadDirHandler handler);

  Status ReleaseDir(Ino ino, uint64_t fh);

  Status RmDir(Ino parent, const std::string& name);

  Status Ioctl(Ino ino, uint32_t uid, unsigned int cmd, unsigned flags,
               const void* in_buf, size_t in_bufsz, char* out_buf,
               size_t out_bufsz);

  Status StatFs(Ino ino, FsStat* fs_stat);

  uint64_t GetFsId();

  uint64_t GetMaxNameLength();

 private:
  bool Dump();
  bool Load(const Json::Value& value);

  std::unique_ptr<VFS> vfs_;
  std::unique_ptr<metrics::client::ClientOpMetric> client_op_metric_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_WRAPPER_H_
