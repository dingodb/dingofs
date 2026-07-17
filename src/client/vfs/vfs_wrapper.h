/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CLIENT_VFS_VFS_WRAPPER_H_
#define DINGOFS_CLIENT_VFS_VFS_WRAPPER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "client/vfs/vfs.h"
#include "common/meta.h"
#include "common/metrics/client/client.h"
#include "common/status.h"
#include "json/value.h"

namespace dingofs {
namespace client {

struct DingofsConfig {
  std::string mds_addrs;
  std::string mount_point;
  std::string fs_name;
  std::string metasystem_type;  // "mds", "memory", "local"
  std::string storage_info;
  // Filesystem-internal directory exposed as the local mountpoint root.
  // "/" means the whole filesystem (default).
  std::string subdir = "/";
};

struct Context {
  uint32_t uid;
  uint32_t gid;
  int32_t pid;
  uint32_t umask;

  std::string ToString() const {
    return fmt::format("pid({}) uid({}) gid({}) umask({})", pid, uid, gid,
                       umask);
  }
  std::string ToShortString() const {
    return fmt::format("{}:{}:{}", pid, uid, gid);
  }
};

class VFSWrapper {
 public:
  VFSWrapper() = default;

  ~VFSWrapper() = default;

  // Normal start: upgrade_from_pid = 0.
  // Graceful-upgrade start (new process taking over): upgrade_from_pid is the
  // PID of the old process. The new process will restore client state from
  // the old process's persisted state file and skip re-mounting the FS on MDS.
  Status Start(const DingofsConfig& config, int upgrade_from_pid = 0);

  // Stop the VFS. With handover=true, skip MDS unmount and persist state for
  // the new process. It is idempotent after success, so post-exit teardown does
  // not stop/dump twice.
  Status Stop(bool handover = false);

  Status GetInfo(std::string* info);

  double GetAttrTimeout(FileType type);

  double GetEntryTimeout(FileType type);

  uint64_t GetFsId();

  uint64_t GetMaxNameLength();

  Status Lookup(const Context& ctx, Ino parent, const std::string& name,
                Attr* attr);

  Status GetAttr(const Context& ctx, Ino ino, Attr* attr);

  Status SetAttr(const Context& ctx, Ino ino, int set, const Attr& in_attr,
                 Attr* out_attr);

  Status Fallocate(const Context& ctx, Ino ino, int mode, uint64_t offset,
                   uint64_t length);

  Status CopyFileRange(const Context& ctx, Ino src_ino, uint64_t src_off,
                       uint64_t src_fh, Ino dst_ino, uint64_t dst_off,
                       uint64_t dst_fh, uint64_t len, uint32_t flags,
                       uint64_t* bytes_copied);

  Status ReadLink(const Context& ctx, Ino ino, std::string* link);

  Status MkNod(const Context& ctx, Ino parent, const std::string& name,
               uint32_t mode, uint64_t dev, Attr* attr);

  Status Unlink(const Context& ctx, Ino parent, const std::string& name);

  Status Symlink(const Context& ctx, Ino parent, const std::string& name,
                 const std::string& link, Attr* attr);

  Status Rename(const Context& ctx, Ino old_parent, const std::string& old_name,
                Ino new_parent, const std::string& new_name);

  Status Link(const Context& ctx, Ino ino, Ino new_parent,
              const std::string& new_name, Attr* attr);

  Status Open(const Context& ctx, Ino ino, int flags, uint64_t* fh);

  Status Create(const Context& ctx, Ino parent, const std::string& name,
                uint32_t mode, int flags, uint64_t* fh, Attr* attr);

  Status Read(const Context& ctx, Ino ino, DataBuffer* data_buffer,
              uint64_t size, uint64_t offset, uint64_t fh, uint64_t* out_rsize);

  Status Write(const Context& ctx, Ino ino, const char* buf, uint64_t size,
               uint64_t offset, uint64_t fh, uint64_t* out_wsize);

  Status Flush(const Context& ctx, Ino ino, uint64_t fh);

  Status Release(const Context& ctx, Ino ino, uint64_t fh);

  Status Fsync(const Context& ctx, Ino ino, int datasync, uint64_t fh);

  Status SetXattr(const Context& ctx, Ino ino, const std::string& name,
                  const std::string& value, int flags);

  Status GetXattr(const Context& ctx, Ino ino, const std::string& name,
                  std::string* value);

  Status RemoveXattr(const Context& ctx, Ino ino, const std::string& name);

  Status ListXattr(const Context& ctx, Ino ino,
                   std::vector<std::string>* xattrs);

  Status MkDir(const Context& ctx, Ino parent, const std::string& name,
               uint32_t mode, Attr* attr);

  Status OpenDir(const Context& ctx, Ino ino, uint64_t* fh, bool& need_cache);

  Status ReadDir(const Context& ctx, Ino ino, uint64_t fh, uint64_t offset,
                 bool with_attr, ReadDirHandler handler);

  Status ReleaseDir(const Context& ctx, Ino ino, uint64_t fh);

  Status RmDir(const Context& ctx, Ino parent, const std::string& name);

  Status StatFs(const Context& ctx, Ino ino, FsStat* fs_stat);

  Status Ioctl(const Context& ctx, Ino ino, unsigned int cmd, unsigned flags,
               const void* in_buf, size_t in_bufsz, char* out_buf,
               size_t out_bufsz);

 private:
  bool Dump();

  bool Load(const Json::Value& value);

  std::atomic<bool> started_{false};
  std::unique_ptr<vfs::VFS> vfs_;

  std::unique_ptr<metrics::client::ClientOpMetric> client_op_metric_;

  uint32_t uid_{0};
  uint32_t gid_{0};
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_VFS_WRAPPER_H_
