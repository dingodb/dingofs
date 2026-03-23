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

#ifndef DINGOFS_CLIENT_SHIM_BINDING_CLIENT_H_
#define DINGOFS_CLIENT_SHIM_BINDING_CLIENT_H_

#include <memory>
#include <string>
#include <vector>

#include "client/vfs/data_buffer.h"
#include "client/vfs/vfs_wrapper.h"
#include "common/meta.h"
#include "common/status.h"

namespace dingofs {
namespace client {

// BindingConfig holds all parameters needed to start a DingoFS client from
// language bindings (Python, C, etc.).
struct BindingConfig {
  std::string mds_addrs;
  std::string fs_name;
  std::string mount_point;

  // Optional: load a gflags configuration file (same format as
  // dingo-client --conf client.conf). Each line contains one --flag=value
  // entry and overrides all internal gflag defaults.
  std::string conf_file;

  // Whether BindingClient owns glog initialisation.
  //
  //   true  — Start() calls Logger::Init() to set up glog (log files, level,
  //            etc.) using the fields below.  Stop() calls
  //            ShutdownGoogleLogging().  Use this when the caller does not
  //            manage glog itself (pure C programs, Python, etc.).
  //
  //   false — Start() leaves glog untouched.  log_dir / log_level / log_v
  //           are ignored.  Use this when the caller has already initialised
  //           glog and wants DingoFS to share it.
  //
  // Default: false (safe for C++ callers that already run glog).
  // The C API (dingofs_mount) and Python SDK set this to true automatically.
  bool init_glog = false;

  // Log configuration — only effective when init_glog == true.
  std::string log_dir;
  std::string log_level;  // INFO WARNING ERROR FATAL
  int log_v = 0;
};

// Tunable option descriptor returned by ListOptions().
struct OptionInfo {
  std::string name;
  std::string type;
  std::string default_value;
  std::string description;
};

// BindingClient wraps VFSWrapper for use from language bindings.
// It handles gflags/logging setup before delegating to VFSWrapper.
class BindingClient {
 public:
  BindingClient();

  ~BindingClient();

  Status Start(const BindingConfig& config);

  Status Stop();

  Status StatFs(Ino ino, FsStat* fs_stat);

  Status Lookup(Ino parent, const std::string& name, Attr* attr);

  Status GetAttr(Ino ino, Attr* attr);

  Status SetAttr(Ino ino, int set, const Attr& in_attr, Attr* out_attr);

  Status MkDir(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, Attr* attr);

  Status RmDir(Ino parent, const std::string& name);

  Status OpenDir(Ino ino, uint64_t* fh, bool& need_cache);

  Status ReadDir(Ino ino, uint64_t fh, uint64_t offset, bool with_attr,
                 ReadDirHandler handler);

  Status ReleaseDir(Ino ino, uint64_t fh);

  Status Create(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
                uint32_t mode, int flags, uint64_t* fh, Attr* attr);

  Status MkNod(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, uint64_t dev, Attr* attr);

  Status Open(Ino ino, int flags, uint64_t* fh);

  Status Read(Ino ino, DataBuffer* data_buffer, uint64_t size, uint64_t offset,
              uint64_t fh, uint64_t* out_rsize);

  Status Write(Ino ino, const char* buf, uint64_t size, uint64_t offset,
               uint64_t fh, uint64_t* out_wsize);

  Status Flush(Ino ino, uint64_t fh);

  Status Fsync(Ino ino, int datasync, uint64_t fh);

  Status Release(Ino ino, uint64_t fh);

  Status Unlink(Ino parent, const std::string& name);

  Status Link(Ino ino, Ino new_parent, const std::string& new_name, Attr* attr);

  Status Symlink(Ino parent, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& link, Attr* attr);

  Status ReadLink(Ino ino, std::string* link);

  Status Rename(Ino old_parent, const std::string& old_name, Ino new_parent,
                const std::string& new_name);

  Status SetXattr(Ino ino, const std::string& name, const std::string& value,
                  int flags);

  Status GetXattr(Ino ino, const std::string& name, std::string* value);

  Status ListXattr(Ino ino, std::vector<std::string>* xattrs);

  Status RemoveXattr(Ino ino, const std::string& name);

  uint64_t GetMaxNameLength();

  Status Ioctl(Ino ino, uint32_t uid, unsigned int cmd, unsigned flags,
               const void* in_buf, size_t in_bufsz, char* out_buf,
               size_t out_bufsz);

  // Set/get a tunable parameter by name. Equivalent to setting --key=value
  // in the conf file. Operates on process-wide gflags state.
  static Status SetOption(const std::string& key, const std::string& value);
  static Status GetOption(const std::string& key, std::string* value);
  static std::vector<OptionInfo> ListOptions();
  static void PrintOptions();

 private:
  std::unique_ptr<VFSWrapper> vfs_;
  bool self_inited_glog_ = false;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_SHIM_BINDING_CLIENT_H_
