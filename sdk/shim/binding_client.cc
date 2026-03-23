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

#include "binding_client.h"

#include <cstdio>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <memory>

#include "common/flag.h"
#include "common/logging.h"

DECLARE_string(log_dir);
DECLARE_string(log_level);
DECLARE_int32(log_v);

namespace dingofs {
namespace client {

BindingClient::BindingClient() : vfs_(std::make_unique<VFSWrapper>()) {}

BindingClient::~BindingClient() = default;

Status BindingClient::Start(const BindingConfig& config) {
  // 1. Load conf file to populate gflags (overrides compiled-in defaults).
  if (!config.conf_file.empty()) {
    gflags::ReadFromFlagsFile(config.conf_file, "dingofs-binding", true);
  }

  // Remember whether log_dir was set by conf_file before step 2 runs.
  // ResetBrpcFlagDefaultValue() (step 2) will overwrite an empty FLAGS_log_dir
  // with ~/.dingofs/log, so we must capture the state beforehand.
  const bool conf_file_set_log_dir = !FLAGS_log_dir.empty();

  // 2. Apply dingofs-preferred brpc defaults for flags the user didn't set.
  FlagsHelper::ResetBrpcFlagDefaultValue();

  // 3. Init logging only when the caller delegates glog ownership to us.
  if (config.init_glog) {
    if (!config.log_dir.empty()) {
      // config.log_dir has highest priority.
      FLAGS_log_dir = config.log_dir;
    } else if (!conf_file_set_log_dir) {
      // Neither config.log_dir nor conf_file specified a log directory.
      // Use the system temp dir to match glog's own default behaviour,
      // rather than letting Logger::Init() redirect to ~/.dingofs/log/.
      FLAGS_log_dir = P_tmpdir;
    }
    // else: conf_file set log_dir — keep the value it loaded.
    if (!config.log_level.empty()) FLAGS_log_level = config.log_level;
    if (config.log_v != 0) FLAGS_log_v = config.log_v;
    dingofs::Logger::Init("dingofs-binding");
    self_inited_glog_ = true;
  }

  // 4. Build DingofsConfig and start the VFS.
  DingofsConfig c;
  c.mds_addrs = config.mds_addrs;
  c.fs_name = config.fs_name;
  c.mount_point = config.mount_point;
  c.metasystem_type = "mds";
  return vfs_->Start(c);
}

Status BindingClient::Stop() {
  Status s = vfs_->Stop();
  if (self_inited_glog_) {
    google::ShutdownGoogleLogging();
    self_inited_glog_ = false;
  }
  return s;
}

Status BindingClient::StatFs(Ino ino, FsStat* fs_stat) {
  return vfs_->StatFs(ino, fs_stat);
}

Status BindingClient::Lookup(Ino parent, const std::string& name, Attr* attr) {
  return vfs_->Lookup(parent, name, attr);
}

Status BindingClient::GetAttr(Ino ino, Attr* attr) {
  return vfs_->GetAttr(ino, attr);
}

Status BindingClient::SetAttr(Ino ino, int set, const Attr& in_attr,
                              Attr* out_attr) {
  return vfs_->SetAttr(ino, set, in_attr, out_attr);
}

Status BindingClient::MkDir(Ino parent, const std::string& name, uint32_t uid,
                            uint32_t gid, uint32_t mode, Attr* attr) {
  return vfs_->MkDir(parent, name, uid, gid, mode, attr);
}

Status BindingClient::RmDir(Ino parent, const std::string& name) {
  return vfs_->RmDir(parent, name);
}

Status BindingClient::OpenDir(Ino ino, uint64_t* fh, bool& need_cache) {
  return vfs_->OpenDir(ino, fh, need_cache);
}

Status BindingClient::ReadDir(Ino ino, uint64_t fh, uint64_t offset,
                              bool with_attr, ReadDirHandler handler) {
  return vfs_->ReadDir(ino, fh, offset, with_attr, handler);
}

Status BindingClient::ReleaseDir(Ino ino, uint64_t fh) {
  return vfs_->ReleaseDir(ino, fh);
}

Status BindingClient::Create(Ino parent, const std::string& name, uint32_t uid,
                             uint32_t gid, uint32_t mode, int flags,
                             uint64_t* fh, Attr* attr) {
  return vfs_->Create(parent, name, uid, gid, mode, flags, fh, attr);
}

Status BindingClient::MkNod(Ino parent, const std::string& name, uint32_t uid,
                            uint32_t gid, uint32_t mode, uint64_t dev,
                            Attr* attr) {
  return vfs_->MkNod(parent, name, uid, gid, mode, dev, attr);
}

Status BindingClient::Open(Ino ino, int flags, uint64_t* fh) {
  return vfs_->Open(ino, flags, fh);
}

Status BindingClient::Read(Ino ino, DataBuffer* data_buffer, uint64_t size,
                           uint64_t offset, uint64_t fh, uint64_t* out_rsize) {
  return vfs_->Read(ino, data_buffer, size, offset, fh, out_rsize);
}

Status BindingClient::Write(Ino ino, const char* buf, uint64_t size,
                            uint64_t offset, uint64_t fh,
                            uint64_t* out_wsize) {
  return vfs_->Write(ino, buf, size, offset, fh, out_wsize);
}

Status BindingClient::Flush(Ino ino, uint64_t fh) {
  return vfs_->Flush(ino, fh);
}

Status BindingClient::Fsync(Ino ino, int datasync, uint64_t fh) {
  return vfs_->Fsync(ino, datasync, fh);
}

Status BindingClient::Release(Ino ino, uint64_t fh) {
  return vfs_->Release(ino, fh);
}

Status BindingClient::Unlink(Ino parent, const std::string& name) {
  return vfs_->Unlink(parent, name);
}

Status BindingClient::Link(Ino ino, Ino new_parent, const std::string& new_name,
                           Attr* attr) {
  return vfs_->Link(ino, new_parent, new_name, attr);
}

Status BindingClient::Symlink(Ino parent, const std::string& name, uint32_t uid,
                              uint32_t gid, const std::string& link,
                              Attr* attr) {
  return vfs_->Symlink(parent, name, uid, gid, link, attr);
}

Status BindingClient::ReadLink(Ino ino, std::string* link) {
  return vfs_->ReadLink(ino, link);
}

Status BindingClient::Rename(Ino old_parent, const std::string& old_name,
                             Ino new_parent, const std::string& new_name) {
  return vfs_->Rename(old_parent, old_name, new_parent, new_name);
}

Status BindingClient::SetXattr(Ino ino, const std::string& name,
                               const std::string& value, int flags) {
  return vfs_->SetXattr(ino, name, value, flags);
}

Status BindingClient::GetXattr(Ino ino, const std::string& name,
                               std::string* value) {
  return vfs_->GetXattr(ino, name, value);
}

Status BindingClient::ListXattr(Ino ino, std::vector<std::string>* xattrs) {
  return vfs_->ListXattr(ino, xattrs);
}

Status BindingClient::RemoveXattr(Ino ino, const std::string& name) {
  return vfs_->RemoveXattr(ino, name);
}

uint64_t BindingClient::GetMaxNameLength() {
  return vfs_->GetMaxNameLength();
}

Status BindingClient::Ioctl(Ino ino, uint32_t uid, unsigned int cmd,
                            unsigned flags, const void* in_buf, size_t in_bufsz,
                            char* out_buf, size_t out_bufsz) {
  return vfs_->Ioctl(ino, uid, cmd, flags, in_buf, in_bufsz, out_buf,
                     out_bufsz);
}

// static
Status BindingClient::SetOption(const std::string& key,
                                const std::string& value) {
  if (gflags::SetCommandLineOption(key.c_str(), value.c_str()).empty()) {
    return Status::InvalidParam("unknown or invalid option: " + key + "=" +
                                value);
  }
  return Status::OK();
}

// static
Status BindingClient::GetOption(const std::string& key, std::string* value) {
  if (!gflags::GetCommandLineOption(key.c_str(), value)) {
    return Status::InvalidParam("unknown option: " + key);
  }
  return Status::OK();
}

// static
std::vector<OptionInfo> BindingClient::ListOptions() {
  std::vector<OptionInfo> result;
  for (const auto& f :
       FlagsHelper::GetAllGFlags("dingofs-sdk", kSdkFlagPatterns)) {
    result.push_back({f.name, f.type, f.default_value, f.description});
  }
  return result;
}

// static
void BindingClient::PrintOptions() {
  std::string cur_module;
  for (const auto& f :
       FlagsHelper::GetAllGFlags("dingofs-sdk", kSdkFlagPatterns)) {
    if (f.filename != cur_module) {
      cur_module = f.filename;
      const auto pos = cur_module.rfind('/');
      const std::string base =
          (pos == std::string::npos) ? cur_module : cur_module.substr(pos + 1);
      std::cout << "\n[" << base << "]\n";
    }
    std::cout << "  --" << f.name << "=" << f.type
              << "  (default: " << f.default_value << ")\n"
              << "    " << f.description << "\n";
  }
  std::cout << std::flush;
}

}  // namespace client
}  // namespace dingofs
