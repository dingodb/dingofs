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

#include "client/vfs_wrapper/vfs_wrapper.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstdint>
#include <memory>

#include "cache/common/log.h"
#include "client/common/status.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/meta/meta_log.h"
#include "client/vfs/vfs_impl.h"
#include "client/vfs/vfs_meta.h"
#include "client/vfs_old/vfs_old.h"
#include "client/vfs_wrapper/access_log.h"
#include "common/rpc_stream.h"
#include "dataaccess/aws/s3_access_log.h"
#include "stub/metric/metric.h"
#include "stub/rpcclient/meta_access_log.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METRIC_GUARD(REQUEST)              \
  ClientOpMetricGuard clientOpMetricGuard( \
      &rc, {&client_op_metric_->op##REQUEST, &client_op_metric_->opAll});

static Status LoadConfig(const std::string& config_path,
                         utils::Configuration& conf) {
  conf.SetConfigPath(config_path);
  if (!conf.LoadConfig()) {
    return Status::InvalidParam("load config fail");
  }
  conf.PrintConfig();

  return Status::OK();
}

Status InitLog() {
  // Todo: remove InitMetaAccessLog when vfs is ready,  used by vfs old and old
  // metaserver
  bool succ = dingofs::client::InitAccessLog(FLAGS_log_dir) &&
              dingofs::cache::common::InitTraceLog(FLAGS_log_dir) &&
              dataaccess::aws::InitS3AccessLog(FLAGS_log_dir) &&
              dingofs::client::vfs::InitMetaLog(FLAGS_log_dir) &&
              dingofs::stub::InitMetaAccessLog(FLAGS_log_dir);

  CHECK(succ) << "Init log failed, unexpected!";
  return Status::OK();
}

static Status InitConfig(utils::Configuration& conf,
                         common::ClientOption& fuse_client_option) {
  // init fuse client option
  common::InitClientOption(&conf, &fuse_client_option);

  return Status::OK();
}

Status VFSWrapper::Start(const char* argv0, const VFSConfig& vfs_conf) {
  VLOG(1) << "VFSStart argv0: " << argv0;

  if (vfs_conf.fs_name.empty()) {
    LOG(ERROR) << "fs_name is empty";
    return Status::InvalidParam("fs_name is empty");
  }

  if (vfs_conf.mount_point.empty()) {
    LOG(ERROR) << "mount_point is empty";
    return Status::InvalidParam("mount_point is empty");
  }

  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("start: %s", s.ToString()); });

  // load config
  s = LoadConfig(vfs_conf.config_path, conf_);
  if (!s.ok()) {
    return s;
  }

  // init client option
  common::InitClientOption(&conf_, &fuse_client_option_);

  // init log
  s = InitLog();
  if (!s.ok()) {
    return s;
  }

  int32_t bthread_worker_num =
      dingofs::client::common::FLAGS_bthread_worker_num;
  if (bthread_worker_num > 0) {
    bthread_setconcurrency(bthread_worker_num);
    LOG(INFO) << "set bthread concurrency to " << bthread_worker_num
              << " actual concurrency:" << bthread_getconcurrency();
  }

  LOG(INFO) << "use vfs type: " << vfs_conf.fs_type;

  client_op_metric_ = std::make_unique<stub::metric::ClientOpMetric>();
  if (vfs_conf.fs_type == "vfs" || vfs_conf.fs_type == "vfs_v1" ||
      vfs_conf.fs_type == "vfs_v2" || vfs_conf.fs_type == "vfs_dummy") {
    vfs_ = std::make_unique<vfs::VFSImpl>(fuse_client_option_);

  } else {
    vfs_ = std::make_unique<vfs::VFSOld>(fuse_client_option_);
  }

  return vfs_->Start(vfs_conf);
}

Status VFSWrapper::Stop() {
  VLOG(1) << "VFSStop";
  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("stop: %s", s.ToString()); });
  s = vfs_->Stop();
  return s;
}

void VFSWrapper::Init() {
  VLOG(1) << "VFSInit";
  AccessLogGuard log([&]() { return "init : OK"; });
}

void VFSWrapper::Destory() {
  VLOG(1) << "VFSDestroy";
  AccessLogGuard log([&]() { return "destroy: OK"; });
}

double VFSWrapper::GetAttrTimeout(const FileType& type) {
  return vfs_->GetAttrTimeout(type);
}

double VFSWrapper::GetEntryTimeout(const FileType& type) {
  return vfs_->GetEntryTimeout(type);
}

Status VFSWrapper::Lookup(Ino parent, const std::string& name, Attr* attr) {
  VLOG(1) << "VFSLookup parent: " << parent << " name: " << name;

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("lookup (%d/%s): %s %s", parent, name, s.ToString(),
                           StrAttr(attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opLookup, &client_op_metric_->opAll});

  if (name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->Lookup(parent, name, attr);
  VLOG(1) << "VFSLookup end parent: " << parent << " name: " << name
          << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::GetAttr(Ino ino, Attr* attr) {
  VLOG(1) << "VFSGetAttr inodeId=" << ino;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("getattr (%d): %s %s", ino, s.ToString(),
                           StrAttr(attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opGetAttr, &client_op_metric_->opAll});

  s = vfs_->GetAttr(ino, attr);
  VLOG(1) << "VFSGetAttr end inodeId=" << ino << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::SetAttr(Ino ino, int set, const Attr& in_attr,
                           Attr* out_attr) {
  VLOG(1) << "VFSSetAttr inodeId=" << ino << " set: " << set;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("setattr (%d,0x%X): %s %s", ino, set, s.ToString(),
                           StrAttr(out_attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opSetAttr, &client_op_metric_->opAll});

  s = vfs_->SetAttr(ino, set, in_attr, out_attr);
  VLOG(1) << "VFSSetAttr end inodeId=" << ino << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::ReadLink(Ino ino, std::string* link) {
  VLOG(1) << "VFSReadLink inodeId=" << ino;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("readlink (%d): %s %s", ino, s.ToString(), *link);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opReadLink, &client_op_metric_->opAll});

  s = vfs_->ReadLink(ino, link);
  VLOG(1) << "VFSReadLink end inodeId=" << ino << " status: " << s.ToString()
          << " link: " << *link;
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::MkNod(Ino parent, const std::string& name, uint32_t uid,
                         uint32_t gid, uint32_t mode, uint64_t dev,
                         Attr* attr) {
  VLOG(1) << "VFSMknod parent: " << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " mode: " << mode
          << " dev: " << dev;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("mknod (%d,%s,%s:0%04o): %s %s", parent, name,
                           StrMode(mode), mode, s.ToString(), StrAttr(attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opMkNod, &client_op_metric_->opAll});

  if (name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->MkNod(parent, name, uid, gid, mode, dev, attr);
  VLOG(1) << "VFSMknod end parent: " << parent << " name: " << name
          << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Unlink(Ino parent, const std::string& name) {
  VLOG(1) << "VFSUnlink parent: " << parent << " name: " << name;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("unlink (%d,%s): %s", parent, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opUnlink, &client_op_metric_->opAll});

  if (name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->Unlink(parent, name);
  VLOG(1) << "VFSUnlink end parent: " << parent << " name: " << name
          << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Symlink(Ino parent, const std::string& name, uint32_t uid,
                           uint32_t gid, const std::string& link, Attr* attr) {
  VLOG(1) << "VFSSymlink parent: " << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " link: " << link;

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("symlink (%d,%s,%s): %s %s", parent, name, link,
                           s.ToString(), Attr2Str(*attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opSymlink, &client_op_metric_->opAll});

  if (name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->Symlink(parent, name, uid, gid, link, attr);
  VLOG(1) << "VFSSymlink end parent: " << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " link: " << link
          << " status: " << s.ToString();
  return s;
}

Status VFSWrapper::Rename(Ino old_parent, const std::string& old_name,
                          Ino new_parent, const std::string& new_name) {
  VLOG(1) << "VFSRename old_parent: " << old_parent << " old_name: " << old_name
          << " new_parent: " << new_parent << " new_name: " << new_name;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("rename (%d,%s,%d,%s): %s", old_parent, old_name,
                           new_parent, new_name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRename, &client_op_metric_->opAll});

  if (old_name.length() > fuse_client_option_.fileSystemOption.maxNameLength ||
      new_name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
    s = Status::NameTooLong(fmt::format("name({}|{}) too long",
                                        old_name.length(), new_name.length()));
    return s;
  }

  s = vfs_->Rename(old_parent, old_name, new_parent, new_name);
  VLOG(1) << "VFSRename end old_parent: " << old_parent
          << " old_name: " << old_name << " new_parent: " << new_parent
          << " new_name: " << new_name << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Link(Ino ino, Ino new_parent, const std::string& new_name,
                        Attr* attr) {
  VLOG(1) << "VFSLink inodeId=" << ino << " new_parent: " << new_parent
          << " new_name: " << new_name;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("link (%d,%d,%s): %s %s", ino, new_parent, new_name,
                           s.ToString(), StrAttr(attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opLink, &client_op_metric_->opAll});

  s = vfs_->Link(ino, new_parent, new_name, attr);
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Open(Ino ino, int flags, uint64_t* fh) {
  VLOG(1) << "VFSOpen inodeId=" << ino << " flags: " << flags;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("open (%d): %s [fh:%d]", ino, s.ToString(), *fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opOpen, &client_op_metric_->opAll});

  s = vfs_->Open(ino, flags, fh);
  VLOG(1) << "VFSOpen end inodeId=" << ino << " flags: " << flags
          << " status: " << s.ToString() << " fh: " << *fh;
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Create(Ino parent, const std::string& name, uint32_t uid,
                          uint32_t gid, uint32_t mode, int flags, uint64_t* fh,
                          Attr* attr) {
  VLOG(1) << "VFSCreate parent: " << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " mode: " << mode
          << " flags: " << flags;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("create (%d,%s): %s %s [fh:%d]", parent, name,
                           s.ToString(), StrAttr(attr), *fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opCreate, &client_op_metric_->opAll});

  if (name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->Create(parent, name, uid, gid, mode, flags, fh, attr);

  VLOG(1) << "VFSCreate end parent: " << parent << " name: " << name
          << " fh: " << *fh << " status: " << s.ToString()
          << " attr: " << Attr2Str(*attr);
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Read(Ino ino, char* buf, uint64_t size, uint64_t offset,
                        uint64_t fh, uint64_t* out_rsize) {
  VLOG(1) << "VFSRead inodeId=" << ino << " size: " << size
          << " offset: " << offset << " fh: " << fh;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("read (%d,%d,%d): %s (%d)", ino, size, offset,
                           s.ToString(), *out_rsize);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRead, &client_op_metric_->opAll});

  s = vfs_->Read(ino, buf, size, offset, fh, out_rsize);
  VLOG(1) << "VFSRead end inodeId=" << ino << " parma_size: " << size
          << " offset: " << offset << " fh: " << fh
          << ", read_size: " << *out_rsize << ", status : " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Write(Ino ino, const char* buf, uint64_t size,
                         uint64_t offset, uint64_t fh, uint64_t* out_wsize) {
  VLOG(1) << "VFSWrite inodeId=" << ino << " size: " << size
          << " offset: " << offset << " fh: " << fh;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("write (%d,%d,%d,%d): %s (%d)", ino, size, offset,
                           fh, s.ToString(), *out_wsize);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opWrite, &client_op_metric_->opAll});

  s = vfs_->Write(ino, buf, size, offset, fh, out_wsize);
  VLOG(1) << "VFSWrite end inodeId=" << ino << " size: " << size
          << " offset: " << offset << " fh: " << fh
          << " write_size: " << *out_wsize << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Flush(Ino ino, uint64_t fh) {
  VLOG(1) << "VFSFlush inodeId=" << ino << " fh: " << fh;
  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("flush (%d): %s", ino, s.ToString()); });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opFlush, &client_op_metric_->opAll});

  s = vfs_->Flush(ino, fh);
  VLOG(1) << "VFSFlush end inodeId=" << ino << " fh: " << fh
          << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Release(Ino ino, uint64_t fh) {
  VLOG(1) << "VFSRelease inodeId=" << ino << " fh: " << fh;
  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("release (%d): %s", ino, s.ToString()); });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRelease, &client_op_metric_->opAll});

  s = vfs_->Release(ino, fh);
  VLOG(1) << "VFSRelease end inodeId=" << ino << " fh: " << fh
          << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Fsync(Ino ino, int datasync, uint64_t fh) {
  VLOG(1) << "VFSFsync inodeId=" << ino << " datasync: " << datasync
          << " fh: " << fh;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("fsync (%d,%d): %s", ino, datasync, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opFsync, &client_op_metric_->opAll});

  s = vfs_->Fsync(ino, datasync, fh);
  VLOG(1) << "VFSFsync end inodeId=" << ino << " datasync: " << datasync
          << " fh: " << fh << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::SetXattr(Ino ino, const std::string& name,
                            const std::string& value, int flags) {
  VLOG(1) << "VFSSetXattr inodeId=" << ino << " name: " << name;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("setxattr (%d,%s): %s", ino, name, s.ToString());
  });

  if (name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  // NOTE: rc is used by log guard
  s = vfs_->SetXattr(ino, name, value, flags);
  LOG(INFO) << "VFSSetXattr end inodeId=" << ino << " name: " << name
            << " value: " << value << " status: " << s.ToString();
  return s;
}

Status VFSWrapper::GetXattr(Ino ino, const std::string& name,
                            std::string* value) {
  VLOG(1) << "VFSGetXattr inodeId=" << ino << " name: " << name;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("getxattr (%d,%s): %s", ino, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opGetXattr, &client_op_metric_->opAll});

  if (name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->GetXattr(ino, name, value);
  VLOG(1) << "VFSGetXattr end inodeId=" << ino << " name: " << name
          << " value: " << *value << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  if (value->empty()) {
    return Status::NoData("no data");
  }

  return s;
}

Status VFSWrapper::ListXattr(Ino ino, std::vector<std::string>* xattrs) {
  VLOG(1) << "VFSListXattr inodeId=" << ino;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("listxattr (%d): %s %d", ino, s.ToString(),
                           xattrs->size());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opListXattr, &client_op_metric_->opAll});

  s = vfs_->ListXattr(ino, xattrs);
  VLOG(1) << "VFSListXattr end inodeId=" << ino << " status: " << s.ToString()
          << " xattrs: " << xattrs->size();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::MkDir(Ino parent, const std::string& name, uint32_t uid,
                         uint32_t gid, uint32_t mode, Attr* attr) {
  VLOG(1) << "VFSMkDir parent inodeId=" << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " mode: " << mode;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("mkdir (%d,%s,%s:0%04o): %s %s", parent, name,
                           StrMode(mode), mode, s.ToString(), StrAttr(attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opMkDir, &client_op_metric_->opAll});

  if (name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->MkDir(parent, name, uid, gid, S_IFDIR | mode, attr);
  VLOG(1) << "VFSMkdir end parent inodeId=" << parent << " name: " << name
          << " status: " << s.ToString() << " attr: " << Attr2Str(*attr);
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::OpenDir(Ino ino, uint64_t* fh) {
  VLOG(1) << "VFSOpendir inodeId=" << ino;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("opendir (%d): %s [fh:%d]", ino, s.ToString(), *fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opOpenDir, &client_op_metric_->opAll});

  s = vfs_->OpenDir(ino, fh);
  VLOG(1) << "VFSOpendir end inodeId=" << ino << " fh: " << *fh;
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::ReadDir(Ino ino, uint64_t fh, uint64_t offset,
                           bool with_attr, ReadDirHandler handler) {
  VLOG(1) << "VFSReaddir inodeId=" << ino << " fh: " << fh
          << " offset: " << offset
          << " with_attr: " << (with_attr ? "true" : "false");
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("readdir (%d,%d): %s (%d)", ino, fh, s.ToString(),
                           offset);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opReadDir, &client_op_metric_->opAll});

  s = vfs_->ReadDir(ino, fh, offset, with_attr, handler);
  VLOG(1) << "VFSReaddir end inodeId=" << ino << " fh: " << fh
          << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::ReleaseDir(Ino ino, uint64_t fh) {
  VLOG(1) << "VFSReleaseDir inodeId=" << ino << " fh: " << fh;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("releasedir (%d,%d): %s", ino, fh, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opReleaseDir, &client_op_metric_->opAll});

  s = vfs_->ReleaseDir(ino, fh);
  VLOG(1) << "VFSReleaseDir end inodeId=" << ino << " fh: " << fh
          << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::RmDir(Ino parent, const std::string& name) {
  VLOG(1) << "VFSRmdir parent: " << parent << " name: " << name;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("rmdir (%d,%s): %s", parent, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRmDir, &client_op_metric_->opAll});

  if (name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->RmDir(parent, name);
  VLOG(1) << "VFSRmdir end parent: " << parent << " name: " << name
          << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::StatFs(Ino ino, FsStat* fs_stat) {
  VLOG(1) << "VFSStatFs inodeId=" << ino;
  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("statfs (%d): %s", ino, s.ToString()); });

  s = vfs_->StatFs(ino, fs_stat);
  VLOG(1) << "VFSStatFs end inodeId=" << ino << " status: " << s.ToString()
          << " fs_stat: " << FsStat2Str(*fs_stat);

  return s;
}

uint64_t VFSWrapper::GetFsId() {
  uint64_t fs_id = vfs_->GetFsId();
  VLOG(6) << "VFSGetFsId fs_id: " << fs_id;
  return fs_id;
}

uint64_t VFSWrapper::GetMaxNameLength() {
  uint64_t max_name_length = vfs_->GetMaxNameLength();
  VLOG(6) << "VFSGetMaxNameLength max_name_length: " << max_name_length;
  return max_name_length;
}

common::FuseOption VFSWrapper::GetFuseOption() const {
  return vfs_->GetFuseOption();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs