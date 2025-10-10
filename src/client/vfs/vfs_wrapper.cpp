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

#include "client/vfs/vfs_wrapper.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <json/reader.h>
#include <json/writer.h>

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>

#include "blockaccess/block_access_log.h"
#include "cache/utils/logging.h"
#include "client/common/const.h"
#include "client/common/utils.h"
#include "client/fuse/fuse_upgrade_manager.h"
#include "client/vfs/access_log.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/metasystem/meta_log.h"
#include "client/vfs/vfs_impl.h"
#include "client/vfs/vfs_meta.h"
#include "common/define.h"
#include "common/status.h"
#include "metrics/client/client.h"
#include "metrics/metric_guard.h"
#include "options/client/option.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("VFSWrapper::" + std::string(__FUNCTION__))

const std::string kFdStatePath = "/tmp/dingo-fuse-state.json";

using ::dingofs::client::fuse::FuseUpgradeManager;
using metrics::ClientOpMetricGuard;
using metrics::VFSRWMetricGuard;
using metrics::client::VFSRWMetric;

static auto& g_rw_metric = VFSRWMetric::GetInstance();

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

static Status InitLog() {
  bool succ = dingofs::client::InitAccessLog(FLAGS_log_dir) &&
              dingofs::cache::InitCacheTraceLog(FLAGS_log_dir) &&
              blockaccess::InitBlockAccessLog(FLAGS_log_dir) &&
              dingofs::client::vfs::InitMetaLog(FLAGS_log_dir);

  CHECK(succ) << "init log failed, unexpected!";
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
  VFSOption vfs_option;
  if (vfs_conf.fs_type == "vfs" || vfs_conf.fs_type == "vfs_v1" ||
      vfs_conf.fs_type == "vfs_v2" || vfs_conf.fs_type == "vfs_dummy") {
    InitVFSOption(&conf_, &vfs_option);

    DINGOFS_RETURN_NOT_OK(InitLog());
  } else {
    LOG(ERROR) << "unsupported fs_type: " << vfs_conf.fs_type;
    return Status::InvalidParam("unsupported fs_type: " + vfs_conf.fs_type);
  }

  LOG(INFO) << "use vfs type: " << vfs_conf.fs_type;

  if (FLAGS_client_bthread_worker_num > 0) {
    bthread_setconcurrency(FLAGS_client_bthread_worker_num);
    LOG(INFO) << fmt::format(
        "set bthread concurrency({}) actual concurrency({}).",
        FLAGS_client_bthread_worker_num, bthread_getconcurrency());
  }

  client_op_metric_ = std::make_unique<metrics::client::ClientOpMetric>();

  vfs_ = std::make_unique<vfs::VFSImpl>(vfs_option);
  DINGOFS_RETURN_NOT_OK(vfs_->Start(vfs_conf));

  // load vfs state
  if (FuseUpgradeManager::GetInstance().GetFuseState() ==
      fuse::FuseUpgradeState::kFuseUpgradeNew) {
    if (!Load()) {
      return Status::InvalidParam("load vfs state fail");
    }
  }

  return Status::OK();
}

Status VFSWrapper::Stop() {
  VLOG(1) << "VFSStop";
  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("stop: %s", s.ToString()); });
  s = vfs_->Stop();

  if (FuseUpgradeManager::GetInstance().GetFuseState() ==
      fuse::FuseUpgradeState::kFuseUpgradeOld) {
    if (!Dump()) {
      return Status::InvalidParam("dump vfs state fail");
    }
  }

  return s;
}

bool VFSWrapper::Dump() {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());

  Json::Value root;
  if (!vfs_->Dump(span->GetContext(), root)) {
    LOG(ERROR) << "dump vfs state fail.";
    return false;
  }

  const std::string path = fmt::format("{}.{}", kFdStatePath, getpid());
  std::ofstream file(path);
  if (!file.is_open()) {
    LOG(ERROR) << "open dingo-fuse state file fail, file: " << path;
    return false;
  }

  Json::StreamWriterBuilder writer;
  file << Json::writeString(writer, root);
  file.close();

  LOG(INFO) << fmt::format("dump vfs state success, path({}).", path);

  return true;
}

bool VFSWrapper::Load() {
  int pid = FuseUpgradeManager::GetInstance().GetOldFusePid();

  const std::string path = fmt::format("{}.{}", kFdStatePath, pid);
  std::ifstream file(path);
  if (!file.is_open()) {
    LOG(ERROR) << "load dingo-fuse state file fail, file: " << path;
    return false;
  }

  Json::Value root;
  Json::CharReaderBuilder reader;
  std::string err;
  if (!Json::parseFromStream(reader, file, &root, &err)) {
    LOG(ERROR) << fmt::format("parse json fail, path({}) error({}).", path,
                              err);
    return false;
  }

  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());

  if (!vfs_->Load(span->GetContext(), root)) {
    LOG(ERROR) << "load vfs state fail.";
    return false;
  }

  LOG(INFO) << fmt::format("load vfs state success, path({}).", path);

  return true;
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
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSLookup parent: " << parent << " name: " << name;
  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("lookup (%d/%s): %s %s", parent, name,
                               s.ToString(), StrAttr(attr));
      },
      !IsInternalName(name));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opLookup, &client_op_metric_->opAll},
      !IsInternalName(name));

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->Lookup(span->GetContext(), parent, name, attr);
  VLOG(1) << "VFSLookup end parent: " << parent << " name: " << name
          << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::GetAttr(Ino ino, Attr* attr) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  auto ctx = span->GetContext();
  VLOG(1) << "VFSGetAttr ino: " << ino;
  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("getattr (%d): %s %d %s", ino, s.ToString(),
                               ctx->hit_cache, StrAttr(attr));
      },
      !IsInternalNode(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opGetAttr, &client_op_metric_->opAll},
      !IsInternalNode(ino));

  s = vfs_->GetAttr(ctx, ino, attr);
  VLOG(1) << "VFSGetAttr end ino: " << ino << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::SetAttr(Ino ino, int set, const Attr& in_attr,
                           Attr* out_attr) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSSetAttr ino: " << ino << " set: " << set;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("setattr (%d,0x%X): %s %s", ino, set, s.ToString(),
                           StrAttr(out_attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opSetAttr, &client_op_metric_->opAll});

  s = vfs_->SetAttr(span->GetContext(), ino, set, in_attr, out_attr);
  VLOG(1) << "VFSSetAttr end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::ReadLink(Ino ino, std::string* link) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSReadLink ino: " << ino;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("readlink (%d): %s %s", ino, s.ToString(), *link);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opReadLink, &client_op_metric_->opAll});

  s = vfs_->ReadLink(span->GetContext(), ino, link);
  VLOG(1) << "VFSReadLink end, status: " << s.ToString() << " link: " << *link;
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::MkNod(Ino parent, const std::string& name, uint32_t uid,
                         uint32_t gid, uint32_t mode, uint64_t dev,
                         Attr* attr) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
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

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->MkNod(span->GetContext(), parent, name, uid, gid, mode, dev, attr);
  VLOG(1) << "VFSMknod end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Unlink(Ino parent, const std::string& name) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSUnlink parent: " << parent << " name: " << name;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("unlink (%d,%s): %s", parent, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opUnlink, &client_op_metric_->opAll});

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->Unlink(span->GetContext(), parent, name);
  VLOG(1) << "VFSUnlink end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Symlink(Ino parent, const std::string& name, uint32_t uid,
                           uint32_t gid, const std::string& link, Attr* attr) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSSymlink parent: " << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " link: " << link;

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("symlink (%d,%s,%s): %s %s", parent, name, link,
                           s.ToString(), Attr2Str(*attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opSymlink, &client_op_metric_->opAll});

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(
        fmt::format("link name({}) too long", link.length()));
    return s;
  }

  s = vfs_->Symlink(span->GetContext(), parent, name, uid, gid, link, attr);
  VLOG(1) << "VFSSymlink end, status: " << s.ToString();
  return s;
}

Status VFSWrapper::Rename(Ino old_parent, const std::string& old_name,
                          Ino new_parent, const std::string& new_name) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSRename old_parent: " << old_parent << " old_name: " << old_name
          << " new_parent: " << new_parent << " new_name: " << new_name;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("rename (%d,%s,%d,%s): %s", old_parent, old_name,
                           new_parent, new_name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRename, &client_op_metric_->opAll});

  if (old_name.length() > vfs_->GetMaxNameLength() ||
      new_name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}|{}) too long",
                                        old_name.length(), new_name.length()));
    return s;
  }

  s = vfs_->Rename(span->GetContext(), old_parent, old_name, new_parent,
                   new_name);
  VLOG(1) << "VFSRename end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Link(Ino ino, Ino new_parent, const std::string& new_name,
                        Attr* attr) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSLink ino: " << ino << " new_parent: " << new_parent
          << " new_name: " << new_name;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("link (%d,%d,%s): %s %s", ino, new_parent, new_name,
                           s.ToString(), StrAttr(attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opLink, &client_op_metric_->opAll});

  uint64_t max_name_len = vfs_->GetMaxNameLength();

  if (new_name.length() > max_name_len) {
    LOG(WARNING) << "name too long, name: " << new_name
                 << ", maxNameLength: " << max_name_len;
    s = Status::NameTooLong("name too long, length: " +
                            std::to_string(new_name.length()));
    return s;
  }

  s = vfs_->Link(span->GetContext(), ino, new_parent, new_name, attr);
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::Open(Ino ino, int flags, uint64_t* fh) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSOpen ino: " << ino << " octal flags: " << std::oct << flags;
  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("open (%d): %s [fh:%d]", ino, s.ToString(), *fh);
      },
      !IsInternalNode(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opOpen, &client_op_metric_->opAll},
      !IsInternalNode(ino));

  s = vfs_->Open(span->GetContext(), ino, flags, fh);
  VLOG(1) << "VFSOpen end, status: " << s.ToString() << " fh: " << *fh;
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Create(Ino parent, const std::string& name, uint32_t uid,
                          uint32_t gid, uint32_t mode, int flags, uint64_t* fh,
                          Attr* attr) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSCreate parent: " << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " mode: " << mode
          << " octal flags: " << std::oct << flags;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("create (%d,%s): %s %s [fh:%d]", parent, name,
                           s.ToString(), StrAttr(attr), *fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opCreate, &client_op_metric_->opAll});

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->Create(span->GetContext(), parent, name, uid, gid, mode, flags, fh,
                   attr);

  VLOG(1) << "VFSCreate end, status: " << s.ToString()
          << " attr: " << Attr2Str(*attr);
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Read(Ino ino, char* buf, uint64_t size, uint64_t offset,
                        uint64_t fh, uint64_t* out_rsize) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());

  std::string trace_id = span->GetContext()->TraceId();
  VLOG(1) << fmt::format(
      "[{}] VFSRead ino: {}, buf: {}, size: {}, offset: {}, fh: {}", trace_id,
      ino, Char2Addr(buf), size, offset, fh);

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("read (%d,%d,%d): %s (%d) [fh:%d]", ino, size,
                               offset, s.ToString(), *out_rsize, fh);
      },
      !IsInternalNode(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRead, &client_op_metric_->opAll},
      !IsInternalNode(ino));
  VFSRWMetricGuard guard(&s, &g_rw_metric.read, out_rsize,
                         !IsInternalNode(ino));

  s = vfs_->Read(span->GetContext(), ino, buf, size, offset, fh, out_rsize);

  VLOG(1) << fmt::format(
      "[{}] VFSRead end ino: {}, buf: {}, size: {}, offset: {}, fh: {}, "
      "read_size: {}, status: {}",
      trace_id, ino, Char2Addr(buf), size, offset, fh, *out_rsize,
      s.ToString());

  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Write(Ino ino, const char* buf, uint64_t size,
                         uint64_t offset, uint64_t fh, uint64_t* out_wsize) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSWrite ino: " << ino << ", buf:  " << Char2Addr(buf)
          << ", size: " << size << " offset: " << offset << " fh: " << fh;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("write (%d,%d,%d): %s (%d) [fh:%d]", ino, size,
                           offset, s.ToString(), *out_wsize, fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opWrite, &client_op_metric_->opAll});

  VFSRWMetricGuard guard(&s, &g_rw_metric.write, out_wsize);

  s = vfs_->Write(span->GetContext(), ino, buf, size, offset, fh, out_wsize);
  VLOG(1) << "VFSWrite end ino: " << ino << ", buf:  " << Char2Addr(buf)
          << ", size: " << size << " offset: " << offset << " fh: " << fh
          << ", write_size: " << *out_wsize << ", status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Flush(Ino ino, uint64_t fh) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSFlush ino: " << ino << " fh: " << fh;
  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("flush (%d): %s [fh:%d]", ino, s.ToString(), fh);
      },
      !IsInternalNode(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opFlush, &client_op_metric_->opAll},
      !IsInternalNode(ino));

  s = vfs_->Flush(span->GetContext(), ino, fh);
  VLOG(1) << "VFSFlush end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Release(Ino ino, uint64_t fh) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSRelease ino: " << ino << " fh: " << fh;
  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("release (%d): %s [fh:%d]", ino, s.ToString(),
                               fh);
      },
      !IsInternalNode(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRelease, &client_op_metric_->opAll},
      !IsInternalNode(ino));

  s = vfs_->Release(span->GetContext(), ino, fh);
  VLOG(1) << "VFSRelease end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::Fsync(Ino ino, int datasync, uint64_t fh) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSFsync ino: " << ino << " datasync: " << datasync
          << " fh: " << fh;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("fsync (%d,%d): %s [fh:%d]", ino, datasync,
                           s.ToString(), fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opFsync, &client_op_metric_->opAll});

  s = vfs_->Fsync(span->GetContext(), ino, datasync, fh);
  VLOG(1) << "VFSFsync end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::SetXattr(Ino ino, const std::string& name,
                            const std::string& value, int flags) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSSetXattr ino: " << ino << " name: " << name
          << " value: " << value << " octal flags: " << std::oct << flags;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("setxattr (%d,%s): %s", ino, name, s.ToString());
  });

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  // NOTE: s is used by log guard
  s = vfs_->SetXattr(span->GetContext(), ino, name, value, flags);
  LOG(INFO) << "VFSSetXattr end, status: " << s.ToString();
  return s;
}

Status VFSWrapper::GetXattr(Ino ino, const std::string& name,
                            std::string* value) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  auto ctx = span->GetContext();
  VLOG(1) << "VFSGetXattr ino: " << ino << " name: " << name;
  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("getxattr (%d,%s): %s %d %s", ino, name,
                               s.ToString(), ctx->hit_cache, *value);
      },
      !IsInternalNode(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opGetXattr, &client_op_metric_->opAll},
      !IsInternalNode(ino));

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->GetXattr(ctx, ino, name, value);
  VLOG(1) << "VFSGetXattr end, value: " << *value
          << ", status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  if (value->empty()) {
    s = Status::NoData("no data");
  }

  return s;
}

Status VFSWrapper::RemoveXattr(Ino ino, const std::string& name) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSRemoveXattr ino: " << ino << " name: " << name;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("removexattr (%d,%s): %s", ino, name, s.ToString());
  });

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  // NOTE: s is used by log guard
  s = vfs_->RemoveXattr(span->GetContext(), ino, name);
  LOG(INFO) << "VFSSetXattr end, status: " << s.ToString();
  return s;
}

Status VFSWrapper::ListXattr(Ino ino, std::vector<std::string>* xattrs) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSListXattr ino: " << ino;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("listxattr (%d): %s %d", ino, s.ToString(),
                           xattrs->size());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opListXattr, &client_op_metric_->opAll});

  s = vfs_->ListXattr(span->GetContext(), ino, xattrs);
  VLOG(1) << "VFSListXattr end, status: " << s.ToString()
          << " xattrs: " << xattrs->size();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::MkDir(Ino parent, const std::string& name, uint32_t uid,
                         uint32_t gid, uint32_t mode, Attr* attr) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSMkDir parent ino: " << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " mode: " << mode;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("mkdir (%d,%s,%s:0%04o,%d,%d): %s %s", parent, name,
                           StrMode(mode), mode, uid, gid, s.ToString(),
                           StrAttr(attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opMkDir, &client_op_metric_->opAll});

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->MkDir(span->GetContext(), parent, name, uid, gid, S_IFDIR | mode,
                  attr);
  VLOG(1) << "VFSMkdir end, status: " << s.ToString()
          << " attr: " << Attr2Str(*attr);
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::OpenDir(Ino ino, uint64_t* fh) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSOpendir ino: " << ino;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("opendir (%d): %s [fh:%d]", ino, s.ToString(), *fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opOpenDir, &client_op_metric_->opAll});

  s = vfs_->OpenDir(span->GetContext(), ino, fh);
  VLOG(1) << "VFSOpendir end, ino: " << ino << " fh: " << *fh;
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::ReadDir(Ino ino, uint64_t fh, uint64_t offset,
                           bool with_attr, ReadDirHandler handler) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSReaddir ino: " << ino << " fh: " << fh << " offset: " << offset
          << " with_attr: " << (with_attr ? "true" : "false");
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("readdir %d: %s (%d) [fh:%d]", ino, s.ToString(),
                           offset, fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opReadDir, &client_op_metric_->opAll});

  s = vfs_->ReadDir(span->GetContext(), ino, fh, offset, with_attr, handler);
  VLOG(1) << "VFSReaddir end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::ReleaseDir(Ino ino, uint64_t fh) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSReleaseDir ino: " << ino << " fh: " << fh;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("releasedir %d: %s [fh:%d]", ino, s.ToString(), fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opReleaseDir, &client_op_metric_->opAll});

  s = vfs_->ReleaseDir(span->GetContext(), ino, fh);
  VLOG(1) << "VFSReleaseDir end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::RmDir(Ino parent, const std::string& name) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSRmdir parent: " << parent << " name: " << name;
  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("rmdir (%d,%s): %s", parent, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRmDir, &client_op_metric_->opAll});

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->RmDir(span->GetContext(), parent, name);
  VLOG(1) << "VFSRmdir end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::StatFs(Ino ino, FsStat* fs_stat) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  VLOG(1) << "VFSStatFs ino: " << ino;
  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("statfs (%d): %s", ino, s.ToString()); });

  s = vfs_->StatFs(span->GetContext(), ino, fs_stat);
  VLOG(1) << "VFSStatFs end, status: " << s.ToString()
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

FuseOption VFSWrapper::GetFuseOption() const { return vfs_->GetFuseOption(); }

}  // namespace vfs
}  // namespace client
}  // namespace dingofs