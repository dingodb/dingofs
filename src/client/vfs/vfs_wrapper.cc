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

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>

#include "cache/utils/logging.h"
#include "client/fuse/fuse_upgrade_manager.h"
#include "client/vfs/access_log.h"
#include "client/vfs/blockstore/block_store_access_log.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/metasystem/meta_log.h"
#include "client/vfs/vfs_impl.h"
#include "client/vfs/vfs_meta.h"
#include "common/blockaccess/block_access_log.h"
#include "common/const.h"
#include "common/logging.h"
#include "common/metrics/client/client.h"
#include "common/metrics/metric_guard.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/types.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "json/reader.h"
#include "json/writer.h"
#include "utils/uuid.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("VFSWrapper::" + std::string(__FUNCTION__))

const std::string kFdStatePath = "/tmp/dingo-client-state.json";

using ::dingofs::client::fuse::FuseUpgradeManager;
using metrics::ClientOpMetricGuard;
using metrics::VFSRWMetricGuard;
using metrics::client::VFSRWMetric;

static auto& g_rw_metric = VFSRWMetric::GetInstance();

#define METRIC_GUARD(REQUEST)              \
  ClientOpMetricGuard clientOpMetricGuard( \
      &rc, {&client_op_metric_->op##REQUEST, &client_op_metric_->opAll});

static Status InitLog() {
  const std::string log_dir = Logger::LogDir();
  bool succ = dingofs::client::InitAccessLog(log_dir) &&
              dingofs::cache::InitCacheTraceLog(log_dir) &&
              blockaccess::InitBlockAccessLog(log_dir) &&
              dingofs::client::vfs::InitMetaLog(log_dir) &&
              dingofs::client::vfs::InitBlockStoreAccessLog(log_dir);

  CHECK(succ) << "init log failed, unexpected!";
  return Status::OK();
}

static bool LoadStateFile(Json::Value& root) {
  int pid = FuseUpgradeManager::GetInstance().GetOldFusePid();

  const std::string path = fmt::format("{}.{}", kFdStatePath, pid);
  std::ifstream file(path);
  if (!file.is_open()) {
    LOG(ERROR) << fmt::format("open state file fail, file: {}", path);
    return false;
  }

  std::string err;
  Json::CharReaderBuilder reader;
  if (!Json::parseFromStream(reader, file, &root, &err)) {
    LOG(ERROR) << fmt::format("parse json fail, path({}) error({}).", path,
                              err);
    return false;
  }

  LOG(INFO) << fmt::format("load state success, path({}).", path);

  return true;
}

Status VFSWrapper::Start(const char* argv0, const VFSConfig& vfs_conf) {
  LOG(INFO) << "start vfs wrapper: " << argv0;

  if (vfs_conf.fs_name.empty()) {
    return Status::InvalidParam("fs_name is empty");
  }

  if (vfs_conf.mount_point.empty()) {
    return Status::InvalidParam("mount_point is empty");
  }
  if (vfs_conf.metasystem_type != MetaSystemType::MDS &&
      vfs_conf.metasystem_type != MetaSystemType::LOCAL &&
      vfs_conf.metasystem_type != MetaSystemType::MEMORY) {
    return Status::InvalidParam(
        "unsupported metaystem_type " +
        MetaSystemTypeToString(vfs_conf.metasystem_type));
  }

  LOG(INFO) << "use vfs type: "
            << MetaSystemTypeToString(vfs_conf.metasystem_type);

  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("start: %s", s.ToString()); });

  DINGOFS_RETURN_NOT_OK(InitLog());

  if (FLAGS_vfs_bthread_worker_num > 0) {
    bthread_setconcurrency(FLAGS_vfs_bthread_worker_num);
    LOG(INFO) << fmt::format(
        "set bthread concurrency({}) actual concurrency({}).",
        FLAGS_vfs_bthread_worker_num, bthread_getconcurrency());
  }

  client_op_metric_ = std::make_unique<metrics::client::ClientOpMetric>();

  bool is_upgrade = (FuseUpgradeManager::GetInstance().GetFuseState() ==
                     fuse::FuseUpgradeState::kFuseUpgradeNew);

  Json::Value root;
  if (is_upgrade && !LoadStateFile(root)) {
    return Status::InvalidParam("load vfs state fail");
  }

  // client id
  const std::string hostname = Helper::GetHostName();
  if (hostname.empty()) return Status::Internal("get hostname fail");

  ClientId client_id(utils::GenerateUUID(), hostname,
                     FLAGS_vfs_dummy_server_port, vfs_conf.mount_point);
  if (is_upgrade) client_id.Load(root);
  CHECK(!client_id.ID().empty()) << "client id is empty.";

  LOG(INFO) << "client id: " << client_id.Description();

  // vfs start
  vfs_ = std::make_unique<vfs::VFSImpl>(client_id);
  DINGOFS_RETURN_NOT_OK(vfs_->Start(vfs_conf, is_upgrade));

  // load vfs state
  if (is_upgrade && !Load(root)) {
    return Status::InvalidParam("load vfs state fail");
  }

  uid_ = dingofs::Helper::GetOriginalUid();
  gid_ = dingofs::Helper::GetOriginalGid();

  return Status::OK();
}

void VFSWrapper::Init() {
  LOG(INFO) << "init vfs wrapper.";
  AccessLogGuard log([&]() { return "init vfs wrapper: OK"; });
}

Status VFSWrapper::Stop() {
  LOG(INFO) << "stop vfs wrapper.";

  const bool is_upgrade = (FuseUpgradeManager::GetInstance().GetFuseState() ==
                           fuse::FuseUpgradeState::kFuseUpgradeOld);

  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("stop: %s", s.ToString()); });
  s = vfs_->Stop(is_upgrade);

  if (is_upgrade && !Dump()) {
    return Status::InvalidParam("dump vfs state fail");
  }

  return s;
}

bool VFSWrapper::Dump() {
  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Dump");

  Json::Value root;
  if (!vfs_->Dump(SpanScope::GetContext(span), root)) {
    LOG(ERROR) << "dump vfs state fail.";
    return false;
  }

  const std::string path = fmt::format("{}.{}", kFdStatePath, getpid());
  std::ofstream file(path);
  if (!file.is_open()) {
    LOG(ERROR) << "open dingo-client state file fail, file: " << path;
    return false;
  }

  Json::StreamWriterBuilder writer;
  file << Json::writeString(writer, root);
  file.close();

  LOG(INFO) << fmt::format("dump vfs state success, path({}).", path);

  return true;
}

bool VFSWrapper::Load(const Json::Value& value) {
  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Load");

  if (!vfs_->Load(SpanScope::GetContext(span), value)) {
    LOG(ERROR) << "load vfs state fail.";
    return false;
  }

  return true;
}

double VFSWrapper::GetAttrTimeout(const FileType& type) {
  return vfs_->GetAttrTimeout(type);
}

double VFSWrapper::GetEntryTimeout(const FileType& type) {
  return vfs_->GetEntryTimeout(type);
}

Status VFSWrapper::Lookup(Ino parent, const std::string& name, Attr* attr) {
  VLOG(2) << "VFSLookup parent: " << parent << " name: " << name;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Lookup");

  Status s;
  AccessLogGuard log(
      [&]() {
        if (s.ok()) {
          return absl::StrFormat("lookup (%d/%s): %s %s", parent, name,
                                 s.ToString(), StrAttr(attr));
        } else {
          return absl::StrFormat("lookup (%d/%s): %s", parent, name,
                                 s.ToString());
        }
      },
      !IsInternalName(name));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opLookup, &client_op_metric_->opAll},
      !IsInternalName(name));

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  s = vfs_->Lookup(SpanScope::GetContext(span), parent, name, attr);
  VLOG(2) << "VFSLookup end parent: " << parent << " name: " << name
          << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::GetAttr(Ino ino, Attr* attr) {
  VLOG(2) << "VFSGetAttr ino: " << ino;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::GetAttr");
  auto ctx = SpanScope::GetContext(span);

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("getattr (%d): %s %s %s", ino, s.ToString(),
                               ctx->hit_cache ? "true" : "false",
                               StrAttr(attr));
      },
      !IsInternalNode(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opGetAttr, &client_op_metric_->opAll},
      !IsInternalNode(ino));

  s = vfs_->GetAttr(ctx, ino, attr);
  VLOG(2) << "VFSGetAttr end ino: " << ino << " status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  if (ino == kRootIno) {
    attr->uid = uid_;
    attr->gid = gid_;
  }

  return s;
}

Status VFSWrapper::SetAttr(Ino ino, int set, const Attr& in_attr,
                           Attr* out_attr) {
  VLOG(2) << "VFSSetAttr ino: " << ino << " set: " << set;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::SetAttr");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("setattr (%d,0x%X): %s %s", ino, set, s.ToString(),
                           StrAttr(out_attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opSetAttr, &client_op_metric_->opAll});

  s = vfs_->SetAttr(SpanScope::GetContext(span), ino, set, in_attr, out_attr);
  VLOG(2) << "VFSSetAttr end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::ReadLink(Ino ino, std::string* link) {
  VLOG(2) << "VFSReadLink ino: " << ino;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::ReadLink");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("readlink (%d): %s %s", ino, s.ToString(), *link);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opReadLink, &client_op_metric_->opAll});

  s = vfs_->ReadLink(SpanScope::GetContext(span), ino, link);
  VLOG(2) << "VFSReadLink end, status: " << s.ToString() << " link: " << *link;
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::MkNod(Ino parent, const std::string& name, uint32_t uid,
                         uint32_t gid, uint32_t mode, uint64_t dev,
                         Attr* attr) {
  VLOG(2) << "VFSMknod parent: " << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " mode: " << mode
          << " dev: " << dev;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::MkNod");

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

  s = vfs_->MkNod(SpanScope::GetContext(span), parent, name, uid, gid, mode,
                  dev, attr);
  VLOG(2) << "VFSMknod end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::Unlink(Ino parent, const std::string& name) {
  VLOG(2) << "VFSUnlink parent: " << parent << " name: " << name;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Unlink");

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

  s = vfs_->Unlink(SpanScope::GetContext(span), parent, name);
  VLOG(2) << "VFSUnlink end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::Symlink(Ino parent, const std::string& name, uint32_t uid,
                           uint32_t gid, const std::string& link, Attr* attr) {
  VLOG(2) << "VFSSymlink parent: " << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " link: " << link;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Symlink");

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

  s = vfs_->Symlink(SpanScope::GetContext(span), parent, name, uid, gid, link,
                    attr);
  VLOG(2) << "VFSSymlink end, status: " << s.ToString();

  return s;
}

Status VFSWrapper::Rename(Ino old_parent, const std::string& old_name,
                          Ino new_parent, const std::string& new_name) {
  VLOG(2) << "VFSRename old_parent: " << old_parent << " old_name: " << old_name
          << " new_parent: " << new_parent << " new_name: " << new_name;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Rename");

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

  s = vfs_->Rename(SpanScope::GetContext(span), old_parent, old_name,
                   new_parent, new_name);
  VLOG(2) << "VFSRename end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::Link(Ino ino, Ino new_parent, const std::string& new_name,
                        Attr* attr) {
  VLOG(2) << "VFSLink ino: " << ino << " new_parent: " << new_parent
          << " new_name: " << new_name;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Link");

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

  s = vfs_->Link(SpanScope::GetContext(span), ino, new_parent, new_name, attr);
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::Open(Ino ino, int flags, uint64_t* fh) {
  VLOG(2) << "VFSOpen ino: " << ino << " octal flags: " << std::oct << flags;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Open");

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("open (%d): %s [fh:%d]", ino, s.ToString(), *fh);
      },
      !IsInternalNode(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opOpen, &client_op_metric_->opAll},
      !IsInternalNode(ino));

  s = vfs_->Open(SpanScope::GetContext(span), ino, flags, fh);
  VLOG(2) << "VFSOpen end, status: " << s.ToString() << " fh: " << *fh;
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::Create(Ino parent, const std::string& name, uint32_t uid,
                          uint32_t gid, uint32_t mode, int flags, uint64_t* fh,
                          Attr* attr) {
  VLOG(2) << "VFSCreate parent: " << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " mode: " << mode
          << " octal flags: " << std::oct << flags;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Create");

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

  s = vfs_->Create(SpanScope::GetContext(span), parent, name, uid, gid, mode,
                   flags, fh, attr);

  VLOG(2) << "VFSCreate end, status: " << s.ToString()
          << " attr: " << Attr2Str(*attr);
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::Read(Ino ino, DataBuffer* data_buffer, uint64_t size,
                        uint64_t offset, uint64_t fh, uint64_t* out_rsize) {
  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Read");
  std::string session_id = SpanScope::GetSessionID(span);

  VLOG(2) << fmt::format("[{}] VFSRead ino: {}, size: {}, offset: {}, fh: {}",
                         session_id, ino, size, offset, fh);

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

  s = vfs_->Read(SpanScope::GetContext(span), ino, data_buffer, size, offset,
                 fh, out_rsize);

  VLOG(2) << fmt::format(
      "[{}] VFSRead end ino: {},  size: {}, offset: {}, fh: {}, "
      "read_size: {}, status: {}",
      session_id, ino, size, offset, fh, *out_rsize, s.ToString());

  if (!s.ok()) {
    op_metric.FailOp();
  }

  SpanScope::SetStatus(span, s);

  return s;
}

Status VFSWrapper::Write(Ino ino, const char* buf, uint64_t size,
                         uint64_t offset, uint64_t fh, uint64_t* out_wsize) {
  VLOG(2) << "VFSWrite ino: " << ino << ", buf:  " << Helper::Char2Addr(buf)
          << ", size: " << size << " offset: " << offset << " fh: " << fh;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Write");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("write (%d,%d,%d): %s (%d) [fh:%d]", ino, size,
                           offset, s.ToString(), *out_wsize, fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opWrite, &client_op_metric_->opAll});

  VFSRWMetricGuard guard(&s, &g_rw_metric.write, out_wsize);

  s = vfs_->Write(SpanScope::GetContext(span), ino, buf, size, offset, fh,
                  out_wsize);
  VLOG(2) << "VFSWrite end ino: " << ino << ", buf:  " << Helper::Char2Addr(buf)
          << ", size: " << size << " offset: " << offset << " fh: " << fh
          << ", write_size: " << *out_wsize << ", status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::Flush(Ino ino, uint64_t fh) {
  VLOG(2) << "VFSFlush ino: " << ino << " fh: " << fh;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Flush");

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("flush (%d): %s [fh:%d]", ino, s.ToString(), fh);
      },
      !IsInternalNode(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opFlush, &client_op_metric_->opAll},
      !IsInternalNode(ino));

  s = vfs_->Flush(SpanScope::GetContext(span), ino, fh);
  VLOG(2) << "VFSFlush end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::Release(Ino ino, uint64_t fh) {
  VLOG(2) << "VFSRelease ino: " << ino << " fh: " << fh;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Release");

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

  s = vfs_->Release(SpanScope::GetContext(span), ino, fh);
  VLOG(2) << "VFSRelease end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::Fsync(Ino ino, int datasync, uint64_t fh) {
  VLOG(2) << "VFSFsync ino: " << ino << " datasync: " << datasync
          << " fh: " << fh;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Fsync");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("fsync (%d,%d): %s [fh:%d]", ino, datasync,
                           s.ToString(), fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opFsync, &client_op_metric_->opAll});

  s = vfs_->Fsync(SpanScope::GetContext(span), ino, datasync, fh);
  VLOG(2) << "VFSFsync end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::SetXattr(Ino ino, const std::string& name,
                            const std::string& value, int flags) {
  VLOG(2) << "VFSSetXattr ino: " << ino << " name: " << name
          << " value: " << value << " octal flags: " << std::oct << flags;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::SetXattr");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("setxattr (%d,%s): %s", ino, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opSetAttr, &client_op_metric_->opAll},
      !IsInternalNode(ino));

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  // NOTE: s is used by log guard
  s = vfs_->SetXattr(SpanScope::GetContext(span), ino, name, value, flags);
  LOG(INFO) << "VFSSetXattr end, status: " << s.ToString();

  return s;
}

Status VFSWrapper::GetXattr(Ino ino, const std::string& name,
                            std::string* value) {
  VLOG(2) << "VFSGetXattr ino: " << ino << " name: " << name;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::GetXattr");
  auto ctx = SpanScope::GetContext(span);

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("getxattr (%d,%s): %s %s %s", ino, name,
                               s.ToString(), ctx->hit_cache ? "true" : "false",
                               *value);
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
  VLOG(2) << "VFSGetXattr end, value: " << *value
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
  VLOG(2) << "VFSRemoveXattr ino: " << ino << " name: " << name;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::RemoveXattr");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("removexattr (%d,%s): %s", ino, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRemoveXattr, &client_op_metric_->opAll},
      !IsInternalNode(ino));

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  // NOTE: s is used by log guard
  s = vfs_->RemoveXattr(SpanScope::GetContext(span), ino, name);
  LOG(INFO) << "VFSSetXattr end, status: " << s.ToString();

  return s;
}

Status VFSWrapper::ListXattr(Ino ino, std::vector<std::string>* xattrs) {
  VLOG(2) << "VFSListXattr ino: " << ino;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::ListXattr");
  auto ctx = SpanScope::GetContext(span);

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("listxattr (%d): %s %d %s", ino, s.ToString(),
                           xattrs->size(), ctx->hit_cache ? "true" : "false");
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opListXattr, &client_op_metric_->opAll});

  s = vfs_->ListXattr(SpanScope::GetContext(span), ino, xattrs);
  VLOG(2) << "VFSListXattr end, status: " << s.ToString()
          << " xattrs: " << xattrs->size();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::MkDir(Ino parent, const std::string& name, uint32_t uid,
                         uint32_t gid, uint32_t mode, Attr* attr) {
  VLOG(2) << "VFSMkDir parent ino: " << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " mode: " << mode;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::MkDir");

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

  s = vfs_->MkDir(SpanScope::GetContext(span), parent, name, uid, gid,
                  S_IFDIR | mode, attr);
  VLOG(2) << "VFSMkdir end, status: " << s.ToString()
          << " attr: " << Attr2Str(*attr);
  if (!s.ok()) {
    op_metric.FailOp();
  }
  return s;
}

Status VFSWrapper::OpenDir(Ino ino, uint64_t* fh, bool& need_cache) {
  VLOG(2) << "VFSOpendir ino: " << ino;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::OpenDir");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("opendir (%d): %s [fh:%d]", ino, s.ToString(), *fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opOpenDir, &client_op_metric_->opAll});

  auto ctx = SpanScope::GetContext(span);
  s = vfs_->OpenDir(ctx, ino, fh);
  VLOG(2) << "VFSOpendir end, ino: " << ino << " fh: " << *fh;
  if (!s.ok()) {
    op_metric.FailOp();
  }

  need_cache = ctx->need_cache;

  return s;
}

Status VFSWrapper::ReadDir(Ino ino, uint64_t fh, uint64_t offset,
                           bool with_attr, ReadDirHandler handler) {
  VLOG(2) << "VFSReaddir ino: " << ino << " fh: " << fh << " offset: " << offset
          << " with_attr: " << (with_attr ? "true" : "false");

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::ReadDir");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("readdir (%d): %s (%d) [fh:%d]", ino, s.ToString(),
                           offset, fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opReadDir, &client_op_metric_->opAll});

  s = vfs_->ReadDir(SpanScope::GetContext(span), ino, fh, offset, with_attr,
                    handler);

  VLOG(2) << "VFSReaddir end, status: " << s.ToString();

  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::ReleaseDir(Ino ino, uint64_t fh) {
  VLOG(2) << "VFSReleaseDir ino: " << ino << " fh: " << fh;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::ReleaseDir");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("releasedir (%d): %s [fh:%d]", ino, s.ToString(),
                           fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opReleaseDir, &client_op_metric_->opAll});

  s = vfs_->ReleaseDir(SpanScope::GetContext(span), ino, fh);
  VLOG(2) << "VFSReleaseDir end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::RmDir(Ino parent, const std::string& name) {
  VLOG(2) << "VFSRmdir parent: " << parent << " name: " << name;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::RmDir");

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

  s = vfs_->RmDir(SpanScope::GetContext(span), parent, name);
  VLOG(2) << "VFSRmdir end, status: " << s.ToString();
  if (!s.ok()) {
    op_metric.FailOp();
  }

  return s;
}

Status VFSWrapper::Ioctl(Ino ino, uint32_t uid, unsigned int cmd,
                         unsigned flags, const void* in_buf, size_t in_bufsz,
                         char* out_buf, size_t out_bufsz) {
  VLOG(2) << "VFSIoctl ino: " << ino << " cmd: " << cmd << " flags: " << flags
          << " in_bufsz: " << in_bufsz << " out_bufsz: " << out_bufsz;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::Ioctl");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("ioctl (%d,%u,%u,%zu,%zu): %s", ino, cmd, flags,
                           in_bufsz, out_bufsz, s.ToString());
  });

  s = vfs_->Ioctl(SpanScope::GetContext(span), ino, uid, cmd, flags, in_buf,
                  in_bufsz, out_buf, out_bufsz);

  VLOG(2) << "VFSIoctl end, status: " << s.ToString();

  return s;
}

Status VFSWrapper::StatFs(Ino ino, FsStat* fs_stat) {
  VLOG(2) << "VFSStatFs ino: " << ino;

  auto span = vfs_->GetTraceManager().StartSpan("VFSWrapper::StatFs");

  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("statfs (%d): %s", ino, s.ToString()); });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opStatfs, &client_op_metric_->opAll});

  s = vfs_->StatFs(SpanScope::GetContext(span), ino, fs_stat);

  VLOG(2) << "VFSStatFs end, status: " << s.ToString()
          << " fs_stat: " << FsStat2Str(*fs_stat);

  return s;
}

uint64_t VFSWrapper::GetFsId() {
  uint64_t fs_id = vfs_->GetFsId();
  VLOG(6) << "fs_id: " << fs_id;
  return fs_id;
}

uint64_t VFSWrapper::GetMaxNameLength() {
  uint64_t max_name_length = vfs_->GetMaxNameLength();
  VLOG(6) << "max name length: " << max_name_length;
  return max_name_length;
}

blockaccess::BlockAccessOptions VFSWrapper::GetBlockAccesserOptions() {
  CHECK(vfs_ != nullptr) << "vfs_ is nullptr";

  return vfs_->GetBlockAccesserOptions();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs