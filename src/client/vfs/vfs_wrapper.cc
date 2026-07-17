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

#include "client/vfs/vfs_wrapper.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>

#include "client/common/client_state.h"
#include "client/vfs/access_log.h"
#include "client/vfs/blockstore/block_store_access_log.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/metasystem/meta_log.h"
#include "client/vfs/vfs_impl.h"
#include "client/vfs/vfs_meta.h"
#include "common/blockaccess/block_access_log.h"
#include "common/const.h"
#include "common/directory.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/metrics/client/client.h"
#include "common/metrics/metric_guard.h"
#include "common/options/cache.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/types.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "json/reader.h"
#include "json/writer.h"
#include "utils/uuid.h"

DECLARE_string(log_dir);

namespace dingofs {
namespace client {

// State file lives under the dingofs runtime data dir (not /tmp), e.g.
// $DINGOFS_BASE_DIR/data, /var/dingofs/data (root) or $HOME/.dingofs/data.
// Old (writer) and new (reader) processes resolve the same path as long as
// they run with the same uid and DINGOFS_BASE_DIR env -- the same assumption
// already made for the fd-comm socket dir (GetDefaultDir(kSocketDir)).
const std::string kFdStateDir = GetDefaultDir(kDataDir);
const std::string kFdStatePath = kFdStateDir + "/dingo-client-state.json";

using metrics::ClientOpMetricGuard;
using metrics::VFSRWMetricGuard;
using metrics::client::VFSRWMetric;

using vfs::Attr2Str;
using vfs::StrAttr;
using vfs::StrMode;

static auto& g_rw_metric = VFSRWMetric::GetInstance();

static Status InitLog() {
  const std::string log_dir = Logger::LogDir();
  bool succ = dingofs::client::InitAccessLog(log_dir) &&
              blockaccess::InitBlockAccessLog(log_dir) &&
              dingofs::client::vfs::InitMetaLog(log_dir) &&
              dingofs::client::vfs::InitBlockStoreAccessLog(log_dir);

  CHECK(succ) << "init log failed, unexpected!";
  return Status::OK();
}

// Atomically publish `content` to `path`: write a sibling temp file, fsync it,
// rename(2) it over the target (atomic on the same filesystem), then fsync the
// directory so the rename survives a crash. Because the temp sits in the same
// dir as the target, the reader (new process) never observes a half-written
// state file -- it sees either the previous file or the fully renamed one.
// Returns false and removes the temp on any error so the caller can abort the
// handover instead of letting the new process load a truncated state.
static bool AtomicWriteStateFile(const std::string& dir,
                                 const std::string& path,
                                 const std::string& content) {
  if (!dingofs::Helper::CreateDirectory(dir)) {
    LOG(ERROR) << "create state dir fail, dir: " << dir;
    return false;
  }

  const std::string tmp = fmt::format("{}.tmp.{}", path, getpid());
  int fd = open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    LOG(ERROR) << "open temp state file fail, file: " << tmp
               << ", error: " << std::strerror(errno);
    return false;
  }

  bool ok = false;
  do {
    size_t off = 0;
    while (off < content.size()) {
      ssize_t n = write(fd, content.data() + off, content.size() - off);
      if (n < 0) {
        if (errno == EINTR) continue;
        LOG(ERROR) << "write temp state file fail, file: " << tmp
                   << ", error: " << std::strerror(errno);
        break;
      }
      off += static_cast<size_t>(n);
    }
    if (off != content.size()) break;
    if (fsync(fd) != 0) {
      LOG(ERROR) << "fsync temp state file fail, file: " << tmp
                 << ", error: " << std::strerror(errno);
      break;
    }
    ok = true;
  } while (false);
  // A close() error after a successful fsync() can still report a deferred
  // write-back failure; treat it as a failed publish.
  if (close(fd) != 0 && ok) {
    LOG(ERROR) << "close temp state file fail, file: " << tmp
               << ", error: " << std::strerror(errno);
    ok = false;
  }

  if (!ok) {
    std::remove(tmp.c_str());
    return false;
  }

  if (rename(tmp.c_str(), path.c_str()) != 0) {
    LOG(ERROR) << "rename state file fail, " << tmp << " -> " << path
               << ", error: " << std::strerror(errno);
    std::remove(tmp.c_str());
    return false;
  }

  // fsync the directory so the rename is durable. If this cannot be guaranteed,
  // fail closed AND remove the just-published file: the rename already replaced
  // the target, so leaving it would expose the new process to a failed
  // attempt's residue. The new process must not take over a state file whose
  // rename may not have reached disk.
  int dfd = open(dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (dfd < 0) {
    LOG(ERROR) << "open state dir for fsync fail, dir: " << dir
               << ", error: " << std::strerror(errno);
    std::remove(path.c_str());
    return false;
  }
  if (fsync(dfd) != 0) {
    LOG(ERROR) << "fsync state dir fail, dir: " << dir
               << ", error: " << std::strerror(errno);
    close(dfd);
    std::remove(path.c_str());
    return false;
  }
  if (close(dfd) != 0) {
    LOG(ERROR) << "close state dir fail, dir: " << dir
               << ", error: " << std::strerror(errno);
    std::remove(path.c_str());
    return false;
  }
  return true;
}

static bool LoadStateFile(int pid, Json::Value& root) {
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

// Normalize a filesystem-internal mount-root path.
// Accepts: "/", "/a", "/a/b", "/a/b/" (trailing slash trimmed),
//          repeated slashes collapsed.
// Rejects: empty string, relative paths (no leading '/'), and any
//          ".", ".." or empty components from malformed input.
static Status NormalizeMountRootPath(const std::string& in, std::string& out) {
  if (in.empty()) {
    return Status::InvalidParam("subdir is empty");
  }
  if (in[0] != '/') {
    return Status::InvalidParam(fmt::format(
        "subdir({}) must be an absolute path starting with '/'", in));
  }

  std::vector<std::string> parts;
  std::string cur;
  for (char c : in) {
    if (c == '/') {
      if (!cur.empty()) {
        if (cur == "." || cur == "..") {
          return Status::InvalidParam(fmt::format(
              "subdir({}) must not contain '.' or '..' components", in));
        }
        parts.push_back(std::move(cur));
        cur.clear();
      }
    } else {
      cur.push_back(c);
    }
  }
  if (!cur.empty()) {
    if (cur == "." || cur == "..") {
      return Status::InvalidParam(fmt::format(
          "subdir({}) must not contain '.' or '..' components", in));
    }
    parts.push_back(std::move(cur));
  }

  if (parts.empty()) {
    out = "/";
  } else {
    std::string normalized;
    for (const auto& p : parts) {
      normalized.push_back('/');
      normalized.append(p);
    }
    out = std::move(normalized);
  }

  return Status::OK();
}

Status VFSWrapper::Start(const DingofsConfig& config, int upgrade_from_pid) {
  LOG(INFO) << "vfs start";

  if (config.fs_name.empty()) {
    return Status::InvalidParam("fs_name is empty");
  }
  if (config.mount_point.empty()) {
    return Status::InvalidParam("mount_point is empty");
  }

  // Propagate mds_addrs to the cache layer's own MDS client so that
  // remote cache peer discovery works when remote cache is enabled.
  cache::FLAGS_mds_addrs = config.mds_addrs;

  vfs::VFSConfig vfs_conf;
  vfs_conf.mds_addrs = config.mds_addrs;
  vfs_conf.mount_point = config.mount_point;
  vfs_conf.fs_name = config.fs_name;
  vfs_conf.metasystem_type = ParseMetaSystemType(config.metasystem_type);
  vfs_conf.storage_info = config.storage_info;

  DINGOFS_RETURN_NOT_OK(
      NormalizeMountRootPath(config.subdir, vfs_conf.mount_root_path));
  LOG(INFO) << "vfs mount_root_path: " << vfs_conf.mount_root_path;

  if (vfs_conf.metasystem_type != MetaSystemType::MDS &&
      vfs_conf.metasystem_type != MetaSystemType::LOCAL &&
      vfs_conf.metasystem_type != MetaSystemType::MEMORY) {
    return Status::InvalidParam(
        "unsupported metasystem_type " +
        MetaSystemTypeToString(vfs_conf.metasystem_type));
  }

  LOG(INFO) << "use vfs type: "
            << MetaSystemTypeToString(vfs_conf.metasystem_type);

  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("start: %s", s.ToString()); });

  if (FLAGS_log_dir.empty()) {
    FLAGS_log_dir = "/tmp";
  }

  DINGOFS_RETURN_NOT_OK(InitLog());

  if (FLAGS_vfs_bthread_worker_num > 0) {
    bthread_setconcurrency(FLAGS_vfs_bthread_worker_num);
    LOG(INFO) << fmt::format(
        "set bthread concurrency({}) actual concurrency({}).",
        FLAGS_vfs_bthread_worker_num, bthread_getconcurrency());
  }

  client_op_metric_ = std::make_unique<metrics::client::ClientOpMetric>();

  const bool is_upgrade = upgrade_from_pid > 0;

  Json::Value root;
  if (is_upgrade && !LoadStateFile(upgrade_from_pid, root)) {
    return Status::InvalidParam("load vfs state fail");
  }

  const std::string hostname = dingofs::Helper::GetHostName();
  if (hostname.empty()) return Status::Internal("get hostname fail");

  vfs::ClientId client_id(utils::GenerateUUID(), hostname,
                          FLAGS_vfs_dummy_server_port, vfs_conf.mount_point);
  if (is_upgrade) client_id.Load(root);
  CHECK(!client_id.ID().empty()) << "client id is empty.";

  LOG(INFO) << "client id: " << client_id.Description();

  vfs_ = std::make_unique<vfs::VFSImpl>(vfs_conf, client_id);
  DINGOFS_RETURN_NOT_OK(vfs_->Start(/*skip_mount=*/is_upgrade));

  if (is_upgrade) {
    if (!Load(root)) {
      return Status::InvalidParam("load vfs state fail");
    }
    // Remove the old process's state file now that we've consumed it.
    const std::string state_path =
        fmt::format("{}.{}", kFdStatePath, upgrade_from_pid);
    std::remove(state_path.c_str());
  }

  uid_ = dingofs::Helper::GetOriginalUid();
  gid_ = dingofs::Helper::GetOriginalGid();

  started_.store(true);
  return Status::OK();
}

Status VFSWrapper::Stop(bool handover) {
  if (!started_.load()) {
    LOG(INFO) << "vfs not started, no need to stop.";
    return Status::OK();
  }

  LOG(INFO) << fmt::format("stopping vfs, handover({}).", handover);

  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("stop: %s", s.ToString()); });
  s = vfs_->Stop(/*skip_unmount=*/handover);

  // The VFS teardown has run; mark stopped so a second Stop() (e.g. the
  // post-exit FuseOpDestroy after a successful handover gate) is a no-op and
  // does not stop/dump twice.
  started_.store(false);

  LOG(INFO) << fmt::format("stopped vfs, handover({}).", handover);

  // Handover teardown: dump after vfs_->Stop() so the persisted state reflects
  // the same stop -> dump order used by normal teardown. This is currently past
  // the clean rollback point; callers treat a non-OK return here as an
  // unrecoverable handover fault after the pre-teardown flush has succeeded.
  if (handover && !s.ok()) {
    return s;
  }

  if (handover && !Dump()) {
    return Status::InvalidParam("dump vfs state fail");
  }

  return s;
}

bool VFSWrapper::Dump() {
  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Dump");

  Json::Value root;
  if (!vfs_->Dump(dingofs::SpanScope::GetContext(span), root)) {
    LOG(ERROR) << "dump vfs state fail.";
    return false;
  }

  // TODO(hotupgrade): write root["schema_version"] here and verify it in
  // Load() so a new process refuses to consume an incompatible state file.
  root["epch"] = Json::Value::UInt64(ClientState::GetEpoch());
  root["first_start_time_ms"] =
      Json::Value::UInt64(ClientState::GetFirstStartTime());

  const std::string path = fmt::format("{}.{}", kFdStatePath, getpid());
  Json::StreamWriterBuilder writer;
  if (!AtomicWriteStateFile(kFdStateDir, path,
                            Json::writeString(writer, root))) {
    LOG(ERROR) << "dump vfs state fail, path: " << path;
    return false;
  }

  LOG(INFO) << fmt::format("dump vfs state success, path({}).", path);
  return true;
}

bool VFSWrapper::Load(const Json::Value& value) {
  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Load");

  // TODO(hotupgrade): verify value["schema_version"] matches the running
  // binary's expected version; refuse the handover (return false) on mismatch
  // instead of loading a state file produced by an incompatible layout.
  if (!value["epch"].isNull()) {
    ClientState::SetEpoch(value["epch"].asUInt64() + 1);
    LOG(INFO) << "load epoch: " << ClientState::GetEpoch();
  }

  if (!value["first_start_time_ms"].isNull()) {
    ClientState::SetFirstStartTime(value["first_start_time_ms"].asUInt64());
  }

  if (!vfs_->Load(dingofs::SpanScope::GetContext(span), value)) {
    LOG(ERROR) << "load vfs state fail.";
    return false;
  }

  return true;
}

Status VFSWrapper::GetInfo(std::string* info) {
  CHECK(vfs_ != nullptr) << "vfs_ is nullptr";
  return vfs_->GetInfo(info);
}

double VFSWrapper::GetAttrTimeout(FileType type) {
  return vfs_->GetAttrTimeout(type);
}

double VFSWrapper::GetEntryTimeout(FileType type) {
  return vfs_->GetEntryTimeout(type);
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

Status VFSWrapper::Lookup(const Context& ctx, Ino parent,
                          const std::string& name, Attr* attr) {
  VLOG(2) << "VFSLookup parent: " << parent << " name: " << name;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Lookup");

  Status s;
  AccessLogGuard log(
      [&]() {
        if (s.ok()) {
          return absl::StrFormat("[%s] lookup (%llu/%s): %s %s",
                                 ctx.ToShortString(), parent, name,
                                 s.ToString(), StrAttr(attr));
        } else {
          return absl::StrFormat("[%s] lookup (%llu/%s): %s",
                                 ctx.ToShortString(), parent, name,
                                 s.ToString());
        }
      },
      !dingofs::IsInternalName(name));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opLookup, &client_op_metric_->opAll},
      !dingofs::IsInternalName(name));

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Lookup(span_ctx, parent, name, attr);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::GetAttr(const Context& ctx, Ino ino, Attr* attr) {
  VLOG(2) << "VFSGetAttr ino: " << ino;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::GetAttr");

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("[%s] getattr (%llu): %s %s",
                               ctx.ToShortString(), ino, s.ToString(),
                               StrAttr(attr));
      },
      !dingofs::IsInternalIno(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opGetAttr, &client_op_metric_->opAll},
      !dingofs::IsInternalIno(ino));

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->GetAttr(span_ctx, ino, attr);
  if (!s.ok()) op_metric.FailOp();

  if (ino == dingofs::kRootIno) {
    attr->uid = uid_;
    attr->gid = gid_;
  }

  return s;
}

Status VFSWrapper::SetAttr(const Context& ctx, Ino ino, int set,
                           const Attr& in_attr, Attr* out_attr) {
  VLOG(2) << "VFSSetAttr ino: " << ino << " set: " << set;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::SetAttr");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] setattr (%llu,0x%X): %s %s",
                           ctx.ToShortString(), ino, set, s.ToString(),
                           StrAttr(out_attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opSetAttr, &client_op_metric_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->SetAttr(span_ctx, ino, set, in_attr, out_attr);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::Fallocate(const Context& ctx, Ino ino, int mode,
                             uint64_t offset, uint64_t length) {
  VLOG(2) << fmt::format(
      "VFSFallocate ino: {} mode: 0x{:X} offset: {} length: {}", ino, mode,
      offset, length);

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Fallocate");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat(
        "[%s] fallocate (%llu, mode=0x%X, off=%lu, len=%lu): %s",
        ctx.ToShortString(), ino, mode, offset, length, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opFallocate, &client_op_metric_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Fallocate(span_ctx, ino, mode, offset, length);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::CopyFileRange(const Context& ctx, Ino src_ino,
                                 uint64_t src_off, uint64_t src_fh, Ino dst_ino,
                                 uint64_t dst_off, uint64_t dst_fh,
                                 uint64_t len, uint32_t flags,
                                 uint64_t* bytes_copied) {
  VLOG(2) << fmt::format(
      "VFSCopyFileRange src_ino: {} src_off: {} src_fh: {} dst_ino: {} "
      "dst_off: {} dst_fh: {} len: {} flags: 0x{:x}",
      src_ino, src_off, src_fh, dst_ino, dst_off, dst_fh, len, flags);

  CHECK(bytes_copied != nullptr) << "bytes_copied is nullptr";

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::CopyFileRange");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat(
        "[%s] copyfilerange (%llu %lu fh:%lu, %llu %lu fh:%lu, %lu 0x%X): %s "
        "%lu",
        ctx.ToShortString(), src_ino, src_off, src_fh, dst_ino, dst_off, dst_fh,
        len, flags, s.ToString(), *bytes_copied);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opCopyFileRange, &client_op_metric_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->CopyFileRange(span_ctx, src_ino, src_off, src_fh, dst_ino, dst_off,
                          dst_fh, len, flags, bytes_copied);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::ReadLink(const Context& ctx, Ino ino, std::string* link) {
  VLOG(2) << "VFSReadLink ino: " << ino;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::ReadLink");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] readlink (%llu): %s %s", ctx.ToShortString(),
                           ino, s.ToString(), *link);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opReadLink, &client_op_metric_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->ReadLink(span_ctx, ino, link);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::MkNod(const Context& ctx, Ino parent,
                         const std::string& name, uint32_t mode, uint64_t dev,
                         Attr* attr) {
  VLOG(2) << "VFSMknod parent: " << parent << " name: " << name
          << " uid: " << ctx.uid << " gid: " << ctx.gid << " mode: " << mode
          << " dev: " << dev;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::MkNod");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] mknod (%llu,%s,%s:0%04o): %s %s",
                           ctx.ToShortString(), parent, name, StrMode(mode),
                           mode, s.ToString(), StrAttr(attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opMkNod, &client_op_metric_->opAll});

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->MkNod(span_ctx, parent, name, ctx.uid, ctx.gid, mode, dev, attr);
  VLOG(2) << "VFSMknod end, status: " << s.ToString();
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::Unlink(const Context& ctx, Ino parent,
                          const std::string& name) {
  VLOG(2) << "VFSUnlink parent: " << parent << " name: " << name;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Unlink");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] unlink (%llu,%s): %s", ctx.ToShortString(),
                           parent, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opUnlink, &client_op_metric_->opAll});

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  if (span_ctx) span_ctx->uid = ctx.uid;

  s = vfs_->Unlink(span_ctx, parent, name);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::Symlink(const Context& ctx, Ino parent,
                           const std::string& name, const std::string& link,
                           Attr* attr) {
  VLOG(2) << "VFSSymlink parent: " << parent << " name: " << name
          << " uid: " << ctx.uid << " gid: " << ctx.gid << " link: " << link;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Symlink");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] symlink (%llu,%s,%s): %s %s",
                           ctx.ToShortString(), parent, name, link,
                           s.ToString(), Attr2Str(*attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opSymlink, &client_op_metric_->opAll});

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Symlink(span_ctx, parent, name, ctx.uid, ctx.gid, link, attr);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::Rename(const Context& ctx, Ino old_parent,
                          const std::string& old_name, Ino new_parent,
                          const std::string& new_name) {
  VLOG(2) << "VFSRename old_parent: " << old_parent << " old_name: " << old_name
          << " new_parent: " << new_parent << " new_name: " << new_name;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Rename");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] rename (%llu,%s,%llu,%s): %s",
                           ctx.ToShortString(), old_parent, old_name,
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

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  if (span_ctx) span_ctx->uid = ctx.uid;

  s = vfs_->Rename(span_ctx, old_parent, old_name, new_parent, new_name);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::Link(const Context& ctx, Ino ino, Ino new_parent,
                        const std::string& new_name, Attr* attr) {
  VLOG(2) << "VFSLink ino: " << ino << " new_parent: " << new_parent
          << " new_name: " << new_name;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Link");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] link (%llu,%llu,%s): %s %s",
                           ctx.ToShortString(), ino, new_parent, new_name,
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

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Link(span_ctx, ino, new_parent, new_name, attr);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::Open(const Context& ctx, Ino ino, int flags, uint64_t* fh) {
  VLOG(2) << "VFSOpen ino: " << ino << " octal flags: " << std::oct << flags;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Open");

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("[%s] open (%llu): %d %s %s [fh:%d]",
                               ctx.ToShortString(), ino, flags,
                               Helper::DescOpenFlags(flags), s.ToString(), *fh);
      },
      !dingofs::IsInternalIno(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opOpen, &client_op_metric_->opAll},
      !dingofs::IsInternalIno(ino));

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Open(span_ctx, ino, flags, fh);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::Create(const Context& ctx, Ino parent,
                          const std::string& name, uint32_t mode, int flags,
                          uint64_t* fh, Attr* attr) {
  VLOG(2) << "VFSCreate parent: " << parent << " name: " << name
          << " uid: " << ctx.uid << " gid: " << ctx.gid << " mode: " << mode
          << " octal flags: " << std::oct << flags;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Create");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] create (%llu,%s): %s %s [fh:%d]",
                           ctx.ToShortString(), parent, name, s.ToString(),
                           StrAttr(attr), *fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opCreate, &client_op_metric_->opAll});

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Create(span_ctx, parent, name, ctx.uid, ctx.gid, mode, flags, fh,
                   attr);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::Read(const Context& ctx, Ino ino, DataBuffer* data_buffer,
                        uint64_t size, uint64_t offset, uint64_t fh,
                        uint64_t* out_rsize) {
  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Read");
  std::string session_id = dingofs::SpanScope::GetSessionID(span);

  VLOG(2) << fmt::format("[{}] VFSRead ino: {}, size: {}, offset: {}, fh: {}",
                         session_id, ino, size, offset, fh);

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat(
            "[%s] read (%llu,%llu,%llu): %s (%llu) [fh:%llu]",
            ctx.ToShortString(), ino, size, offset, s.ToString(), *out_rsize,
            fh);
      },
      !dingofs::IsInternalIno(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRead, &client_op_metric_->opAll},
      !dingofs::IsInternalIno(ino));
  VFSRWMetricGuard guard(&s, &g_rw_metric.read, out_rsize,
                         !dingofs::IsInternalIno(ino));

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Read(span_ctx, ino, data_buffer, size, offset, fh, out_rsize);
  if (!s.ok()) op_metric.FailOp();

  dingofs::SpanScope::SetStatus(span, s);

  return s;
}

Status VFSWrapper::Write(const Context& ctx, Ino ino, const char* buf,
                         uint64_t size, uint64_t offset, uint64_t fh,
                         uint64_t* out_wsize) {
  VLOG(2) << "VFSWrite ino: " << ino
          << ", buf: " << dingofs::Helper::Char2Addr(buf) << ", size: " << size
          << " offset: " << offset << " fh: " << fh;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Write");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] write (%llu,%llu,%llu): %s (%llu) [fh:%llu]",
                           ctx.ToShortString(), ino, size, offset, s.ToString(),
                           *out_wsize, fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opWrite, &client_op_metric_->opAll});

  VFSRWMetricGuard guard(&s, &g_rw_metric.write, out_wsize);

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Write(span_ctx, ino, buf, size, offset, fh, out_wsize);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::Flush(const Context& ctx, Ino ino, uint64_t fh) {
  VLOG(2) << "VFSFlush ino: " << ino << " fh: " << fh;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Flush");

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("[%s] flush (%llu): %s [fh:%llu]",
                               ctx.ToShortString(), ino, s.ToString(), fh);
      },
      !dingofs::IsInternalIno(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opFlush, &client_op_metric_->opAll},
      !dingofs::IsInternalIno(ino));

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Flush(span_ctx, ino, fh);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::Release(const Context& ctx, Ino ino, uint64_t fh) {
  VLOG(2) << "VFSRelease ino: " << ino << " fh: " << fh;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Release");

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("[%s] release (%llu): %s [fh:%llu]",
                               ctx.ToShortString(), ino, s.ToString(), fh);
      },
      !dingofs::IsInternalIno(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRelease, &client_op_metric_->opAll},
      !dingofs::IsInternalIno(ino));

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Release(span_ctx, ino, fh);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::Fsync(const Context& ctx, Ino ino, int datasync,
                         uint64_t fh) {
  VLOG(2) << "VFSFsync ino: " << ino << " datasync: " << datasync
          << " fh: " << fh;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Fsync");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] fsync (%llu,%d): %s [fh:%llu]",
                           ctx.ToShortString(), ino, datasync, s.ToString(),
                           fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opFsync, &client_op_metric_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Fsync(span_ctx, ino, datasync, fh);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::SetXattr(const Context& ctx, Ino ino,
                            const std::string& name, const std::string& value,
                            int flags) {
  VLOG(2) << "VFSSetXattr ino: " << ino << " name: " << name
          << " value: " << value << " octal flags: " << std::oct << flags;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::SetXattr");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] setxattr (%llu,%s): %s", ctx.ToShortString(),
                           ino, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opSetAttr, &client_op_metric_->opAll},
      !dingofs::IsInternalIno(ino));

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->SetXattr(span_ctx, ino, name, value, flags);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::GetXattr(const Context& ctx, Ino ino,
                            const std::string& name, std::string* value) {
  VLOG(2) << "VFSGetXattr ino: " << ino << " name: " << name;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::GetXattr");

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("[%s] getxattr (%llu,%s): %s %s",
                               ctx.ToShortString(), ino, name, s.ToString(),
                               *value);
      },
      !dingofs::IsInternalIno(ino));

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opGetXattr, &client_op_metric_->opAll},
      !dingofs::IsInternalIno(ino));

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->GetXattr(span_ctx, ino, name, value);
  if (!s.ok()) op_metric.FailOp();

  if (value->empty()) s = Status::NoData("no data");

  return s;
}

Status VFSWrapper::RemoveXattr(const Context& ctx, Ino ino,
                               const std::string& name) {
  VLOG(2) << "VFSRemoveXattr ino: " << ino << " name: " << name;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::RemoveXattr");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] removexattr (%llu,%s): %s",
                           ctx.ToShortString(), ino, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRemoveXattr, &client_op_metric_->opAll},
      !dingofs::IsInternalIno(ino));

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->RemoveXattr(span_ctx, ino, name);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::ListXattr(const Context& ctx, Ino ino,
                             std::vector<std::string>* xattrs) {
  VLOG(2) << "VFSListXattr ino: " << ino;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::ListXattr");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] listxattr (%llu): %s %d", ctx.ToShortString(),
                           ino, s.ToString(), xattrs->size());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opListXattr, &client_op_metric_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->ListXattr(span_ctx, ino, xattrs);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::MkDir(const Context& ctx, Ino parent,
                         const std::string& name, uint32_t mode, Attr* attr) {
  VLOG(2) << "VFSMkDir parent ino: " << parent << " name: " << name
          << " uid: " << ctx.uid << " gid: " << ctx.gid << " mode: " << mode;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::MkDir");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] mkdir (%llu,%s,%s:0%04o,%d,%d): %s %s",
                           ctx.ToShortString(), parent, name, StrMode(mode),
                           mode, ctx.uid, ctx.gid, s.ToString(), StrAttr(attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opMkDir, &client_op_metric_->opAll});

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->MkDir(span_ctx, parent, name, ctx.uid, ctx.gid, S_IFDIR | mode,
                  attr);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::OpenDir(const Context& ctx, Ino ino, uint64_t* fh,
                           bool& need_cache) {
  VLOG(2) << "VFSOpendir ino: " << ino;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::OpenDir");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] opendir (%llu): %s [fh:%llu] %s",
                           ctx.ToShortString(), ino, s.ToString(), *fh,
                           need_cache ? "cache" : "nocache");
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opOpenDir, &client_op_metric_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->OpenDir(span_ctx, ino, fh, need_cache);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::ReadDir(const Context& ctx, Ino ino, uint64_t fh,
                           uint64_t offset, bool with_attr,
                           ReadDirHandler handler) {
  VLOG(2) << "VFSReaddir ino: " << ino << " fh: " << fh << " offset: " << offset
          << " with_attr: " << (with_attr ? "true" : "false");

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::ReadDir");

  Status s;
  uint32_t count = 0;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] readdir (%llu): %s (%llu %u) [fh:%llu] %s",
                           ctx.ToShortString(), ino, s.ToString(), offset,
                           count, fh, with_attr ? "true" : "false");
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opReadDir, &client_op_metric_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->ReadDir(span_ctx, ino, fh, offset, with_attr, handler, count);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::ReleaseDir(const Context& ctx, Ino ino, uint64_t fh) {
  VLOG(2) << "VFSReleaseDir ino: " << ino << " fh: " << fh;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::ReleaseDir");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] releasedir (%llu): %s [fh:%llu]",
                           ctx.ToShortString(), ino, s.ToString(), fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opReleaseDir, &client_op_metric_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->ReleaseDir(span_ctx, ino, fh);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::RmDir(const Context& ctx, Ino parent,
                         const std::string& name) {
  VLOG(2) << "VFSRmdir parent: " << parent << " name: " << name;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::RmDir");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] rmdir (%llu,%s): %s", ctx.ToShortString(),
                           parent, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opRmDir, &client_op_metric_->opAll});

  if (name.length() > vfs_->GetMaxNameLength()) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  if (span_ctx) span_ctx->uid = ctx.uid;

  s = vfs_->RmDir(span_ctx, parent, name);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::StatFs(const Context& ctx, Ino ino, FsStat* fs_stat) {
  VLOG(2) << "VFSStatFs ino: " << ino;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::StatFs");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] statfs (%llu): %s", ctx.ToShortString(), ino,
                           s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_op_metric_->opStatfs, &client_op_metric_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->StatFs(span_ctx, ino, fs_stat);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status VFSWrapper::Ioctl(const Context& ctx, Ino ino, unsigned int cmd,
                         unsigned flags, const void* in_buf, size_t in_bufsz,
                         char* out_buf, size_t out_bufsz) {
  VLOG(2) << "VFSIoctl ino: " << ino << " cmd: " << cmd << " flags: " << flags
          << " in_bufsz: " << in_bufsz << " out_bufsz: " << out_bufsz;

  auto span = vfs_->GetTraceManager()->StartSpan("VFSWrapper::Ioctl");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] ioctl (%llu,%u,%u,%zu,%zu): %s",
                           ctx.ToShortString(), ino, cmd, flags, in_bufsz,
                           out_bufsz, s.ToString());
  });

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Ioctl(span_ctx, ino, ctx.uid, cmd, flags, in_buf, in_bufsz, out_buf,
                  out_bufsz);

  return s;
}

}  // namespace client
}  // namespace dingofs
