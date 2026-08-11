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

#include "client/vfs/client_session.h"

#include <bvar/bvar.h>
#include <fcntl.h>
#include <unistd.h>

#include <chrono>
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
#include "common/trace/trace_manager.h"
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
static bvar::Adder<int64_t> g_active_public_operations(
    "vfs_active_public_operations");
static bvar::Adder<uint64_t> g_rejected_public_operations(
    "vfs_rejected_public_operations_total");

#define CLIENT_SESSION_OPERATION_GUARD()                           \
  auto operation = TryAcquireOperation();                          \
  if (!operation.has_value()) {                                    \
    return Status::Stop("VFS is not accepting public operations"); \
  }

static std::string DescribeSetAttr(int set) {
  const uint32_t value = static_cast<uint32_t>(set);
  constexpr uint32_t kKnownSetAttr =
      kSetAttrMode | kSetAttrUid | kSetAttrGid | kSetAttrSize | kSetAttrAtime |
      kSetAttrMtime | kSetAttrAtimeNow | kSetAttrMtimeNow | kSetAttrCtime |
      kSetAttrKillSuid | kSetAttrKillSgid | kSetAttrFile | kSetAttrKillPriv |
      kSetAttrOpen | kSetAttrTimesSet | kSetAttrTouch | kSetAttrFlags |
      kSetAttrNlink;

  std::string description;
  auto append = [&description, value](uint32_t flag, const char* name) {
    if ((value & flag) == 0) return;
    if (!description.empty()) description += "|";
    description += name;
  };

  append(kSetAttrMode, "mode");
  append(kSetAttrUid, "uid");
  append(kSetAttrGid, "gid");
  append(kSetAttrSize, "size");
  append(kSetAttrAtime, "atime");
  append(kSetAttrMtime, "mtime");
  append(kSetAttrAtimeNow, "atime_now");
  append(kSetAttrMtimeNow, "mtime_now");
  append(kSetAttrCtime, "ctime");
  append(kSetAttrKillSuid, "kill_suid");
  append(kSetAttrKillSgid, "kill_sgid");
  append(kSetAttrFile, "file");
  append(kSetAttrKillPriv, "kill_priv");
  append(kSetAttrOpen, "open");
  append(kSetAttrTimesSet, "times_set");
  append(kSetAttrTouch, "touch");
  append(kSetAttrFlags, "flags");
  append(kSetAttrNlink, "nlink");

  const uint32_t unknown = value & ~kKnownSetAttr;
  if (unknown != 0) {
    if (!description.empty()) description += "|";
    description += fmt::format("unknown(0x{:X})", unknown);
  }
  return description.empty() ? "none" : description;
}

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
ClientSession::ClientSession() = default;

ClientSession::~ClientSession() { Stop(/*handover=*/false); }

ClientSession::OperationLease::~OperationLease() {
  if (owner_ != nullptr) owner_->ReleaseOperation();
}

std::optional<ClientSession::OperationLease>
ClientSession::TryAcquireOperation() {
  {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (lifecycle_state_ != LifecycleState::kRunning) {
      g_rejected_public_operations << 1;
      return std::nullopt;
    }

    ++active_public_operations_;
  }

  g_active_public_operations << 1;
  return OperationLease(this);
}

void ClientSession::ReleaseOperation() {
  g_active_public_operations << -1;

  std::lock_guard<std::mutex> lock(lifecycle_mutex_);
  CHECK_GT(active_public_operations_, 0);
  --active_public_operations_;
  if (active_public_operations_ == 0 &&
      lifecycle_state_ == LifecycleState::kQuiescing) {
    lifecycle_cv_.notify_all();
  }
}

Status ClientSession::FinishStartFailure(const Status& status) {
  if (vfs_ != nullptr) {
    Status cleanup_status = vfs_->Stop(/*skip_unmount=*/false);
    if (!cleanup_status.ok()) {
      LOG(ERROR) << "cleanup after VFS start failure failed: "
                 << cleanup_status.ToString();
    }
  }
  if (trace_started_) {
    trace_manager_->Stop();
    trace_started_ = false;
  }

  {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    stop_status_ = status;
    lifecycle_state_ = LifecycleState::kStopped;
  }
  lifecycle_cv_.notify_all();
  return status;
}

Status ClientSession::Start(const DingofsConfig& config, int upgrade_from_pid) {
  LOG(INFO) << "vfs start";

  {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (lifecycle_state_ != LifecycleState::kCreated) {
      return Status::InvalidParam("VFS can only be started once");
    }
    lifecycle_state_ = LifecycleState::kStarting;
  }

  if (config.fs_name.empty()) {
    return FinishStartFailure(Status::InvalidParam("fs_name is empty"));
  }
  if (config.mount_point.empty()) {
    return FinishStartFailure(Status::InvalidParam("mount_point is empty"));
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

  Status s = NormalizeMountRootPath(config.subdir, vfs_conf.mount_root_path);
  if (!s.ok()) return FinishStartFailure(s);
  LOG(INFO) << "vfs mount_root_path: " << vfs_conf.mount_root_path;

  if (vfs_conf.metasystem_type != MetaSystemType::MDS &&
      vfs_conf.metasystem_type != MetaSystemType::LOCAL &&
      vfs_conf.metasystem_type != MetaSystemType::MEMORY) {
    return FinishStartFailure(
        Status::InvalidParam("unsupported metasystem_type " +
                             MetaSystemTypeToString(vfs_conf.metasystem_type)));
  }

  LOG(INFO) << "use vfs type: "
            << MetaSystemTypeToString(vfs_conf.metasystem_type);

  AccessLogGuard log(
      [&]() { return absl::StrFormat("start: %s", s.ToString()); });

  if (FLAGS_log_dir.empty()) {
    FLAGS_log_dir = "/tmp";
  }

  s = InitLog();
  if (!s.ok()) return FinishStartFailure(s);

  if (FLAGS_vfs_bthread_worker_num > 0) {
    bthread_setconcurrency(FLAGS_vfs_bthread_worker_num);
    LOG(INFO) << fmt::format(
        "set bthread concurrency({}) actual concurrency({}).",
        FLAGS_vfs_bthread_worker_num, bthread_getconcurrency());
  }

  trace_manager_ = std::make_unique<TraceManager>();
  client_metrics_ = std::make_unique<metrics::client::ClientOpMetric>();
  if (FLAGS_enable_trace) {
    if (!trace_manager_->Init()) {
      return FinishStartFailure(Status::Internal("init trace manager fail"));
    }
    trace_started_ = true;
  }

  const bool is_upgrade = upgrade_from_pid > 0;

  Json::Value root;
  if (is_upgrade && !LoadStateFile(upgrade_from_pid, root)) {
    return FinishStartFailure(Status::InvalidParam("load vfs state fail"));
  }

  const std::string hostname = dingofs::Helper::GetHostName();
  if (hostname.empty()) {
    return FinishStartFailure(Status::Internal("get hostname fail"));
  }

  vfs::ClientId client_id(utils::GenerateUUID(), hostname,
                          FLAGS_vfs_dummy_server_port, vfs_conf.mount_point);
  if (is_upgrade) client_id.Load(root);
  CHECK(!client_id.ID().empty()) << "client id is empty.";

  LOG(INFO) << "client id: " << client_id.Description();

  vfs_ = std::make_unique<vfs::VFSImpl>(vfs_conf, client_id, *trace_manager_);
  s = vfs_->Start(/*skip_mount=*/is_upgrade);
  if (!s.ok()) return FinishStartFailure(s);

  if (is_upgrade) {
    if (!Load(root)) {
      return FinishStartFailure(Status::InvalidParam("load vfs state fail"));
    }
    // Remove the old process's state file now that we've consumed it.
    const std::string state_path =
        fmt::format("{}.{}", kFdStatePath, upgrade_from_pid);
    std::remove(state_path.c_str());  // NOLINT
  }

  uid_ = dingofs::Helper::GetOriginalUid();
  gid_ = dingofs::Helper::GetOriginalGid();

  {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    lifecycle_state_ = LifecycleState::kRunning;
    stop_status_ = Status::OK();
  }
  lifecycle_cv_.notify_all();
  return Status::OK();
}

Status ClientSession::Stop(bool handover) {
  {
    std::unique_lock<std::mutex> lock(lifecycle_mutex_);
    while (lifecycle_state_ == LifecycleState::kStarting) {
      if (lifecycle_cv_.wait_for(lock, std::chrono::seconds(30)) ==
          std::cv_status::timeout) {
        LOG(ERROR) << "VFS Stop still waiting for Start to finish";
      }
    }

    if (lifecycle_state_ == LifecycleState::kCreated) {
      lifecycle_state_ = LifecycleState::kStopped;
      stop_status_ = Status::OK();
      return stop_status_;
    }

    if (lifecycle_state_ == LifecycleState::kStopped) {
      return stop_status_;
    }

    if (lifecycle_state_ == LifecycleState::kQuiescing) {
      const bool same_stop_mode = stop_handover_ == handover;
      lifecycle_cv_.wait(lock, [this]() {
        return lifecycle_state_ == LifecycleState::kStopped;
      });
      if (!same_stop_mode) {
        return Status::InvalidParam(
            "concurrent VFS Stop requested with conflicting handover mode");
      }
      return stop_status_;
    }

    CHECK(lifecycle_state_ == LifecycleState::kRunning);
    lifecycle_state_ = LifecycleState::kQuiescing;
    stop_handover_ = handover;
    while (active_public_operations_ != 0) {
      if (lifecycle_cv_.wait_for(lock, std::chrono::seconds(30)) ==
          std::cv_status::timeout) {
        LOG(ERROR) << fmt::format(
            "VFS Stop still waiting for {} public operation(s) to drain",
            active_public_operations_);
      }
    }
  }

  LOG(INFO) << fmt::format("stopping vfs, handover({}).", handover);

  Status s;
  AccessLogGuard log(
      [&]() { return absl::StrFormat("stop: %s", s.ToString()); });
  s = vfs_->Stop(/*skip_unmount=*/handover);

  LOG(INFO) << fmt::format("stopped vfs, handover({}).", handover);

  // Handover teardown: dump after vfs_->Stop() so the persisted state reflects
  // the same stop -> dump order used by normal teardown. This is currently past
  // the clean rollback point; callers treat a non-OK return here as an
  // unrecoverable handover fault after the pre-teardown flush has succeeded.
  if (handover && s.ok() && !Dump()) {
    s = Status::InvalidParam("dump vfs state fail");
  }
  if (trace_started_) {
    trace_manager_->Stop();
    trace_started_ = false;
  }

  {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    stop_status_ = s;
    lifecycle_state_ = LifecycleState::kStopped;
  }
  lifecycle_cv_.notify_all();
  return s;
}

bool ClientSession::Dump() {
  auto span = trace_manager_->StartSpan("ClientSession::Dump");

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

bool ClientSession::Load(const Json::Value& value) {
  auto span = trace_manager_->StartSpan("ClientSession::Load");

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

Status ClientSession::GetInfo(std::string* info) {
  CLIENT_SESSION_OPERATION_GUARD();
  CHECK(vfs_ != nullptr) << "vfs_ is nullptr";
  return vfs_->GetInfo(info);
}

double ClientSession::GetAttrTimeout(FileType type) {
  (void)type;
  return static_cast<double>(FLAGS_fuse_attr_cache_timeout_s);
}

double ClientSession::GetEntryTimeout(FileType type) {
  (void)type;
  return static_cast<double>(FLAGS_fuse_entry_cache_timeout_s);
}

uint64_t ClientSession::GetMaxNameLength() {
  const uint64_t max_name_length = FLAGS_vfs_meta_max_name_length;
  VLOG(6) << "max name length: " << max_name_length;
  return max_name_length;
}

Status ClientSession::Lookup(const Context& ctx, Ino parent,
                             const std::string& name, Attr* attr) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSLookup parent: " << parent << " name: " << name;

  auto span = trace_manager_->StartSpan("ClientSession::Lookup");

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
      {&client_metrics_->opLookup, &client_metrics_->opAll},
      !dingofs::IsInternalName(name));

  if (name.length() > FLAGS_vfs_meta_max_name_length) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Lookup(span_ctx, parent, name, attr);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::GetAttr(const Context& ctx, Ino ino, Attr* attr) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSGetAttr ino: " << ino;

  auto span = trace_manager_->StartSpan("ClientSession::GetAttr");

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("[%s] getattr (%llu): %s %s",
                               ctx.ToShortString(), ino, s.ToString(),
                               StrAttr(attr));
      },
      !dingofs::IsInternalIno(ino));

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opGetAttr, &client_metrics_->opAll},
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

Status ClientSession::SetAttr(const Context& ctx, Ino ino, int set,
                              const Attr& in_attr, Attr* out_attr) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSSetAttr ino: " << ino << " set: " << set;

  auto span = trace_manager_->StartSpan("ClientSession::SetAttr");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] setattr (%llu,0x%X[%s]): %s %s",
                           ctx.ToShortString(), ino, set, DescribeSetAttr(set),
                           s.ToString(), StrAttr(out_attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opSetAttr, &client_metrics_->opAll});

  if (set & (kSetAttrOpen | kSetAttrTimesSet)) {
    s = Status::InvalidParam("not supported");
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->SetAttr(span_ctx, ino, set, in_attr, out_attr);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::Fallocate(const Context& ctx, Ino ino, int mode,
                                uint64_t offset, uint64_t length) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << fmt::format(
      "VFSFallocate ino: {} mode: 0x{:X} offset: {} length: {}", ino, mode,
      offset, length);

  auto span = trace_manager_->StartSpan("ClientSession::Fallocate");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat(
        "[%s] fallocate (%llu, mode=0x%X, off=%lu, len=%lu): %s",
        ctx.ToShortString(), ino, mode, offset, length, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opFallocate, &client_metrics_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Fallocate(span_ctx, ino, mode, offset, length);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::CopyFileRange(const Context& ctx, Ino src_ino,
                                    uint64_t src_off, uint64_t src_fh,
                                    Ino dst_ino, uint64_t dst_off,
                                    uint64_t dst_fh, uint64_t len,
                                    uint32_t flags, uint64_t* bytes_copied) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << fmt::format(
      "VFSCopyFileRange src_ino: {} src_off: {} src_fh: {} dst_ino: {} "
      "dst_off: {} dst_fh: {} len: {} flags: 0x{:x}",
      src_ino, src_off, src_fh, dst_ino, dst_off, dst_fh, len, flags);

  CHECK(bytes_copied != nullptr) << "bytes_copied is nullptr";

  auto span = trace_manager_->StartSpan("ClientSession::CopyFileRange");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat(
        "[%s] copyfilerange (%llu %lu fh:%lu, %llu %lu fh:%lu, %lu 0x%X): %s "
        "%lu",
        ctx.ToShortString(), src_ino, src_off, src_fh, dst_ino, dst_off, dst_fh,
        len, flags, s.ToString(), *bytes_copied);
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opCopyFileRange, &client_metrics_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->CopyFileRange(span_ctx, src_ino, src_off, src_fh, dst_ino, dst_off,
                          dst_fh, len, flags, bytes_copied);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::ReadLink(const Context& ctx, Ino ino, std::string* link) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSReadLink ino: " << ino;

  auto span = trace_manager_->StartSpan("ClientSession::ReadLink");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] readlink (%llu): %s %s", ctx.ToShortString(),
                           ino, s.ToString(), *link);
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opReadLink, &client_metrics_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->ReadLink(span_ctx, ino, link);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::MkNod(const Context& ctx, Ino parent,
                            const std::string& name, uint32_t mode,
                            uint64_t dev, Attr* attr) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSMknod parent: " << parent << " name: " << name
          << " uid: " << ctx.uid << " gid: " << ctx.gid << " mode: " << mode
          << " dev: " << dev;

  auto span = trace_manager_->StartSpan("ClientSession::MkNod");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] mknod (%llu,%s,%s:0%04o): %s %s",
                           ctx.ToShortString(), parent, name, StrMode(mode),
                           mode, s.ToString(), StrAttr(attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opMkNod, &client_metrics_->opAll});

  if (name.length() > FLAGS_vfs_meta_max_name_length) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->MkNod(span_ctx, parent, name, ctx.uid, ctx.gid, mode, dev, attr);
  VLOG(2) << "VFSMknod end, status: " << s.ToString();
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::Unlink(const Context& ctx, Ino parent,
                             const std::string& name) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSUnlink parent: " << parent << " name: " << name;

  auto span = trace_manager_->StartSpan("ClientSession::Unlink");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] unlink (%llu,%s): %s", ctx.ToShortString(),
                           parent, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opUnlink, &client_metrics_->opAll});

  if (name.length() > FLAGS_vfs_meta_max_name_length) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  if (span_ctx) span_ctx->uid = ctx.uid;

  s = vfs_->Unlink(span_ctx, parent, name);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::Symlink(const Context& ctx, Ino parent,
                              const std::string& name, const std::string& link,
                              Attr* attr) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSSymlink parent: " << parent << " name: " << name
          << " uid: " << ctx.uid << " gid: " << ctx.gid << " link: " << link;

  auto span = trace_manager_->StartSpan("ClientSession::Symlink");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] symlink (%llu,%s,%s): %s %s",
                           ctx.ToShortString(), parent, name, link,
                           s.ToString(), Attr2Str(*attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opSymlink, &client_metrics_->opAll});

  if (name.length() > FLAGS_vfs_meta_max_name_length) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Symlink(span_ctx, parent, name, ctx.uid, ctx.gid, link, attr);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::Rename(const Context& ctx, Ino old_parent,
                             const std::string& old_name, Ino new_parent,
                             const std::string& new_name) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSRename old_parent: " << old_parent << " old_name: " << old_name
          << " new_parent: " << new_parent << " new_name: " << new_name;

  auto span = trace_manager_->StartSpan("ClientSession::Rename");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] rename (%llu,%s,%llu,%s): %s",
                           ctx.ToShortString(), old_parent, old_name,
                           new_parent, new_name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opRename, &client_metrics_->opAll});

  const uint64_t max_name_length = FLAGS_vfs_meta_max_name_length;
  if (old_name.length() > max_name_length ||
      new_name.length() > max_name_length) {
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

Status ClientSession::Link(const Context& ctx, Ino ino, Ino new_parent,
                           const std::string& new_name, Attr* attr) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSLink ino: " << ino << " new_parent: " << new_parent
          << " new_name: " << new_name;

  auto span = trace_manager_->StartSpan("ClientSession::Link");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] link (%llu,%llu,%s): %s %s",
                           ctx.ToShortString(), ino, new_parent, new_name,
                           s.ToString(), StrAttr(attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opLink, &client_metrics_->opAll});

  uint64_t max_name_len = FLAGS_vfs_meta_max_name_length;
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

Status ClientSession::Open(const Context& ctx, Ino ino, int flags, uint64_t* fh,
                           bool* keep_cache) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSOpen ino: " << ino << " octal flags: " << std::oct << flags;
  CHECK(fh != nullptr) << "fh is nullptr";
  CHECK(keep_cache != nullptr) << "keep_cache is nullptr";

  auto span = trace_manager_->StartSpan("ClientSession::Open");

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("[%s] open (%llu): %d %s %s [fh:%d] %s",
                               ctx.ToShortString(), ino, flags,
                               Helper::DescOpenFlags(flags), s.ToString(), *fh,
                               *keep_cache ? "true" : "false");
      },
      !dingofs::IsInternalIno(ino));

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opOpen, &client_metrics_->opAll},
      !dingofs::IsInternalIno(ino));

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Open(span_ctx, ino, flags, fh, keep_cache);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::Create(const Context& ctx, Ino parent,
                             const std::string& name, uint32_t mode, int flags,
                             uint64_t* fh, Attr* attr) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSCreate parent: " << parent << " name: " << name
          << " uid: " << ctx.uid << " gid: " << ctx.gid << " mode: " << mode
          << " octal flags: " << std::oct << flags;

  auto span = trace_manager_->StartSpan("ClientSession::Create");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] create (%llu,%s): %s %s [fh:%d]",
                           ctx.ToShortString(), parent, name, s.ToString(),
                           StrAttr(attr), *fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opCreate, &client_metrics_->opAll});

  if (name.length() > FLAGS_vfs_meta_max_name_length) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Create(span_ctx, parent, name, ctx.uid, ctx.gid, mode, flags, fh,
                   attr);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::Read(const Context& ctx, Ino ino, DataBuffer* data_buffer,
                           uint64_t size, uint64_t offset, uint64_t fh,
                           uint64_t* out_rsize) {
  CLIENT_SESSION_OPERATION_GUARD();
  auto span = trace_manager_->StartSpan("ClientSession::Read");
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
      {&client_metrics_->opRead, &client_metrics_->opAll},
      !dingofs::IsInternalIno(ino));
  VFSRWMetricGuard guard(&s, &g_rw_metric.read, out_rsize,
                         !dingofs::IsInternalIno(ino));

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Read(span_ctx, ino, data_buffer, size, offset, fh, out_rsize);
  if (!s.ok()) op_metric.FailOp();

  dingofs::SpanScope::SetStatus(span, s);

  return s;
}

Status ClientSession::Write(const Context& ctx, Ino ino, const char* buf,
                            uint64_t size, uint64_t offset, uint64_t fh,
                            uint64_t* out_wsize) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSWrite ino: " << ino
          << ", buf: " << dingofs::Helper::Char2Addr(buf) << ", size: " << size
          << " offset: " << offset << " fh: " << fh;

  auto span = trace_manager_->StartSpan("ClientSession::Write");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] write (%llu,%llu,%llu): %s (%llu) [fh:%llu]",
                           ctx.ToShortString(), ino, size, offset, s.ToString(),
                           *out_wsize, fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opWrite, &client_metrics_->opAll});

  VFSRWMetricGuard guard(&s, &g_rw_metric.write, out_wsize);

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Write(span_ctx, ino, buf, size, offset, fh, out_wsize);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::Flush(const Context& ctx, Ino ino, uint64_t fh) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSFlush ino: " << ino << " fh: " << fh;

  auto span = trace_manager_->StartSpan("ClientSession::Flush");

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("[%s] flush (%llu): %s [fh:%llu]",
                               ctx.ToShortString(), ino, s.ToString(), fh);
      },
      !dingofs::IsInternalIno(ino));

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opFlush, &client_metrics_->opAll},
      !dingofs::IsInternalIno(ino));

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Flush(span_ctx, ino, fh);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::Release(const Context& ctx, Ino ino, uint64_t fh) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSRelease ino: " << ino << " fh: " << fh;

  auto span = trace_manager_->StartSpan("ClientSession::Release");

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("[%s] release (%llu): %s [fh:%llu]",
                               ctx.ToShortString(), ino, s.ToString(), fh);
      },
      !dingofs::IsInternalIno(ino));

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opRelease, &client_metrics_->opAll},
      !dingofs::IsInternalIno(ino));

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Release(span_ctx, ino, fh);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::Fsync(const Context& ctx, Ino ino, int datasync,
                            uint64_t fh) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSFsync ino: " << ino << " datasync: " << datasync
          << " fh: " << fh;

  auto span = trace_manager_->StartSpan("ClientSession::Fsync");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] fsync (%llu,%d): %s [fh:%llu]",
                           ctx.ToShortString(), ino, datasync, s.ToString(),
                           fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opFsync, &client_metrics_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->Fsync(span_ctx, ino, datasync, fh);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::SetXattr(const Context& ctx, Ino ino,
                               const std::string& name,
                               const std::string& value, int flags) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSSetXattr ino: " << ino << " name: " << name
          << " value: " << value << " octal flags: " << std::oct << flags;

  auto span = trace_manager_->StartSpan("ClientSession::SetXattr");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] setxattr (%llu,%s): %s", ctx.ToShortString(),
                           ino, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opSetXattr, &client_metrics_->opAll},
      !dingofs::IsInternalIno(ino));

  if (name.length() > FLAGS_vfs_meta_max_name_length) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->SetXattr(span_ctx, ino, name, value, flags);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::GetXattr(const Context& ctx, Ino ino,
                               const std::string& name, std::string* value) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSGetXattr ino: " << ino << " name: " << name;

  auto span = trace_manager_->StartSpan("ClientSession::GetXattr");

  Status s;
  AccessLogGuard log(
      [&]() {
        return absl::StrFormat("[%s] getxattr (%llu,%s): %s %s",
                               ctx.ToShortString(), ino, name, s.ToString(),
                               *value);
      },
      !dingofs::IsInternalIno(ino));

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opGetXattr, &client_metrics_->opAll},
      !dingofs::IsInternalIno(ino));

  if (name.length() > FLAGS_vfs_meta_max_name_length) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->GetXattr(span_ctx, ino, name, value);
  if (!s.ok()) op_metric.FailOp();

  if (value->empty()) s = Status::NoData("no data");

  LOG_DEBUG << "value size: " << value->size() << " value:" << *value
            << " s: " << s.ToString();

  return s;
}

Status ClientSession::RemoveXattr(const Context& ctx, Ino ino,
                                  const std::string& name) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSRemoveXattr ino: " << ino << " name: " << name;

  auto span = trace_manager_->StartSpan("ClientSession::RemoveXattr");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] removexattr (%llu,%s): %s",
                           ctx.ToShortString(), ino, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opRemoveXattr, &client_metrics_->opAll},
      !dingofs::IsInternalIno(ino));

  if (name.length() > FLAGS_vfs_meta_max_name_length) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->RemoveXattr(span_ctx, ino, name);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::ListXattr(const Context& ctx, Ino ino,
                                std::vector<std::string>* xattrs) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSListXattr ino: " << ino;

  auto span = trace_manager_->StartSpan("ClientSession::ListXattr");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] listxattr (%llu): %s %d", ctx.ToShortString(),
                           ino, s.ToString(), xattrs->size());
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opListXattr, &client_metrics_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->ListXattr(span_ctx, ino, xattrs);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::MkDir(const Context& ctx, Ino parent,
                            const std::string& name, uint32_t mode,
                            Attr* attr) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSMkDir parent ino: " << parent << " name: " << name
          << " uid: " << ctx.uid << " gid: " << ctx.gid << " mode: " << mode;

  auto span = trace_manager_->StartSpan("ClientSession::MkDir");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] mkdir (%llu,%s,%s:0%04o,%d,%d): %s %s",
                           ctx.ToShortString(), parent, name, StrMode(mode),
                           mode, ctx.uid, ctx.gid, s.ToString(), StrAttr(attr));
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opMkDir, &client_metrics_->opAll});

  if (name.length() > FLAGS_vfs_meta_max_name_length) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->MkDir(span_ctx, parent, name, ctx.uid, ctx.gid, S_IFDIR | mode,
                  attr);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::OpenDir(const Context& ctx, Ino ino, uint64_t* fh,
                              bool& need_cache) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSOpendir ino: " << ino;

  auto span = trace_manager_->StartSpan("ClientSession::OpenDir");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] opendir (%llu): %s [fh:%llu] %s",
                           ctx.ToShortString(), ino, s.ToString(), *fh,
                           need_cache ? "cache" : "nocache");
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opOpenDir, &client_metrics_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->OpenDir(span_ctx, ino, fh, need_cache);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::ReadDir(const Context& ctx, Ino ino, uint64_t fh,
                              uint64_t offset, bool with_attr,
                              ReadDirHandler handler) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSReaddir ino: " << ino << " fh: " << fh << " offset: " << offset
          << " with_attr: " << (with_attr ? "true" : "false");

  auto span = trace_manager_->StartSpan("ClientSession::ReadDir");

  Status s;
  uint32_t count = 0;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] readdir (%llu): %s (%llu %u) [fh:%llu] %s",
                           ctx.ToShortString(), ino, s.ToString(), offset,
                           count, fh, with_attr ? "true" : "false");
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opReadDir, &client_metrics_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->ReadDir(span_ctx, ino, fh, offset, with_attr, std::move(handler),
                    count);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::ReleaseDir(const Context& ctx, Ino ino, uint64_t fh) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSReleaseDir ino: " << ino << " fh: " << fh;

  auto span = trace_manager_->StartSpan("ClientSession::ReleaseDir");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] releasedir (%llu): %s [fh:%llu]",
                           ctx.ToShortString(), ino, s.ToString(), fh);
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opReleaseDir, &client_metrics_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->ReleaseDir(span_ctx, ino, fh);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::RmDir(const Context& ctx, Ino parent,
                            const std::string& name) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSRmdir parent: " << parent << " name: " << name;

  auto span = trace_manager_->StartSpan("ClientSession::RmDir");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] rmdir (%llu,%s): %s", ctx.ToShortString(),
                           parent, name, s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opRmDir, &client_metrics_->opAll});

  if (name.length() > FLAGS_vfs_meta_max_name_length) {
    s = Status::NameTooLong(fmt::format("name({}) too long", name.length()));
    return s;
  }

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  if (span_ctx) span_ctx->uid = ctx.uid;

  s = vfs_->RmDir(span_ctx, parent, name);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::StatFs(const Context& ctx, Ino ino, FsStat* fs_stat) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSStatFs ino: " << ino;

  auto span = trace_manager_->StartSpan("ClientSession::StatFs");

  Status s;
  AccessLogGuard log([&]() {
    return absl::StrFormat("[%s] statfs (%llu): %s", ctx.ToShortString(), ino,
                           s.ToString());
  });

  ClientOpMetricGuard op_metric(
      {&client_metrics_->opStatfs, &client_metrics_->opAll});

  auto span_ctx = dingofs::SpanScope::GetContext(span);
  s = vfs_->StatFs(span_ctx, ino, fs_stat);
  if (!s.ok()) op_metric.FailOp();

  return s;
}

Status ClientSession::Ioctl(const Context& ctx, Ino ino, unsigned int cmd,
                            unsigned flags, const void* in_buf, size_t in_bufsz,
                            char* out_buf, size_t out_bufsz) {
  CLIENT_SESSION_OPERATION_GUARD();
  VLOG(2) << "VFSIoctl ino: " << ino << " cmd: " << cmd << " flags: " << flags
          << " in_bufsz: " << in_bufsz << " out_bufsz: " << out_bufsz;

  auto span = trace_manager_->StartSpan("ClientSession::Ioctl");

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

#undef CLIENT_SESSION_OPERATION_GUARD

}  // namespace client
}  // namespace dingofs
