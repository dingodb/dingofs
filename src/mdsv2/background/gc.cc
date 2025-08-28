// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mdsv2/background/gc.h"

#include <glog/logging.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "blockaccess/rados/rados_common.h"
#include "blockaccess/s3/s3_common.h"
#include "cache/blockcache/cache_store.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/filesystem/store_operation.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_uint32(mds_scan_batch_size);

DEFINE_uint32(mds_gc_worker_num, 32, "gc worker set num");
DEFINE_uint32(mds_gc_max_pending_task_count, 8192, "gc max pending task count");

DEFINE_bool(mds_gc_delslice_enable, true, "gc delslice enable");
DEFINE_bool(mds_gc_delfile_enable, true, "gc delfile enable");
DEFINE_bool(mds_gc_filesession_enable, true, "gc filesession enable");

DEFINE_uint32(mds_gc_delfile_reserve_time_s, 600, "gc del file reserve time");

DEFINE_uint32(mds_gc_filesession_reserve_time_s, 86400, "gc file session reserve time");

static const std::string kWorkerSetName = "GC";

void CleanDelSliceTask::Run() {
  auto status = CleanDelSlice();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delslice] clean deleted slice fail, {}", status.error_str());
  }

  // forget task
  if (task_memo_ != nullptr) task_memo_->Forget(key_);
}

Status CleanDelSliceTask::CleanDelSlice() {
  // delete data from s3
  std::string slice_id_trace;
  auto trash_slice_list = MetaCodec::DecodeDelSliceValue(value_);
  for (int i = 0; i < trash_slice_list.slices_size(); ++i) {
    const auto& slice = trash_slice_list.slices().at(i);

    uint64_t chunk_offset = slice.chunk_index() * slice.chunk_size();
    for (const auto& range : slice.ranges()) {
      for (uint64_t len = 0; len < range.len(); len += slice.block_size()) {
        uint64_t block_index = (range.offset() - chunk_offset + len) / slice.block_size();
        cache::BlockKey block_key(slice.fs_id(), slice.ino(), slice.slice_id(), block_index,
                                  range.compaction_version());

        DINGO_LOG(INFO) << fmt::format("[gc.delslice] delete block key({}).", block_key.StoreKey());
        auto status = data_accessor_->Delete(block_key.StoreKey());
        if (!status.ok()) {
          return Status(pb::error::EINTERNAL, fmt::format("delete s3 object fail, {}", status.ToString()));
        }
      }
    }

    slice_id_trace += std::to_string(slice.slice_id());
    if (i + 1 < trash_slice_list.slices_size()) {
      slice_id_trace += ",";
    }
  }

  // delete slice
  class Trace trace;
  CleanDelSliceOperation operation(trace, key_);
  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << fmt::format("[gc.delslice] clean slice finish, slice({}).", slice_id_trace);

  return Status::OK();
}

void CleanDelFileTask::Run() {
  auto status = CleanDelFile(attr_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfile] clean delfile fail, {}", status.error_str());
  }

  // forget task
  if (task_memo_ != nullptr) {
    task_memo_->Forget(MetaCodec::EncodeDelFileKey(attr_.fs_id(), attr_.ino()));
  }
}

Status CleanDelFileTask::GetChunks(uint32_t fs_id, Ino ino, std::vector<ChunkEntry>& chunks) {
  class Trace trace;
  ScanChunkOperation operation(trace, fs_id, ino);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  auto& result = operation.GetResult();

  chunks = std::move(result.chunks);

  return Status::OK();
}

Status CleanDelFileTask::CleanDelFile(const AttrEntry& attr) {
  DINGO_LOG(INFO) << fmt::format("[gc.delfile] clean delfile, ino({}) nlink({}) len({}) version({}).", attr.ino(),
                                 attr.nlink(), attr.length(), attr.version());
  // get file chunks
  std::vector<ChunkEntry> chunks;
  auto status = GetChunks(attr.fs_id(), attr.ino(), chunks);
  if (!status.ok()) {
    return status;
  }

  // delete data from s3
  for (const auto& chunk : chunks) {
    uint64_t chunk_offset = chunk.index() * chunk.chunk_size();
    for (const auto& slice : chunk.slices()) {
      for (uint32_t len = 0; len < slice.len(); len += chunk.block_size()) {
        uint64_t block_index = (slice.offset() - chunk_offset + len) / chunk.block_size();
        cache::BlockKey block_key(attr.fs_id(), attr.ino(), slice.id(), block_index, slice.compaction_version());

        DINGO_LOG(INFO) << fmt::format("[gc.delfile] delete block key({}).", block_key.StoreKey());
        auto status = data_accessor_->Delete(block_key.StoreKey());
        if (!status.ok()) {
          return Status(pb::error::EINTERNAL, fmt::format("delete s3 object fail, {}", status.ToString()));
        }
      }
    }
  }

  // delete inode
  class Trace trace;
  CleanDelFileOperation operation(trace, attr.fs_id(), attr.ino());
  status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << fmt::format("[gc.delfile] clean file({}/{}) finish.", attr.fs_id(), attr.ino());

  return Status::OK();
}

void CleanExpiredFileSessionTask::Run() {
  auto status = CleanExpiredFileSession();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.filesession] clean filesession fail, {}", status.error_str());
  }

  // forget task
  if (task_memo_ != nullptr) {
    for (const auto& file_session : file_sessions_) {
      task_memo_->Forget(
          MetaCodec::EncodeFileSessionKey(file_session.fs_id(), file_session.ino(), file_session.session_id()));
    }
  }
}

Status CleanExpiredFileSessionTask::CleanExpiredFileSession() {
  class Trace trace;
  DeleteFileSessionOperation operation(trace, file_sessions_);

  return operation_processor_->RunAlone(&operation);
}

bool GcProcessor::Init() {
  CHECK(dist_lock_ != nullptr) << "dist lock is nullptr.";
  CHECK(task_memo_ != nullptr) << "task memo is nullptr.";

  if (!dist_lock_->Init()) {
    DINGO_LOG(ERROR) << "[gc] init dist lock fail.";
    return false;
  }

  worker_set_ = ExecqWorkerSet::New(kWorkerSetName, FLAGS_mds_gc_worker_num, FLAGS_mds_gc_max_pending_task_count);
  return worker_set_->Init();
}

void GcProcessor::Destroy() {
  if (dist_lock_ != nullptr) {
    dist_lock_->Destroy();
  }

  if (worker_set_ != nullptr) {
    worker_set_->Destroy();
  }
}

void GcProcessor::Run() {
  auto status = LaunchGc();
  if (!status.ok()) {
    DINGO_LOG(INFO) << fmt::format("[gc] run gc, {}.", status.error_str());
  }
}

Status GcProcessor::ManualCleanDelSlice(Trace& trace, uint32_t fs_id, Ino ino, uint64_t chunk_index) {
  auto block_accessor = GetOrCreateDataAccesser(fs_id);
  if (block_accessor == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfile] get data accesser fail, fs_id({}).", fs_id);
    return Status(pb::error::EINTERNAL, "get data accesser fail");
  }

  ScanDelSliceOperation operation(
      trace, fs_id, ino, chunk_index, [&](const std::string& key, const std::string& value) -> bool {
        auto task = CleanDelSliceTask::New(operation_processor_, block_accessor, nullptr, key, value);
        auto status = task->CleanDelSlice();
        if (!status.ok()) {
          LOG(ERROR) << fmt::format("[gc.delslice] clean delfile fail, {}.", status.error_str());
          return false;  // stop scanning on error
        }

        return true;
      });

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delslice] scan delslice fail, {}.", status.error_str());
    return status;
  }

  return status;
}

Status GcProcessor::ManualCleanDelFile(Trace& trace, uint32_t fs_id, Ino ino) {
  auto block_accessor = GetOrCreateDataAccesser(fs_id);
  if (block_accessor == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfile] get data accesser fail, fs_id({}).", fs_id);
    return Status(pb::error::EINTERNAL, "get data accesser fail");
  }

  GetDelFileOperation operation(trace, fs_id, ino);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfile] get delfile fail, fs_id({}) ino({}).", fs_id, ino);
    return status;
  }

  auto& result = operation.GetResult();
  const auto& attr = result.attr;

  auto task = CleanDelFileTask::New(operation_processor_, block_accessor, nullptr, attr);
  status = task->CleanDelFile(attr);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfile] clean delfile fail, {}.", status.error_str());
    return status;
  }

  return Status::OK();
}

Status GcProcessor::LaunchGc() {
  bool running = false;
  if (!is_running_.compare_exchange_strong(running, true)) {
    return Status(pb::error::EINTERNAL, "gc already running");
  }

  DEFER(is_running_.store(false));

  if (!dist_lock_->IsLocked()) {
    return Status(pb::error::EINTERNAL, "not own lock");
  }

  auto fses = file_system_set_->GetAllFileSystem();

  if (worker_set_->IsAlmostFull()) {
    return Status(pb::error::EINTERNAL, "worker set is almost full");
  }

  // delslice
  if (FLAGS_mds_gc_delslice_enable) {
    for (auto& fs : fses) {
      ScanDelSlice(fs->FsId());
    }
  }

  // delfile
  if (FLAGS_mds_gc_delfile_enable) {
    for (auto& fs : fses) {
      ScanDelFile(fs->FsId());
    }
  }

  // filesession
  if (FLAGS_mds_gc_filesession_enable) {
    for (auto& fs : fses) {
      ScanExpiredFileSession(fs->FsId());
    }
  }

  return Status::OK();
}

bool GcProcessor::Execute(TaskRunnablePtr task) {
  if (!worker_set_->ExecuteLeastQueue(task)) {
    DINGO_LOG(ERROR) << "[gc] execute task fail.";
    return false;
  }
  return true;
}

bool GcProcessor::Execute(Ino ino, TaskRunnablePtr task) {
  if (!worker_set_->ExecuteHash(ino, task)) {
    DINGO_LOG(ERROR) << "[gc] execute task fail.";
    return false;
  }
  return true;
}

Status GcProcessor::GetClientList(std::set<std::string>& clients) {
  Trace trace;
  ScanClientOperation operation(trace);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc] get client list fail, error({}).", status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  for (auto& client : result.client_entries) {
    clients.insert(client.id());
  }

  return Status::OK();
}

bool GcProcessor::HasFileSession(uint32_t fs_id, Ino ino) {
  Trace trace;
  bool is_exist = false;
  ScanFileSessionOperation operation(trace, fs_id, ino, [&](const FileSessionEntry&) -> bool {
    is_exist = true;
    return false;
  });

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delslice] scan file session fail, {}.", status.error_str());
    return true;
  }

  return is_exist;
}

void GcProcessor::ScanDelSlice(uint32_t fs_id) {
  Trace trace;
  uint32_t count = 0, exec_count = 0;
  ScanDelSliceOperation operation(trace, fs_id, [&](const std::string& key, const std::string& value) -> bool {
    ++count;

    uint32_t fs_id = 0;
    Ino ino = 0;
    uint64_t chunk_index, time_ns;
    MetaCodec::DecodeDelSliceKey(key, fs_id, ino, chunk_index, time_ns);
    CHECK(fs_id > 0) << "invalid fs id.";
    CHECK(ino > 0) << "invalid ino.";

    // check already exist task
    if (task_memo_->Exist(key)) {
      return true;
    }

    // check file session exist
    if (HasFileSession(fs_id, ino)) {
      LOG(INFO) << fmt::format("[gc.delslice.{}.{}] exist file session so skip.", fs_id, ino);
      return true;
    }

    auto block_accessor = GetOrCreateDataAccesser(fs_id);
    if (block_accessor == nullptr) {
      LOG(ERROR) << fmt::format("[gc.delslice] get data accesser fail, fs_id({}).", fs_id);
      return true;
    }

    task_memo_->Remember(key);
    if (!Execute(ino, CleanDelSliceTask::New(operation_processor_, block_accessor, task_memo_, key, value))) {
      task_memo_->Forget(key);
      return false;
    }

    ++exec_count;

    return true;
  });

  auto status = operation_processor_->RunAlone(&operation);

  DINGO_LOG(INFO) << fmt::format("[gc.delslice] scan delslice count({}/{}), status({}).", exec_count, count,
                                 status.error_str());
}

void GcProcessor::ScanDelFile(uint32_t fs_id) {
  Trace trace;
  uint32_t count = 0, exec_count = 0;
  ScanDelFileOperation operation(trace, fs_id, [&](const std::string& key, const std::string& value) -> bool {
    ++count;

    uint32_t fs_id = 0;
    Ino ino = 0;
    MetaCodec::DecodeDelFileKey(key, fs_id, ino);
    CHECK(fs_id > 0) << "invalid fs id.";
    CHECK(ino > 0) << "invalid ino.";

    // check already exist task
    if (task_memo_->Exist(key)) {
      return true;
    }

    // check file session exist
    if (HasFileSession(fs_id, ino)) {
      LOG(INFO) << fmt::format("[gc.delfile.{}.{}] exist file session so skip.", fs_id, ino);
      return true;
    }

    auto block_accessor = GetOrCreateDataAccesser(fs_id);
    if (block_accessor == nullptr) {
      LOG(ERROR) << fmt::format("[gc.delfile] get data accesser fail, fs_id({}).", fs_id);
      return true;
    }

    auto attr = MetaCodec::DecodeDelFileValue(value);
    if (ShouldDeleteFile(attr)) {
      task_memo_->Remember(key);
      if (!Execute(CleanDelFileTask::New(operation_processor_, block_accessor, task_memo_, attr))) {
        task_memo_->Forget(key);
        return false;
      }

      ++exec_count;
    }

    return true;
  });

  auto status = operation_processor_->RunAlone(&operation);

  DINGO_LOG(INFO) << fmt::format("[gc.delfile] scan delfile count({}/{}), status({}).", exec_count, count,
                                 status.error_str());
}

void GcProcessor::RememberFileSessionTask(const std::vector<FileSessionEntry>& file_sessions) {
  if (task_memo_ != nullptr) {
    for (const auto& file_session : file_sessions) {
      task_memo_->Remember(
          MetaCodec::EncodeFileSessionKey(file_session.fs_id(), file_session.ino(), file_session.session_id()));
    }
  }
}

void GcProcessor::ForgotFileSessionTask(const std::vector<FileSessionEntry>& file_sessions) {
  if (task_memo_ != nullptr) {
    for (const auto& file_session : file_sessions) {
      task_memo_->Forget(
          MetaCodec::EncodeFileSessionKey(file_session.fs_id(), file_session.ino(), file_session.session_id()));
    }
  }
}

void GcProcessor::ScanExpiredFileSession(uint32_t fs_id) {
  // get alive clients
  // to dead clients, we will clean their file sessions
  std::set<std::string> alive_clients;
  auto status = GetClientList(alive_clients);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.filesession] get client list fail, {}.", status.error_str());
    return;
  }

  Trace trace;
  uint32_t count = 0, exec_count = 0;
  std::vector<FileSessionEntry> file_sessions;
  ScanFileSessionOperation operation(trace, fs_id, [&](const FileSessionEntry& file_session) -> bool {
    ++count;
    std::string key =
        MetaCodec::EncodeFileSessionKey(file_session.fs_id(), file_session.ino(), file_session.session_id());

    // check already exist task
    if (task_memo_->Exist(key)) {
      return true;
    }

    if (ShouldCleanFileSession(file_session, alive_clients)) {
      file_sessions.push_back(file_session);
    }

    if (file_sessions.size() >= FLAGS_mds_scan_batch_size) {
      RememberFileSessionTask(file_sessions);
      if (!Execute(CleanExpiredFileSessionTask::New(operation_processor_, task_memo_, file_sessions))) {
        ForgotFileSessionTask(file_sessions);
        file_sessions.clear();
        return false;
      }
      exec_count += file_sessions.size();
      file_sessions.clear();
    }

    return true;
  });

  status = operation_processor_->RunAlone(&operation);

  if (!file_sessions.empty()) {
    RememberFileSessionTask(file_sessions);
    if (Execute(CleanExpiredFileSessionTask::New(operation_processor_, task_memo_, file_sessions))) {
      exec_count += file_sessions.size();
    } else {
      ForgotFileSessionTask(file_sessions);
    }
  }

  DINGO_LOG(INFO) << fmt::format("[gc.filesession] scan file session count({}/{}), status({}).", exec_count, count,
                                 status.error_str());
}

bool GcProcessor::ShouldDeleteFile(const AttrEntry& attr) {
  uint64_t now_s = Helper::Timestamp();
  return (attr.ctime() / 1000000000 + FLAGS_mds_gc_delfile_reserve_time_s) < now_s;
}

bool GcProcessor::ShouldCleanFileSession(const FileSessionEntry& file_session,
                                         const std::set<std::string>& alive_clients) {
  // check whether client exist
  if (alive_clients.count(file_session.client_id()) == 0) {
    return true;
  }

  uint64_t now_s = Helper::Timestamp();
  return file_session.create_time_s() + FLAGS_mds_gc_filesession_reserve_time_s < now_s;
}

blockaccess::BlockAccesserSPtr GcProcessor::GetOrCreateDataAccesser(uint32_t fs_id) {
  auto it = block_accessers_.find(fs_id);
  if (it != block_accessers_.end()) {
    return it->second;
  }

  auto fs = file_system_set_->GetFileSystem(fs_id);
  if (fs == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[gc] get filesystem({}) fail.", fs_id);
    return nullptr;
  }

  const auto fs_info = fs->GetFsInfo();

  blockaccess::BlockAccessOptions options;
  if (fs_info.fs_type() == pb::mdsv2::FsType::S3) {
    const auto& s3_info = fs_info.extra().s3_info();
    if (s3_info.ak().empty() || s3_info.sk().empty() || s3_info.endpoint().empty() || s3_info.bucketname().empty()) {
      DINGO_LOG(ERROR) << fmt::format("[gc] get s3 info fail, fs_id({}) s3_info({}).", fs_id,
                                      s3_info.ShortDebugString());
      return nullptr;
    }

    options.type = blockaccess::AccesserType::kS3;
    options.s3_options.s3_info = blockaccess::S3Info{
        .ak = s3_info.ak(), .sk = s3_info.sk(), .endpoint = s3_info.endpoint(), .bucket_name = s3_info.bucketname()};

  } else {
    const auto& rados_info = fs_info.extra().rados_info();
    if (rados_info.mon_host().empty() || rados_info.user_name().empty() || rados_info.key().empty() ||
        rados_info.pool_name().empty()) {
      DINGO_LOG(ERROR) << fmt::format("[gc] get rados info fail, fs_id({}) rados_info({}).", fs_id,
                                      rados_info.ShortDebugString());
      return nullptr;
    }

    options.type = blockaccess::AccesserType::kRados;
    options.rados_options = blockaccess::RadosOptions{.mon_host = rados_info.mon_host(),
                                                      .user_name = rados_info.user_name(),
                                                      .key = rados_info.key(),
                                                      .pool_name = rados_info.pool_name(),
                                                      .cluster_name = rados_info.cluster_name()};
  }

  auto block_accessor = blockaccess::NewShareBlockAccesser(options);
  auto status = block_accessor->Init();
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("[gc] init block accesser fail, status({}).", status.ToString());
    return nullptr;
  }

  block_accessers_[fs_id] = block_accessor;

  return block_accessor;
}

}  // namespace mdsv2
}  // namespace dingofs