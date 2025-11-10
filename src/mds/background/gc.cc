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

#include "mds/background/gc.h"

#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "brpc/reloadable_flags.h"
#include "cache/blockcache/cache_store.h"
#include "common/blockaccess/rados/rados_common.h"
#include "common/blockaccess/s3/s3_common.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mds/common/codec.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/common/runnable.h"
#include "mds/common/status.h"
#include "mds/common/tracing.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/storage.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_scan_batch_size);

DEFINE_uint32(mds_gc_worker_num, 32, "gc worker set num");
DEFINE_validator(mds_gc_worker_num, brpc::PassValidate);
DEFINE_uint32(mds_gc_max_pending_task_count, 8192, "gc max pending task count");
DEFINE_validator(mds_gc_max_pending_task_count, brpc::PassValidate);

DEFINE_bool(mds_gc_delslice_enable, true, "gc delslice enable");
DEFINE_validator(mds_gc_delslice_enable, brpc::PassValidate);
DEFINE_bool(mds_gc_delfile_enable, true, "gc delfile enable");
DEFINE_validator(mds_gc_delfile_enable, brpc::PassValidate);
DEFINE_bool(mds_gc_filesession_enable, true, "gc filesession enable");
DEFINE_validator(mds_gc_filesession_enable, brpc::PassValidate);
DEFINE_bool(mds_gc_delfs_enable, true, "gc delfs enable");
DEFINE_validator(mds_gc_delfs_enable, brpc::PassValidate);

DEFINE_uint32(mds_gc_delfile_reserve_time_s, 600, "gc del file reserve time");
DEFINE_validator(mds_gc_delfile_reserve_time_s, brpc::PassValidate);

DEFINE_uint32(mds_gc_filesession_reserve_time_s, 86400, "gc file session reserve time");
DEFINE_validator(mds_gc_filesession_reserve_time_s, brpc::PassValidate);

static const std::string kWorkerSetName = "GC";

static const uint32_t kBatchDeleteObjectSize = 1000;

// batch delete s3 object
static Status BatchDeleteBlocks(blockaccess::BlockAccesserSPtr& data_accessor, const std::list<std::string>& keys) {
  if (keys.size() <= kBatchDeleteObjectSize) {
    auto status = data_accessor->BatchDelete(keys);
    if (!status.ok()) {
      return Status(pb::error::EINTERNAL,
                    fmt::format("delete s3 object fail, keys({}) status({}).", keys.size(), status.ToString()));
    }
  } else {
    auto it = keys.begin();
    while (it != keys.end()) {
      std::list<std::string> batch_keys;
      for (uint32_t i = 0; i < kBatchDeleteObjectSize && it != keys.end(); ++i, ++it) {
        batch_keys.push_back(*it);
      }

      auto status = data_accessor->BatchDelete(batch_keys);
      if (!status.ok()) {
        return Status(pb::error::EINTERNAL,
                      fmt::format("delete s3 object fail, keys({}) status({}).", batch_keys.size(), status.ToString()));
      }
    }
  }

  return Status::OK();
}

// range [start, end)
static IntRange CalBlockIndex(uint64_t block_size, uint64_t chunk_offset, const SliceEntry& slice) {
  IntRange range;
  range.start = (slice.offset() - chunk_offset) / block_size;

  uint64_t end_offset = slice.offset() - chunk_offset + slice.len();
  range.end = (end_offset % block_size == 0) ? (end_offset / block_size) : ((end_offset / block_size) + 1);

  return range;
}

// range [start, end)
static IntRange CalBlockIndex(uint64_t block_size, uint64_t chunk_offset, const TrashSliceEntry::Range& slice) {
  IntRange range;
  range.start = (slice.offset() - chunk_offset) / block_size;

  uint64_t end_offset = slice.offset() - chunk_offset + slice.len();
  range.end = (end_offset % block_size == 0) ? (end_offset / block_size) : ((end_offset / block_size) + 1);

  return range;
}

void CleanDelSliceTask::Run() {
  auto status = CleanDelSlice();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delslice.{}] clean deleted slice fail, {}", ino_, status.error_str());
  }

  // forget task
  if (task_memo_ != nullptr) task_memo_->Forget(key_);
}

Status CleanDelSliceTask::CleanDelSlice() {
  std::list<std::string> keys;
  std::string slice_id_trace;
  auto trash_slice_list = MetaCodec::DecodeDelSliceValue(value_);
  for (int i = 0; i < trash_slice_list.slices_size(); ++i) {
    const auto& slice = trash_slice_list.slices().at(i);

    uint64_t chunk_offset = slice.chunk_index() * slice.chunk_size();
    for (const auto& slice_range : slice.ranges()) {
      auto range = CalBlockIndex(slice.block_size(), chunk_offset, slice_range);
      for (uint32_t block_index = range.start; block_index < range.end; ++block_index) {
        cache::BlockKey block_key(slice.fs_id(), slice.ino(), slice.slice_id(), block_index,
                                  slice_range.compaction_version());

        DINGO_LOG(INFO) << fmt::format("[gc.delslice.{}] delete block key({}).", ino_, block_key.StoreKey());
        keys.push_back(block_key.StoreKey());
      }
    }

    slice_id_trace += std::to_string(slice.slice_id());
    if (i + 1 < trash_slice_list.slices_size()) {
      slice_id_trace += ",";
    }
  }

  // delete data from s3
  if (!keys.empty()) {
    auto status = BatchDeleteBlocks(data_accessor_, keys);
    if (!status.ok()) return status;
  }

  // delete slice
  class Trace trace;
  CleanDelSliceOperation operation(trace, key_);
  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << fmt::format("[gc.delslice.{}] clean slice finish, slice({}).", ino_, slice_id_trace);

  return Status::OK();
}

void CleanDelFileTask::Run() {
  auto status = CleanDelFile(attr_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfile.{}] clean delfile fail, status({}).", attr_.ino(), status.error_str());
  }

  // forget task
  if (task_memo_ != nullptr) {
    task_memo_->Forget(MetaCodec::EncodeDelFileKey(attr_.fs_id(), attr_.ino()));
  }
}

static Status GetChunks(OperationProcessorSPtr operation_processor, uint32_t fs_id, Ino ino,
                        std::vector<ChunkEntry>& chunks) {
  class Trace trace;
  ScanChunkOperation operation(trace, fs_id, ino);
  operation.SetIsolationLevel(Txn::kReadCommitted);

  auto status = operation_processor->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  auto& result = operation.GetResult();

  chunks = std::move(result.chunks);

  return Status::OK();
}

Status CleanDelFileTask::CleanDelFile(const AttrEntry& attr) {
  DINGO_LOG(INFO) << fmt::format("[gc.delfile.{}] clean delfile, nlink({}) len({}) version({}).", attr.ino(),
                                 attr.nlink(), attr.length(), attr.version());
  // get file chunks
  std::vector<ChunkEntry> chunks;
  auto status = GetChunks(operation_processor_, attr.fs_id(), attr.ino(), chunks);
  if (!status.ok()) {
    return status;
  }

  // delete data from s3
  std::list<std::string> keys;
  for (const auto& chunk : chunks) {
    uint64_t chunk_offset = chunk.index() * chunk.chunk_size();
    for (const auto& slice : chunk.slices()) {
      auto range = CalBlockIndex(chunk.block_size(), chunk_offset, slice);
      for (uint32_t block_index = range.start; block_index < range.end; ++block_index) {
        cache::BlockKey block_key(attr.fs_id(), attr.ino(), slice.id(), block_index, slice.compaction_version());

        DINGO_LOG(INFO) << fmt::format("[gc.delfile.{}] delete block key({}).", attr.ino(), block_key.StoreKey());
        keys.push_back(block_key.StoreKey());
      }
    }
  }

  if (!keys.empty()) {
    auto status = BatchDeleteBlocks(data_accessor_, keys);
    if (!status.ok()) return status;
  }

  // delete inode
  class Trace trace;
  CleanDelFileOperation operation(trace, attr.fs_id(), attr.ino());
  status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << fmt::format("[gc.delfile.{}] clean file({}/{}) finish.", attr.ino(), attr.fs_id(), attr.ino());

  return Status::OK();
}

void CleanFileTask::Run() {
  auto status = CleanFile(attr_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfs] clean delfs fail, status({}).", status.error_str());
  }
}

Status CleanFileTask::CleanFile(const AttrEntry& attr) {
  DINGO_LOG(INFO) << fmt::format("[gc.delfs] clean delfs, ino({}) nlink({}) len({}) version({}).", attr.ino(),
                                 attr.nlink(), attr.length(), attr.version());
  // get file chunks
  std::vector<ChunkEntry> chunks;
  auto status = GetChunks(operation_processor_, attr.fs_id(), attr.ino(), chunks);
  if (!status.ok()) {
    return status;
  }

  // delete data from s3
  std::list<std::string> keys;
  for (const auto& chunk : chunks) {
    uint64_t chunk_offset = chunk.index() * chunk.chunk_size();
    for (const auto& slice : chunk.slices()) {
      auto range = CalBlockIndex(chunk.block_size(), chunk_offset, slice);
      for (uint32_t block_index = range.start; block_index < range.end; ++block_index) {
        cache::BlockKey block_key(attr.fs_id(), attr.ino(), slice.id(), block_index, slice.compaction_version());

        DINGO_LOG(INFO) << fmt::format("[gc.delfs] delete block key({}).", block_key.StoreKey());
        keys.push_back(block_key.StoreKey());
      }
    }
  }

  if (!keys.empty()) {
    auto status = BatchDeleteBlocks(data_accessor_, keys);
    if (!status.ok()) return status;
  }

  // update recyle progress
  class Trace trace;
  UpdateFsRecycleProgressOperation operation(trace, fs_name_, attr.ino());
  status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << fmt::format("[gc.delfs] clean file({}/{}) finish.", attr.fs_id(), attr.ino());

  return Status::OK();
}

void CleanExpiredFileSessionTask::Run() {
  auto status = CleanExpiredFileSession();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.filesession] clean filesession fail, status({}).", status.error_str());
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
        auto task = CleanDelSliceTask::New(operation_processor_, block_accessor, nullptr, ino, key, value);
        auto status = task->CleanDelSlice();
        if (!status.ok()) {
          LOG(ERROR) << fmt::format("[gc.delslice] clean delfile fail, status({}).", status.error_str());
          return false;  // stop scanning on error
        }

        return true;
      });

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delslice] scan delslice fail, status({}).", status.error_str());
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
    DINGO_LOG(ERROR) << fmt::format("[gc.delfile] clean delfile fail, status({}).", status.error_str());
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

  Context ctx;
  std::vector<FsInfoEntry> fs_infoes;
  auto status = file_system_set_->GetAllFsInfo(ctx, true, fs_infoes);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, fmt::format("get all fs info fail, {}", status.error_str()));
  }

  if (worker_set_->IsAlmostFull()) {
    return Status(pb::error::EINTERNAL, "worker set is almost full");
  }

  // delslice
  if (FLAGS_mds_gc_delslice_enable) {
    for (auto& fs_info : fs_infoes) {
      ScanDelSlice(fs_info);
    }
  }

  // delfile
  if (FLAGS_mds_gc_delfile_enable) {
    for (auto& fs_info : fs_infoes) {
      ScanDelFile(fs_info);
    }
  }

  // filesession
  if (FLAGS_mds_gc_filesession_enable) {
    for (auto& fs_info : fs_infoes) {
      ScanExpiredFileSession(fs_info);
    }
  }

  // fs
  if (FLAGS_mds_gc_delfs_enable) {
    Context ctx;
    std::vector<FsInfoEntry> fs_infoes;
    file_system_set_->GetAllFsInfo(ctx, true, fs_infoes);
    for (auto& fs_info : fs_infoes) {
      if (fs_info.is_deleted() && ShouldRecycleFs(fs_info)) {
        ScanDelFs(fs_info);
      }
    }
  }

  return Status::OK();
}

bool GcProcessor::Execute(TaskRunnablePtr task) {
  if (!worker_set_->ExecuteLeastQueue(task)) {
    DINGO_LOG(WARNING) << "[gc] execute task fail.";
    return false;
  }
  return true;
}

bool GcProcessor::Execute(Ino ino, TaskRunnablePtr task) {
  if (!worker_set_->ExecuteHash(ino, task)) {
    DINGO_LOG(WARNING) << "[gc] execute task fail.";
    return false;
  }
  return true;
}

Status GcProcessor::GetClientList(std::set<std::string>& clients) {
  Trace trace;
  ScanClientOperation operation(trace);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc] get client list fail, status({}).", status.error_str());
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

void GcProcessor::ScanDelSlice(const FsInfoEntry& fs_info) {
  const uint32_t fs_id = fs_info.fs_id();

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

    auto block_accessor = GetOrCreateDataAccesser(fs_info);
    if (block_accessor == nullptr) {
      LOG(ERROR) << fmt::format("[gc.delslice] get data accesser fail, fs_id({}).", fs_id);
      return true;
    }

    task_memo_->Remember(key);
    if (!Execute(ino, CleanDelSliceTask::New(operation_processor_, block_accessor, task_memo_, ino, key, value))) {
      task_memo_->Forget(key);
      return false;
    }

    ++exec_count;

    return true;
  });
  operation.SetIsolationLevel(Txn::kReadCommitted);

  auto status = operation_processor_->RunAlone(&operation);

  DINGO_LOG(INFO) << fmt::format("[gc.delslice.{}] scan delslice count({}/{}), status({}).", fs_id, exec_count, count,
                                 status.error_str());
}

void GcProcessor::ScanDelFile(const FsInfoEntry& fs_info) {
  const uint32_t fs_id = fs_info.fs_id();

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

    auto block_accessor = GetOrCreateDataAccesser(fs_info);
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
  operation.SetIsolationLevel(Txn::kReadCommitted);

  auto status = operation_processor_->RunAlone(&operation);

  DINGO_LOG(INFO) << fmt::format("[gc.delfile.{}] scan delfile count({}/{}), status({}).", fs_id, exec_count, count,
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

void GcProcessor::ScanExpiredFileSession(const FsInfoEntry& fs_info) {
  const uint32_t fs_id = fs_info.fs_id();

  // get alive clients
  // to dead clients, we will clean their file sessions
  std::set<std::string> alive_clients;
  auto status = GetClientList(alive_clients);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.filesession] get client list fail, status({}).", status.error_str());
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
  operation.SetIsolationLevel(Txn::kReadCommitted);

  status = operation_processor_->RunAlone(&operation);

  if (!file_sessions.empty()) {
    RememberFileSessionTask(file_sessions);
    if (Execute(CleanExpiredFileSessionTask::New(operation_processor_, task_memo_, file_sessions))) {
      exec_count += file_sessions.size();
    } else {
      ForgotFileSessionTask(file_sessions);
    }
  }

  DINGO_LOG(INFO) << fmt::format("[gc.filesession.{}] scan file session count({}/{}), status({}).", fs_id, exec_count,
                                 count, status.error_str());
}

void GcProcessor::ScanDelFs(const FsInfoEntry& fs_info) {
  const uint32_t fs_id = fs_info.fs_id();

  // set fs state recycle
  if (fs_info.status() == pb::mds::FsStatus::DELETED) {
    SetFsStateRecycle(fs_info);
  }

  const auto& recycle_progress = fs_info.recycle_progress();
  uint32_t count = 0, file_count = 0, exec_count = 0;

  Trace trace;
  std::string prefix = "delfs." + std::to_string(fs_id) + ".";
  std::string start_key =
      recycle_progress.last_ino() == 0 ? "" : MetaCodec::EncodeInodeKey(fs_id, recycle_progress.last_ino());
  ScanFsMetaTableOperation operation(
      trace, fs_id, start_key, [&](const std::string& key, const std::string& value) -> bool {
        ++count;
        if (!MetaCodec::IsInodeKey(key)) return true;

        // check already exist task
        std::string memo_key = prefix + key;
        if (task_memo_->Exist(memo_key)) return true;

        auto attr = MetaCodec::DecodeInodeValue(value);
        if (attr.type() == pb::mds::FileType::DIRECTORY) return true;

        ++file_count;

        auto block_accessor = GetOrCreateDataAccesser(fs_info);
        if (block_accessor == nullptr) {
          LOG(ERROR) << fmt::format("[gc.delfs] get data accesser fail, fs_id({}).", fs_id);
          return true;
        }

        if (!Execute(CleanFileTask::New(operation_processor_, block_accessor, task_memo_, fs_info.fs_name(), attr))) {
          return false;
        }
        task_memo_->Remember(memo_key);

        ++exec_count;

        return true;
      });
  operation.SetIsolationLevel(Txn::kReadCommitted);

  auto status = operation_processor_->RunAlone(&operation);

  DINGO_LOG(INFO) << fmt::format("[gc.delfs.{}] scan filesystem count({}/{}/{}), status({}).", fs_id, exec_count,
                                 file_count, count, status.error_str());

  // delete file finish
  if ((status.ok() && file_count == 0) || status.error_code() == pb::error::ENOT_FOUND) {
    status = file_system_set_->DestroyFsResource(fs_id);
    DINGO_LOG(INFO) << fmt::format("[gc.delfs.{}] clean fs resource, status({}).", fs_id, status.error_str());

    status = CleanFsInfo(fs_info);
    DINGO_LOG(INFO) << fmt::format("[gc.delfs.{}] clean fs info, status({}).", fs_id, status.error_str());
    if (status.ok() && task_memo_ != nullptr) task_memo_->Clear(prefix);
  }
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

bool GcProcessor::ShouldRecycleFs(const FsInfoEntry& fs_info) {
  uint32_t now_s = Helper::Timestamp();
  return now_s > (fs_info.delete_time_s() + fs_info.recycle_time_hour() * 3600);
}

void GcProcessor::SetFsStateRecycle(const FsInfoEntry& fs_info) {
  Trace trace;
  UpdateFsStateOperation operation(trace, fs_info.fs_name(), pb::mds::FsStatus::RECYCLING);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfs.{}] set fs state recycle fail, status({}).", fs_info.fs_id(),
                                    status.error_str());
  }
}

Status GcProcessor::CleanFsInfo(const FsInfoEntry& fs_info) {
  Trace trace;
  CleanFsOperation operation(trace, fs_info.fs_name(), fs_info.fs_id());

  return operation_processor_->RunAlone(&operation);
}

blockaccess::BlockAccesserSPtr GcProcessor::GetOrCreateDataAccesser(uint32_t fs_id) {
  auto fs = file_system_set_->GetFileSystem(fs_id);
  if (fs == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[gc] get filesystem({}) fail.", fs_id);
    return nullptr;
  }

  return GetOrCreateDataAccesser(fs->GetFsInfo());
}

blockaccess::BlockAccesserSPtr GcProcessor::GetOrCreateDataAccesser(const FsInfoEntry& fs_info) {
  const uint32_t fs_id = fs_info.fs_id();

  auto it = block_accessers_.find(fs_id);
  if (it != block_accessers_.end()) {
    return it->second;
  }

  blockaccess::BlockAccessOptions options;
  if (fs_info.fs_type() == pb::mds::FsType::S3) {
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

}  // namespace mds
}  // namespace dingofs