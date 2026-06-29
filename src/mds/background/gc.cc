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

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "brpc/reloadable_flags.h"
#include "common/block/block_key.h"
#include "common/block/block_utils.h"
#include "common/blockaccess/prefix_block_accesser.h"
#include "common/blockaccess/rados/rados_common.h"
#include "common/blockaccess/s3/s3_common.h"
#include "common/logging.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mds/common/codec.h"
#include "mds/common/helper.h"
#include "mds/common/runnable.h"
#include "mds/common/status.h"
#include "mds/common/tracing.h"
#include "mds/common/trash.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/storage.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_scan_batch_size);

DEFINE_uint32(mds_gc_worker_num, 256, "gc worker set num");
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

DEFINE_uint32(mds_gc_delslice_reserve_time_s, 480, "gc del slice reserve time");
DEFINE_validator(mds_gc_delslice_reserve_time_s, brpc::PassValidate);

DEFINE_uint32(mds_gc_delfile_reserve_time_s, 600, "gc del file reserve time");
DEFINE_validator(mds_gc_delfile_reserve_time_s, brpc::PassValidate);

DEFINE_bool(mds_gc_trash_enable, true, "gc trash cleanup enable");
DEFINE_validator(mds_gc_trash_enable, brpc::PassValidate);

DEFINE_uint32(mds_gc_trash_scan_budget, 10000,
              "max file dentries collected per CleanTrashTask scan round; "
              "bounds memory and single-scan txn size within one drain round. "
              "Must be >= mds_gc_trash_batch_size.");
DEFINE_validator(mds_gc_trash_scan_budget, brpc::PassValidate);

DEFINE_uint32(mds_gc_trash_batch_size, 256, "batch size for BatchTrashUnlinkOperation inside CleanTrashTask.");
DEFINE_validator(mds_gc_trash_batch_size, brpc::PassValidate);

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

// check whether slice should delete, if file is shared by multiple inodes, we can't delete the slice when gc delfile,
// and need to add reference counting for slice
static bool ShouldDeleteSlice(OperationProcessorSPtr operation_processor, Ino ino, uint64_t slice_id) {
  Trace trace;
  DecSliceRefOperation operation(trace, ino, slice_id);

  auto status = operation_processor->RunAlone(&operation);
  if (!status.ok()) {
    return status.error_code() == pb::error::ENOT_FOUND;
  }

  auto& result = operation.GetResult();

  SliceRefEntry& slice_ref = result.slice_ref;

  LOG(INFO) << fmt::format("[gc.delfile.{}] slice({}) refcount({}).", ino, slice_id, slice_ref.ref_count());

  return slice_ref.ref_count() <= 0;
}

void CleanDelSliceTask::Run() {
  auto status = CleanDelSlice();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[gc.delslice.{}] clean deleted slice fail, {}", ino_, status.error_str());
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
    if (slice.slice().id() == 0) continue;  // skip invalid slice

    // check slice whether should delete
    if (ShouldDeleteSlice(operation_processor_, ino_, slice.slice().id())) {
      auto block_keys = dingofs::EnumerateBlockKeys(slice.slice().id(), slice.slice().size(), slice.block_size());

      for (const auto& block_key : block_keys) {
        keys.push_back(block_key.StoreKey());

        LOG(INFO) << fmt::format("[gc.delslice.{}] delete block key({}).", ino_, block_key.StoreKey());
      }
    }

    slice_id_trace += std::to_string(slice.slice().id());
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

  LOG(INFO) << fmt::format("[gc.delslice.{}] clean slice finish, slice({}).", ino_, slice_id_trace);

  return Status::OK();
}

void CleanDelFileTask::Run() {
  auto status = CleanDelFile(attr_);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[gc.delfile.{}] clean delfile fail, status({}).", attr_.ino(), status.error_str());
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
  if (!status.ok()) return status;

  auto& result = operation.GetResult();

  chunks = std::move(result.chunks);

  return Status::OK();
}

Status CleanDelFileTask::CleanDelFile(const AttrEntry& attr) {
  LOG(INFO) << fmt::format("[gc.delfile.{}] clean delfile, nlink({}) len({}) ctime({}) version({}).", attr.ino(),
                           attr.nlink(), attr.length(), attr.ctime(), attr.version());
  // get file chunks
  std::vector<ChunkEntry> chunks;
  auto status = GetChunks(operation_processor_, attr.fs_id(), attr.ino(), chunks);
  if (!status.ok()) return status;

  // delete data from s3
  std::list<std::string> keys;
  for (const auto& chunk : chunks) {
    for (const auto& slice : chunk.slices()) {
      if (slice.id() == 0) continue;  // skip invalid slice

      // if file is shared by multiple inodes, we can't delete the slice when gc delfile, and need to add reference
      // counting for slice
      if (attr.shared_slice()) {
        if (!ShouldDeleteSlice(operation_processor_, attr.ino(), slice.id())) {
          continue;
        }
      }

      std::vector<BlockKey> block_keys = dingofs::EnumerateBlockKeys(slice.id(), slice.size(), chunk.block_size());

      for (const auto& block_key : block_keys) {
        keys.push_back(block_key.StoreKey());

        LOG(INFO) << fmt::format("[gc.delfile.{}] delete block key({}).", attr.ino(), block_key.StoreKey());
      }
    }
  }

  if (!keys.empty()) {
    auto status = BatchDeleteBlocks(data_accessor_, keys);
    if (!status.ok()) return status;
  }

  // delete inode
  class Trace trace;
  CleanDelFileOperation operation(trace, attr.fs_id(), attr.ino(), attr.maybe_tiny_file());
  status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  LOG(INFO) << fmt::format("[gc.delfile.{}] clean file({}/{}) finish.", attr.ino(), attr.fs_id(), attr.ino());

  return Status::OK();
}

void CleanFileTask::Run() {
  auto status = CleanFile(attr_);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[gc.delfs] clean delfs fail, status({}).", status.error_str());
  }
}

Status CleanFileTask::CleanFile(const AttrEntry& attr) {
  LOG(INFO) << fmt::format("[gc.delfs] clean delfs, ino({}) nlink({}) len({}) version({}).", attr.ino(), attr.nlink(),
                           attr.length(), attr.version());
  // get file chunks
  std::vector<ChunkEntry> chunks;
  auto status = GetChunks(operation_processor_, attr.fs_id(), attr.ino(), chunks);
  if (!status.ok()) {
    return status;
  }

  // delete data from s3
  std::list<std::string> keys;
  for (const auto& chunk : chunks) {
    for (const auto& slice : chunk.slices()) {
      if (slice.id() == 0) continue;  // skip invalid slice

      std::vector<BlockKey> block_keys = dingofs::EnumerateBlockKeys(slice.id(), slice.size(), chunk.block_size());
      for (const auto& block_key : block_keys) {
        keys.push_back(block_key.StoreKey());

        LOG(INFO) << fmt::format("[gc.delfs] delete block key({}).", block_key.StoreKey());
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

  LOG(INFO) << fmt::format("[gc.delfs] clean file({}/{}) finish.", attr.fs_id(), attr.ino());

  return Status::OK();
}

void CleanExpiredFileSessionTask::Run() {
  auto status = CleanExpiredFileSession();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[gc.filesession] clean filesession fail, status({}).", status.error_str());
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
    LOG(ERROR) << "[gc] init dist lock fail.";
    return false;
  }

  worker_set_ = ExecqWorkerSet::New(kWorkerSetName, FLAGS_mds_gc_worker_num, FLAGS_mds_gc_max_pending_task_count, true);
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
    LOG(INFO) << fmt::format("[gc] run gc, {}.", status.error_str());
  }
}

Status GcProcessor::ManualCleanDelSlice(Trace& trace, uint32_t fs_id, Ino ino, uint64_t chunk_index) {
  auto block_accessor = GetOrCreateDataAccesser(fs_id);
  if (block_accessor == nullptr) {
    LOG(ERROR) << fmt::format("[gc.delfile] get data accesser fail, fs_id({}).", fs_id);
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
    LOG(ERROR) << fmt::format("[gc.delslice] scan delslice fail, status({}).", status.error_str());
    return status;
  }

  return status;
}

Status GcProcessor::ManualCleanDelFile(Trace& trace, uint32_t fs_id, Ino ino) {
  auto block_accessor = GetOrCreateDataAccesser(fs_id);
  if (block_accessor == nullptr) {
    LOG(ERROR) << fmt::format("[gc.delfile] get data accesser fail, fs_id({}).", fs_id);
    return Status(pb::error::EINTERNAL, "get data accesser fail");
  }

  GetDelFileOperation operation(trace, fs_id, ino);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[gc.delfile] get delfile fail, fs_id({}) ino({}).", fs_id, ino);
    return status;
  }

  auto& result = operation.GetResult();
  const auto& attr = result.attr;

  auto task = CleanDelFileTask::New(operation_processor_, block_accessor, nullptr, attr);
  status = task->CleanDelFile(attr);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[gc.delfile] clean delfile fail, status({}).", status.error_str());
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

void GcProcessor::RunTrash() {
  if (!FLAGS_mds_gc_trash_enable) {
    return;
  }

  bool running = false;
  if (!is_trash_running_.compare_exchange_strong(running, true)) {
    LOG(INFO) << "[gc.trash] previous sweep still in flight, skip.";
    return;
  }
  DEFER(is_trash_running_.store(false));

  if (!dist_lock_->IsLocked()) {
    return;
  }

  Context ctx;
  std::vector<FsInfoEntry> fs_infoes;
  auto status = file_system_set_->GetAllFsInfo(ctx, true, fs_infoes);
  if (!status.ok()) {
    LOG(INFO) << fmt::format("[gc.trash] get all fs info fail, {}.", status.error_str());
    return;
  }

  if (worker_set_->IsAlmostFull()) {
    LOG(INFO) << "[gc.trash] worker set is almost full, skip this round.";
    return;
  }

  for (auto& fs_info : fs_infoes) {
    ScanTrash(fs_info);
  }
}

bool GcProcessor::Execute(TaskRunnablePtr task) {
  if (!worker_set_->ExecuteLeastQueue(task)) {
    LOG(WARNING) << "[gc] execute task fail.";
    return false;
  }
  return true;
}

bool GcProcessor::Execute(Ino ino, TaskRunnablePtr task) {
  if (!worker_set_->ExecuteHash(ino, task)) {
    LOG(WARNING) << "[gc] execute task fail.";
    return false;
  }
  return true;
}

Status GcProcessor::GetClientList(std::set<std::string>& clients) {
  Trace trace;
  ScanClientOperation operation(trace);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[gc] get client list fail, status({}).", status.error_str());
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
    LOG(ERROR) << fmt::format("[gc.delslice] scan file session fail, {}.", status.error_str());
    return true;
  }

  return is_exist;
}

void GcProcessor::ScanDelSlice(const FsInfoEntry& fs_info) {
  const uint32_t fs_id = fs_info.fs_id();

  const uint64_t now_s = utils::Timestamp();

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

    // check reserve time
    if ((time_ns / 1000000000ULL + FLAGS_mds_gc_delslice_reserve_time_s) > now_s) {
      return true;
    }

    // check already exist task
    if (task_memo_->Exist(key)) {
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

  LOG(INFO) << fmt::format("[gc.delslice.{}] scan delslice count({}/{}), status({}).", fs_id, exec_count, count,
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

  LOG(INFO) << fmt::format("[gc.delfile.{}] scan delfile count({}/{}), status({}).", fs_id, exec_count, count,
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
    LOG(ERROR) << fmt::format("[gc.filesession] get client list fail, status({}).", status.error_str());
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

  LOG(INFO) << fmt::format("[gc.filesession.{}] scan file session count({}/{}), status({}).", fs_id, exec_count, count,
                           status.error_str());
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

  LOG(INFO) << fmt::format("[gc.delfs.{}] scan filesystem count({}/{}/{}), status({}).", fs_id, exec_count, file_count,
                           count, status.error_str());

  // delete file finish
  if ((status.ok() && file_count == 0) || status.error_code() == pb::error::ENOT_FOUND) {
    status = file_system_set_->DestroyFsResource(fs_id);
    LOG(INFO) << fmt::format("[gc.delfs.{}] clean fs resource, status({}).", fs_id, status.error_str());

    status = CleanFsInfo(fs_info);
    LOG(INFO) << fmt::format("[gc.delfs.{}] clean fs info, status({}).", fs_id, status.error_str());
    if (status.ok() && task_memo_ != nullptr) task_memo_->Clear(prefix);
  }
}

bool GcProcessor::ShouldDeleteFile(const AttrEntry& attr) {
  uint64_t now_s = utils::Timestamp();
  return ((attr.ctime() / 1000000000) + FLAGS_mds_gc_delfile_reserve_time_s) < now_s;
}

bool GcProcessor::ShouldCleanFileSession(const FileSessionEntry& file_session,
                                         const std::set<std::string>& alive_clients) {
  // check whether client exist
  if (alive_clients.count(file_session.client_id()) == 0) {
    return true;
  }

  return file_session.expire_time_s() < utils::Timestamp();
}

bool GcProcessor::ShouldRecycleFs(const FsInfoEntry& fs_info) {
  uint32_t now_s = utils::Timestamp();
  return now_s > (fs_info.delete_time_s() + fs_info.recycle_time_hour() * 3600);
}

void GcProcessor::SetFsStateRecycle(const FsInfoEntry& fs_info) {
  Trace trace;
  UpdateFsStateOperation operation(trace, fs_info.fs_name(), pb::mds::FsStatus::RECYCLING);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[gc.delfs.{}] set fs state recycle fail, status({}).", fs_info.fs_id(),
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
    LOG(ERROR) << fmt::format("[gc] get filesystem({}) fail.", fs_id);
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
      LOG(ERROR) << fmt::format("[gc] get s3 info fail, fs_id({}) s3_info({}).", fs_id, s3_info.ShortDebugString());
      return nullptr;
    }

    options.type = blockaccess::AccesserType::kS3;
    options.s3_options.s3_info = blockaccess::S3Info{
        .ak = s3_info.ak(), .sk = s3_info.sk(), .endpoint = s3_info.endpoint(), .bucket_name = s3_info.bucketname()};

  } else if (fs_info.fs_type() == pb::mds::FsType::RADOS) {
    const auto& rados_info = fs_info.extra().rados_info();
    if (rados_info.mon_host().empty() || rados_info.user_name().empty() || rados_info.key().empty() ||
        rados_info.pool_name().empty()) {
      LOG(ERROR) << fmt::format("[gc] get rados info fail, fs_id({}) rados_info({}).", fs_id,
                                rados_info.ShortDebugString());
      return nullptr;
    }

    options.type = blockaccess::AccesserType::kRados;
    options.rados_options = blockaccess::RadosOptions{.mon_host = rados_info.mon_host(),
                                                      .user_name = rados_info.user_name(),
                                                      .key = rados_info.key(),
                                                      .pool_name = rados_info.pool_name(),
                                                      .cluster_name = rados_info.cluster_name()};
  } else if (fs_info.fs_type() == pb::mds::FsType::LOCALFILE) {
    const auto& file_info = fs_info.extra().file_info();
    if (file_info.path().empty()) {
      LOG(ERROR) << fmt::format("[gc] get localfile info fail, fs_id({}) file_info({}).", fs_id,
                                file_info.ShortDebugString());
      return nullptr;
    }

    options.type = blockaccess::AccesserType::kLocalFile;
    options.file_options.path = file_info.path();
  }

  auto block_accessor = blockaccess::NewSharePrefixBlockAccesser(fs_info.fs_name(), options);
  auto status = block_accessor->Init();
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("[gc] init block accesser fail, status({}).", status.ToString());
    return nullptr;
  }

  block_accessers_[fs_id] = block_accessor;

  return block_accessor;
}

// Order invariant: every file in the bucket must be unlinked before any
// directory is rmdir'd — a file's origin_parent may itself be a trashed dir
// in this bucket, and the quota ancestor walk needs that dir to still exist.
// Quota debits diverge by immediate_trash_quota: legacy mode debits per-dir
// AND fs-level here; immediate mode already debited per-dir at trash-move
// time, so this path only releases fs-level when nlink reaches 0.
void CleanTrashTask::Run() {
  LOG(INFO) << fmt::format("[gc.trash.{}.{}] start cleaning bucket {}.", fs_id_, sub_trash_ino_, bucket_name_);
  DEFER(if (task_memo_ != nullptr) task_memo_->Forget(key_));

  auto fs = file_system_set_ != nullptr ? file_system_set_->GetFileSystem(fs_id_) : nullptr;
  const bool immediate_quota = fs != nullptr && fs->GetFsInfo().immediate_trash_quota();

  dingofs::mds::Trace trace;
  uint32_t processed = 0, failed = 0;
  std::vector<Dentry> all_dirs;

  const bool drained = DrainBucketFiles(fs, immediate_quota, trace, all_dirs, processed, failed);
  RmdirBucketDirs(fs, immediate_quota, trace, all_dirs, processed, failed);

  if (drained && failed == 0) {
    DeleteBucketAtomic(trace, processed);
  } else {
    LOG(WARNING) << fmt::format("[gc.trash.{}.{}] bucket {} partial: processed({}) failed({}) aborted({})", fs_id_,
                                sub_trash_ino_, bucket_name_, processed, failed, !drained);
  }
}

// To drain arbitrarily large buckets without unbounded memory or a single
// long-lived scan txn, we loop: each round does a fresh kReadCommitted scan
// that collects up to scan_budget files plus any new directories (deduped by
// ino), processes the files, and repeats. The rmdir pass runs once after the
// file loop terminates so the ORDER INVARIANT (see Run()) still holds.
bool CleanTrashTask::DrainBucketFiles(FileSystemSPtr fs, bool immediate_quota, class Trace& trace,
                                      std::vector<Dentry>& all_dirs, uint32_t& processed, uint32_t& failed) {
  const uint32_t scan_budget = FLAGS_mds_gc_trash_scan_budget;
  const uint32_t batch_size = FLAGS_mds_gc_trash_batch_size;

  // Directories are accumulated across rounds (deduped by ino) and rmdir'd
  // only after every file in the bucket has been unlinked. The count of
  // trashed directories is bounded by the number of rmdir's that happened
  // into this hour bucket, typically small even for large buckets.
  std::unordered_set<Ino> seen_dirs;

  while (true) {
    std::vector<Dentry> files;

    ScanTrashDentryOperation scan_op(trace, fs_id_, sub_trash_ino_, [&](const DentryEntry& dentry) -> bool {
      if (dentry.type() == pb::mds::FileType::DIRECTORY) {
        if (seen_dirs.insert(dentry.ino()).second) {
          all_dirs.emplace_back(dentry);
        }
        return true;  // dirs don't count toward the chunk; typically few
      }
      if (files.size() >= scan_budget) {
        return false;  // chunk boundary; pick up the rest next round
      }
      files.emplace_back(dentry);
      return true;
    });
    scan_op.SetIsolationLevel(Txn::kReadCommitted);

    auto scan_status = operation_processor_->RunAlone(&scan_op);
    if (!scan_status.ok()) {
      LOG(ERROR) << fmt::format("[gc.trash.{}.{}] scan fail, {}", fs_id_, sub_trash_ino_, scan_status.error_str());
      return false;
    }

    if (files.empty()) return true;  // no more files; fall through to rmdir pass

    // batch unlink files (idempotent to NotFound)
    const uint32_t processed_before_round = processed;
    for (size_t i = 0; i < files.size(); i += batch_size) {
      const size_t end = std::min(i + static_cast<size_t>(batch_size), files.size());
      std::vector<Dentry> chunk(files.begin() + i, files.begin() + end);

      BatchTrashUnlinkOperation operation(trace, chunk);
      auto status = operation_processor_->RunAlone(&operation);
      if (!status.ok()) {
        LOG(WARNING) << fmt::format("[gc.trash.{}.{}] batch-unlink [{},{}) fail: {}", fs_id_, sub_trash_ino_, i, end,
                                    status.error_str());
        failed += chunk.size();
        continue;
      }

      auto& result = operation.GetResult();
      processed += result.skipped_count + result.child_attrs.size();

      if (fs != nullptr) {
        // result.child_attrs is aligned with the entries we actually processed
        // (skipped entries are not in the vector). For per-dir quota we walk
        // ancestors starting from origin_parent (parsed out of the trash entry
        // name) — UpdateUsage is fail-soft on missing parents, so an ancestor
        // that has been physically removed since trash-move is silently
        // skipped. FS-level is debited only when the last link is gone.
        size_t attr_idx = 0;
        for (const auto& dentry : chunk) {
          if (attr_idx >= result.child_attrs.size()) break;
          const auto& attr = result.child_attrs[attr_idx];
          if (attr.ino() != dentry.INo()) continue;
          ++attr_idx;

          if (!immediate_quota) {
            const int64_t delta_bytes =
                attr.type() == pb::mds::FileType::FILE ? -static_cast<int64_t>(attr.length()) : 0;
            const std::string reason = fmt::format("trash-clean.{}.{}", sub_trash_ino_, attr.ino());
            Ino origin_parent = ParseTrashEntryName(dentry.Name());
            if (origin_parent != 0) {
              fs->GetQuotaManager().AsyncUpdateDirUsage(origin_parent, delta_bytes, -1, reason);
            }
          }

          if (attr.nlink() <= 0) {
            const int64_t delta_bytes = -static_cast<int64_t>(attr.length());
            fs->GetQuotaManager().UpdateFsUsage(delta_bytes, -1,
                                                fmt::format("trash-clean.{}.{}", sub_trash_ino_, attr.ino()));
          }
        }
      }
    }

    // Safety net: if every batch in this round failed, `processed` doesn't
    // advance and the next scan would see the same files -- don't loop
    // forever. Bail out; the next GC_TRASH cycle will retry.
    if (processed == processed_before_round) {
      LOG(WARNING) << fmt::format(
          "[gc.trash.{}.{}] no forward progress ({} files collected, 0 processed); deferring to next cycle", fs_id_,
          sub_trash_ino_, files.size());
      return false;
    }
  }
}

void CleanTrashTask::RmdirBucketDirs(FileSystemSPtr fs, bool immediate_quota, class Trace& trace,
                                     const std::vector<Dentry>& all_dirs, uint32_t& processed, uint32_t& failed) {
  for (const auto& dir : all_dirs) {
    RmDirOperation operation(trace, dir.FsId(), dir.ParentIno(), dir.Name(), dir.INo());
    auto status = operation_processor_->RunAlone(&operation);
    if (status.ok()) {
      ++processed;
      if (fs != nullptr) {
        // Debit the dir's original parent for the inode count (legacy mode
        // only -- immediate_trash_quota already did this at RmDir-to-trash
        // time), release fs-level, and finally drop the trashed dir's own
        // quota config. Dropping quota config is done here in both modes so
        // restored directories keep their quota config intact through the
        // trash lifecycle.
        if (!immediate_quota) {
          const std::string reason = fmt::format("trash-clean.{}.{}", sub_trash_ino_, dir.INo());
          Ino origin_parent = ParseTrashEntryName(dir.Name());
          if (origin_parent != 0) {
            fs->GetQuotaManager().AsyncUpdateDirUsage(origin_parent, 0, -1, reason);
          }
        }
        fs->GetQuotaManager().AsyncDeleteDirQuota(dir.INo());
        fs->GetQuotaManager().UpdateFsUsage(0, -1, fmt::format("trash-clean.{}.{}", sub_trash_ino_, dir.INo()));
      }
    } else if (status.error_code() == pb::error::ENOT_FOUND) {
      ++processed;  // already gone; idempotent
    } else {
      LOG(WARNING) << fmt::format("[gc.trash.{}.{}] rmdir {} fail: {}", fs_id_, sub_trash_ino_, dir.Name(),
                                  status.error_str());
      ++failed;
    }
  }
}

void CleanTrashTask::DeleteBucketAtomic(class Trace& trace, uint32_t processed) {
  CleanTrashBucketOperation operation(trace, fs_id_, sub_trash_ino_, bucket_name_);
  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[gc.trash.{}.{}] clean bucket {} fail: {}", fs_id_, sub_trash_ino_, bucket_name_,
                              status.error_str());
  } else {
    LOG(INFO) << fmt::format("[gc.trash.{}.{}] bucket {} cleaned, processed({}).", fs_id_, sub_trash_ino_, bucket_name_,
                             processed);
  }
}

void GcProcessor::ScanTrash(const FsInfoEntry& fs_info) {
  const uint32_t fs_id = fs_info.fs_id();
  const uint32_t trash_days = fs_info.trash_days();

  uint64_t now_s = utils::Timestamp();
  uint64_t expire_before_s;
  if (trash_days == 0) {
    // Trash feature disabled — drain any residue left from a prior non-zero
    // retention by treating every existing hour bucket as expired.
    expire_before_s = now_s;
  } else {
    uint64_t expire_s = static_cast<uint64_t>(trash_days) * 24 * 3600 + 1 * 3600;
    expire_before_s = now_s - expire_s;
  }

  Trace trace;
  uint32_t count = 0, exec_count = 0;

  // Scan dentries under kTrashInodeId (the hour bucket directories).
  ScanDentryOperation operation(trace, fs_id, kTrashInodeId, [&](const DentryEntry& dentry) -> bool {
    ++count;
    const std::string& bucket_name = dentry.name();

    uint64_t bucket_ts = ParseTrashBucketName(bucket_name);
    CHECK(bucket_ts != 0) << fmt::format("[gc.trash.{}] invalid bucket name: {}", fs_id, bucket_name);

    if (bucket_ts >= expire_before_s) {
      // Bucket names are "YYYY-MM-DD-HH" — lexicographic scan order matches
      // chronological order, so every bucket after this one is also unexpired.
      return false;
    }

    std::string memo_key = fmt::format("trash.{}.{}", fs_id, dentry.ino());
    if (task_memo_->Exist(memo_key)) {
      return true;
    }

    task_memo_->Remember(memo_key);
    if (!Execute(CleanTrashTask::New(operation_processor_, task_memo_, file_system_set_, fs_id, dentry.ino(),
                                     bucket_name))) {
      task_memo_->Forget(memo_key);
      return false;
    }
    ++exec_count;

    return true;
  });
  operation.SetIsolationLevel(Txn::kReadCommitted);

  auto status = operation_processor_->RunAlone(&operation);

  LOG(INFO) << fmt::format("[gc.trash.{}] scan trash count({}/{}), status({}).", fs_id, exec_count, count,
                           status.error_str());
}

}  // namespace mds
}  // namespace dingofs