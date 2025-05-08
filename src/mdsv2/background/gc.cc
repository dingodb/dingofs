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

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstdint>
#include <string>
#include <vector>

#include "cache/blockcache/cache_store.h"
#include "dataaccess/s3/s3_accesser.h"
#include "dingofs/error.pb.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_int32(fs_scan_batch_size);

DEFINE_uint32(gc_worker_num, 4, "gc worker set num");
DEFINE_uint32(gc_max_pending_task_count, 512, "gc max pending task count");

DEFINE_uint32(gc_del_file_reserve_time_s, 86400, "gc del file reserve time");

static const std::string kWorkerSetName = "GC";

void CleanDeletedSliceTask::Run() {
  auto status = CleanDeletedSlice(kv_.key, kv_.value);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc] clean deleted slice fail, {}", status.error_str());
  }
}

Status CleanDeletedSliceTask::CleanDeletedSlice(const std::string& key, const std::string& value) {
  // delete data from s3
  auto trash_slice_list = MetaCodec::DecodeTrashChunkValue(value);
  for (const auto& slice : trash_slice_list.slices()) {
    DINGO_LOG(INFO) << fmt::format("[gc] clean deleted slice {}/{}/{}/{}.", slice.fs_id(), slice.ino(),
                                   slice.chunk_index(), slice.slice_id());

    for (const auto& range : slice.ranges()) {
      uint64_t index = range.offset() / slice.chunk_size();
      cache::blockcache::BlockKey block_key(slice.fs_id(), slice.ino(), slice.chunk_index(), index, 0);

      auto status = data_accessor_->Delete(block_key.StoreKey());
      if (!status.ok()) {
        return Status(pb::error::EINTERNAL, fmt::format("delete s3 object fail, {}", status.ToString()));
      }
    }
  }

  // delete slice
  auto status = kv_storage_->Delete(key);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc] delete slice fail, {}", status.error_str());
  }

  return status;
}

void CleanDeletedFileTask::Run() {
  auto status = CleanDeletedFile(inode_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc] clean deleted file fail, {}", status.error_str());
  }
}

Status CleanDeletedFileTask::CleanDeletedFile(const AttrType& inode) {
  // delete data from s3
  for (const auto& [_, chunk] : inode.chunks()) {
    DINGO_LOG(INFO) << fmt::format("[gc] clean deleted file {}/{}/{}/{}.", inode.fs_id(), inode.ino(), chunk.index(),
                                   chunk.version());

    for (const auto& slice : chunk.slices()) {
      uint64_t index = slice.offset() / chunk.size();
      cache::blockcache::BlockKey block_key(inode.fs_id(), inode.ino(), chunk.index(), index, 0);

      auto status = data_accessor_->Delete(block_key.StoreKey());
      if (!status.ok()) {
        return Status(pb::error::EINTERNAL, fmt::format("delete s3 object fail, {}", status.ToString()));
      }
    }
  }

  // delete inode
  auto status = kv_storage_->Delete(MetaCodec::EncodeDelFileKey(inode.fs_id(), inode.ino()));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc] clean del file fail, {}", status.error_str());
  }

  return status;
}

bool GcProcessor::Init() {
  dataaccess::aws::S3AdapterOption option;
  data_accessor_ = dataaccess::S3Accesser::New(option);
  CHECK(data_accessor_->Init()) << "init data accesser fail.";

  worker_set_ = ExecqWorkerSet::New(kWorkerSetName, FLAGS_gc_worker_num, FLAGS_gc_max_pending_task_count);
  return worker_set_->Init();
}

void GcProcessor::Destroy() {
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

Status GcProcessor::LaunchGc() {
  bool running = false;
  if (!is_running_.compare_exchange_strong(running, true)) {
    return Status(pb::error::EINTERNAL, "gc already running");
  }

  DEFER(is_running_.store(false));

  if (!dist_lock_->IsLocked()) {
    return Status(pb::error::EINTERNAL, "not own lock");
  }

  // ScanDeletedSlice();
  // ScanDeletedFile();

  return Status::OK();
}

void GcProcessor::Execute(TaskRunnablePtr task) {
  if (!worker_set_->ExecuteLeastQueue(task)) {
    DINGO_LOG(ERROR) << "[gc] execute compact task fail.";
  }
}

void GcProcessor::ScanDeletedSlice() {
  Range range;
  MetaCodec::GetTrashChunkTableRange(range.start_key, range.end_key);

  auto txn = kv_storage_->NewTxn();

  Status status;
  std::vector<KeyValue> kvs;
  do {
    status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      break;
    }

    for (auto& kv : kvs) {
      Execute(CleanDeletedSliceTask::New(kv_storage_, data_accessor_, kv));
    }

  } while (kvs.size() >= FLAGS_fs_scan_batch_size);
}

void GcProcessor::ScanDeletedFile() {
  Range range;
  MetaCodec::GetDelFileTableRange(range.start_key, range.end_key);

  auto txn = kv_storage_->NewTxn();

  Status status;
  std::vector<KeyValue> kvs;
  do {
    status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      break;
    }

    for (auto& kv : kvs) {
      auto attr = MetaCodec::DecodeDelFileValue(kv.value);
      if (ShouldDeleteFile(attr)) {
        Execute(CleanDeletedFileTask::New(kv_storage_, data_accessor_, attr));
      }
    }

  } while (kvs.size() >= FLAGS_fs_scan_batch_size);
}

bool GcProcessor::ShouldDeleteFile(const AttrType& attr) {
  uint64_t now_s = Helper::Timestamp();
  return (attr.ctime() / 1000000000 + FLAGS_gc_del_file_reserve_time_s) < now_s;
}

}  // namespace mdsv2
}  // namespace dingofs