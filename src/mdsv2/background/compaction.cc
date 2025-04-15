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

#include "mdsv2/background/compaction.h"

#include <sys/types.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "gflags/gflags.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"

namespace dingofs {
namespace mdsv2 {

DEFINE_uint32(compaction_worker_num, 8, "number of compaction workers");
DEFINE_uint32(compaction_task_max_num, 64, "max number of compaction tasks");

DECLARE_int32(fs_scan_batch_size);
DECLARE_uint32(txn_max_retry_times);

void CompactChunkTask::Run() { DoCompact(); }

void CompactChunkTask::DoCompact() {
  Context ctx;
  std::vector<pb::mdsv2::TrashSlice> trash_slices;
  auto status = fs_->CompactChunk(ctx, ino_, 0, trash_slices);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[compaction] compact chunk fail, {}", status.error_str());
    return;
  }

  status = fs_->CleanTrashFileData(ctx, ino_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[compaction] clean trash file data fail, {}", status.error_str());
  }
}

bool CompactChunkProcessor::Init() {
  worker_set_ = ExecqWorkerSet::New("compaction", FLAGS_compaction_worker_num, FLAGS_compaction_task_max_num);
  if (!worker_set_->Init()) {
    DINGO_LOG(ERROR) << "[compaction] init worker set fail.";
    return false;
  }

  return true;
}

bool CompactChunkProcessor::Destroy() {
  worker_set_->Destroy();
  return true;
}

void CompactChunkProcessor::LaunchCompaction() {
  bool running = false;
  if (!is_running_.compare_exchange_strong(running, true)) {
    DINGO_LOG(INFO) << "[compaction] mds already running......";
    return;
  }

  DEFER(is_running_.store(false));

  // scan all filesystems
  auto filesystems = fs_set_->GetAllFileSystem();
  for (auto& filesystem : filesystems) {
    auto status = ScanFileSystem(filesystem);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[compaction][{}] scan filesystem fail, {}.", filesystem->FsName(),
                                      status.error_str());
    }
  }
}

Status CompactChunkProcessor::ScanFileSystem(FileSystemPtr fs) {
  auto fs_info = fs->FsInfo();

  Range range;
  MetaDataCodec::GetFileInodeTableRange(fs_info.fs_id(), range.start_key, range.end_key);
  range.start_key = MetaDataCodec::EncodeInodeKey(fs_info.fs_id(), last_ino_);

  std::vector<KeyValue> kvs;
  do {
    auto txn = kv_storage_->NewTxn();

    auto status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      return status;
    }

    for (const auto& kv : kvs) {
      auto pb_inode = MetaDataCodec::DecodeInodeValue(kv.value);
      ExecuteCompactTask(CompactChunkTask::New(fs, pb_inode.ino()));
    }

  } while (kvs.size() >= FLAGS_fs_scan_batch_size);

  return Status::OK();
}

void CompactChunkProcessor::ExecuteCompactTask(CompactChunkTaskPtr task) {
  if (!worker_set_->ExecuteLeastQueue(task)) {
    DINGO_LOG(ERROR) << "[compaction] execute compact task fail.";
  }
}

}  // namespace mdsv2
}  // namespace dingofs
