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

#include <gflags/gflags.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"

namespace dingofs {
namespace mdsv2 {

DEFINE_uint32(compaction_worker_num, 8, "number of compaction workers");
DEFINE_uint32(compaction_task_max_num, 64, "max number of compaction tasks");

DECLARE_int32(fs_scan_batch_size);
DECLARE_uint32(txn_max_retry_times);

struct DeleteSlice {
  uint64_t slice_id;
  std::string reason;
};

struct OffsetRange {
  uint64_t start;
  uint64_t end;
  std::vector<pb::mdsv2::Slice> slices;
};

void CompactChunkTask::Run() {}

CompactChunkProcessorPtr CompactChunkProcessor::GetSelfPtr() {
  return std::dynamic_pointer_cast<CompactChunkProcessor>(shared_from_this());
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
    DINGO_LOG(INFO) << "[monitor] mds already running......";
    return;
  }

  DEFER(is_running_.store(false));

  // scan all filesystems
  auto filesystems = fs_set_->GetAllFileSystem();
  for (auto& filesystem : filesystems) {
    auto fs_info = filesystem->FsInfo();
    auto status = ScanFileSystem(fs_info);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[compaction][{}] scan filesystem fail, {}.", fs_info.fs_name(),
                                      status.error_str());
    }
  }
}

Status CompactChunkProcessor::ScanFileSystem(const pb::mdsv2::FsInfo& fs_info) {
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
      auto delete_slices = CheckInvalidSlices(pb_inode, fs_info.chunk_size());
      auto task = std::make_shared<CompactChunkTask>(GetSelfPtr(), delete_slices);
      ExecuteCompactTask(task);
    }

  } while (kvs.size() >= FLAGS_fs_scan_batch_size);

  return Status::OK();
}

static std::map<uint64_t, pb::mdsv2::Slice> GenSliceMap(const std::vector<pb::mdsv2::Slice>& delete_slices) {
  std::map<uint64_t, pb::mdsv2::Slice> slice_map;
  for (const auto& slice : delete_slices) {
    slice_map[slice.id()] = slice;
  }

  return slice_map;
}

static std::map<uint64_t, pb::mdsv2::Slice> GenSliceMap(const std::map<uint64_t, pb::mdsv2::SliceList>& chunk_map) {
  std::map<uint64_t, pb::mdsv2::Slice> slice_map;
  for (const auto& [chunk_index, chunk] : chunk_map) {
    for (const auto& slice : chunk.slices()) {
      slice_map[slice.id()] = slice;
    }
  }

  return slice_map;
}

std::vector<pb::mdsv2::Slice> CompactChunkProcessor::CheckInvalidSlices(pb::mdsv2::Inode& inode, uint64_t chunk_size) {
  auto chunk_map = inode.chunks();
  uint64_t file_length = inode.length();

  std::vector<pb::mdsv2::Slice> delete_slices;

  // find out-of-file-length slices
  for (const auto& [chunk_index, chunk] : chunk_map) {
    if ((chunk_index + 1) * chunk_size < file_length) {
      continue;
    }

    for (const auto& slice : chunk.slices()) {
      if (slice.offset() >= file_length) {
        delete_slices.push_back(slice);
      }
    }
  }

  // get covered slices
  for (auto& [chunk_index, chunk] : chunk_map) {
    if ((chunk_index + 1) * chunk_size < file_length) {
      continue;
    }

    // sort by offset
    std::sort(chunk.mutable_slices()->begin(), chunk.mutable_slices()->end(),
              [](const pb::mdsv2::Slice& a, const pb::mdsv2::Slice& b) { return a.offset() < b.offset(); });

    // get offset ranges
    std::vector<uint64_t> offsets;
    for (const auto& slice : chunk.slices()) {
      offsets.push_back(slice.offset());
      offsets.push_back(slice.offset() + slice.len());
    }

    std::sort(offsets.begin(), offsets.end());

    std::vector<OffsetRange> offset_ranges;
    for (size_t i = 0; i < offsets.size() - 1; ++i) {
      offset_ranges.push_back({.start = offsets[i], .end = offsets[i + 1]});
    }

    for (auto& offset_range : offset_ranges) {
      for (const auto& slice : chunk.slices()) {
        uint64_t slice_start = slice.offset();
        uint64_t slice_end = slice.offset() + slice.len();
        if ((slice_start >= offset_range.start && slice_start < offset_range.end) ||
            (slice_end >= offset_range.start && slice_end < offset_range.end)) {
          offset_range.slices.push_back(slice);
        }
      }
    }

    std::set<uint64_t> reserve_slice_ids;
    for (auto& offset_range : offset_ranges) {
      std::sort(offset_range.slices.begin(), offset_range.slices.end(),
                [](const pb::mdsv2::Slice& a, const pb::mdsv2::Slice& b) { return a.id() > b.id(); });
      reserve_slice_ids.insert(offset_range.slices.front().id());
    }

    // delete slices
    for (const auto& slice : chunk.slices()) {
      if (reserve_slice_ids.count(slice.id()) == 0) {
        delete_slices.push_back(slice);
      }
    }
  }

  return delete_slices;
}

Status CompactChunkProcessor::CleanChunkData(uint32_t fs_id, uint64_t ino,
                                             const std::vector<pb::mdsv2::Slice>& slices) {
  return Status::OK();
}

Status CompactChunkProcessor::UpdateChunkMetaData(uint32_t fs_id, uint64_t ino,
                                                  const std::vector<pb::mdsv2::Slice>& delete_slices) {
  std::string key = MetaDataCodec::EncodeInodeKey(fs_id, ino);

  Status status;
  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();
    std::string value;
    status = txn->Get(key, value);
    if (!status.ok()) {
      return status;
    }

    auto slice_map = GenSliceMap(delete_slices);
    auto pb_inode = MetaDataCodec::DecodeInodeValue(value);
    // delete slices from chunks
    for (auto chunk_it = pb_inode.mutable_chunks()->begin(); chunk_it != pb_inode.mutable_chunks()->end();) {
      auto& chunk = chunk_it->second;
      for (auto slice_it = chunk.mutable_slices()->begin(); slice_it != chunk.mutable_slices()->end();) {
        if (slice_map.count(slice_it->id()) > 0) {
          slice_it = chunk.mutable_slices()->erase(slice_it);
        } else {
          ++slice_it;
        }
      }

      if (chunk.slices().empty()) {
        chunk_it = pb_inode.mutable_chunks()->erase(chunk_it);
      } else {
        ++chunk_it;
      }
    }

    txn->Put(key, MetaDataCodec::EncodeInodeValue(pb_inode));

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  DINGO_LOG(INFO) << fmt::format("[compaction][{}.{}] update chunk finish.", fs_id, ino);

  return status;
}

void CompactChunkProcessor::ExecuteCompactTask(CompactChunkTaskPtr task) {
  if (!worker_set_->ExecuteLeastQueue(task)) {
    DINGO_LOG(ERROR) << "execute compact task fail.";
  }
}

// delete invalid slice
// merge slices
void CompactChunkProcessor::Compact(uint32_t fs_id, uint64_t ino, const std::vector<pb::mdsv2::Slice>& delete_slices) {
  auto status = CleanChunkData(fs_id, ino, delete_slices);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[compaction] clean slice data fail, {}", status.error_str());
    return;
  }

  status = UpdateChunkMetaData(fs_id, ino, delete_slices);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[compaction] update chunk meta data fail, {}", status.error_str());
    return;
  }
}

}  // namespace mdsv2
}  // namespace dingofs
