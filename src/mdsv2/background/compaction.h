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

#ifndef DINGOFS_MDSV2_BACKGROUND_COMPACTION_H_
#define DINGOFS_MDSV2_BACKGROUND_COMPACTION_H_

#include <cstdint>

#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/filesystem.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

class CompactChunkProcessor;
using CompactChunkProcessorPtr = std::shared_ptr<CompactChunkProcessor>;

class CompactChunkTask : public TaskRunnable {
 public:
  CompactChunkTask(CompactChunkProcessorPtr compact_chunk_processor, const std::vector<pb::mdsv2::Slice>& delete_slices)
      : compact_chunk_processor_(compact_chunk_processor), delete_slices_(delete_slices) {}

  ~CompactChunkTask() override = default;

  std::string Type() override { return "COMPACT_CHUNK"; }

  void Run() override;

 private:
  CompactChunkProcessorPtr compact_chunk_processor_;

  std::vector<pb::mdsv2::Slice> delete_slices_;
};
using CompactChunkTaskPtr = std::shared_ptr<CompactChunkTask>;

// compact file chunk, main purpose free data store space and improve read performance
// 1. delete invalid slices
// 2. merge slices
class CompactChunkProcessor : public std::enable_shared_from_this<CompactChunkProcessor> {
 public:
  CompactChunkProcessor(KVStoragePtr kv_storage, FileSystemSetPtr fs_set) : kv_storage_(kv_storage), fs_set_(fs_set) {}
  ~CompactChunkProcessor() = default;

  static CompactChunkProcessorPtr New(KVStoragePtr kv_storage, FileSystemSetPtr fs_set) {
    return std::make_shared<CompactChunkProcessor>(kv_storage, fs_set);
  }

  CompactChunkProcessorPtr GetSelfPtr();

  bool Init();
  bool Destroy();

  void LaunchCompaction();

 private:
  Status ScanFileSystem(const pb::mdsv2::FsInfo& fs_info);

  static std::vector<pb::mdsv2::Slice> CheckInvalidSlices(pb::mdsv2::Inode& inode, uint64_t chunk_size);

  Status CleanChunkData(uint32_t fs_id, uint64_t ino, const std::vector<pb::mdsv2::Slice>& slices);
  Status UpdateChunkMetaData(uint32_t fs_id, uint64_t ino, const std::vector<pb::mdsv2::Slice>& delete_slices);

  void Compact(uint32_t fs_id, uint64_t ino, const std::vector<pb::mdsv2::Slice>& delete_slices);

  void ExecuteCompactTask(CompactChunkTaskPtr task);

  std::atomic<bool> is_running_{false};

  // previous process file inode
  uint64_t last_ino_;
  KVStoragePtr kv_storage_;

  FileSystemSetPtr fs_set_;

  WorkerSetPtr worker_set_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_BACKGROUND_COMPACTION_H_