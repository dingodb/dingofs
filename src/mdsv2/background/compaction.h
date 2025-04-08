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
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

class CompactChunkProcessor;
using CompactChunkProcessorPtr = std::shared_ptr<CompactChunkProcessor>;

class CompactChunkTask : public TaskRunnable {
 public:
  CompactChunkTask(CompactChunkProcessorPtr compact_chunk_processor);
  ~CompactChunkTask() override = default;

  std::string Type() override { return "COMPACT_CHUNK"; }

  void Run() override;

 private:
  CompactChunkProcessorPtr compact_chunk_processor_;
};

class CompactChunkProcessor {
 public:
  CompactChunkProcessor() = default;
  ~CompactChunkProcessor() = default;

  bool Init();
  bool Destroy();

  static void TriggerCompaction();

  void Compact(InodePtr inode);

 private:
  Status ScanFile();

  Status CleanSlice(const std::vector<pb::mdsv2::Slice>& slices);

  static std::vector<pb::mdsv2::Slice> CalculateInvalidSlices(std::map<uint64_t, pb::mdsv2::SliceList>& chunk_map,
                                                              uint64_t file_length, uint64_t chunk_size);

  // previous process file inode
  uint64_t last_ino_;
  KVStoragePtr kv_storage_;

  WorkerSetPtr worker_set_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_BACKGROUND_COMPACTION_H_