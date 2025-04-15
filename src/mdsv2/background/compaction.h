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

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/filesystem.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

class CompactChunkProcessor;
using CompactChunkProcessorPtr = std::shared_ptr<CompactChunkProcessor>;

class CompactChunkTask;
using CompactChunkTaskPtr = std::shared_ptr<CompactChunkTask>;

class CompactChunkTask : public TaskRunnable {
 public:
  CompactChunkTask(FileSystemPtr fs, uint64_t ino) : fs_(fs), ino_(ino) {}
  ~CompactChunkTask() override = default;

  static CompactChunkTaskPtr New(FileSystemPtr fs, uint64_t ino) { return std::make_shared<CompactChunkTask>(fs, ino); }

  std::string Type() override { return "COMPACT_CHUNK"; }

  void Run() override;

 private:
  void DoCompact();

  FileSystemPtr fs_;
  uint64_t ino_;
};
using CompactChunkTaskPtr = std::shared_ptr<CompactChunkTask>;

// compact file chunk, main purpose free data store space and improve read performance
// 1. delete invalid slices
// 2. merge slices
// key term:
//  chunk: a chunk is a collection of slices, it is the smallest unit of data in a file
//  slice: a slice is a continuous data range in chunk
//  block: a block is a s3 object
class CompactChunkProcessor {
 public:
  CompactChunkProcessor(KVStoragePtr kv_storage, FileSystemSetPtr fs_set) : kv_storage_(kv_storage), fs_set_(fs_set) {}
  ~CompactChunkProcessor() = default;

  static CompactChunkProcessorPtr New(KVStoragePtr kv_storage, FileSystemSetPtr fs_set) {
    return std::make_shared<CompactChunkProcessor>(kv_storage, fs_set);
  }

  bool Init();
  bool Destroy();

  void LaunchCompaction();

 private:
  Status ScanFileSystem(FileSystemPtr fs);

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