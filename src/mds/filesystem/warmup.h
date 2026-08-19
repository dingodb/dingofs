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

#ifndef DINGOFS_MDS_FILESYSTEM_WARMUP_H_
#define DINGOFS_MDS_FILESYSTEM_WARMUP_H_

#include <cstdint>
#include <vector>

#include "mds/common/runnable.h"
#include "mds/common/status.h"
#include "mds/filesystem/chunk_cache.h"
#include "mds/filesystem/store_operation.h"

namespace dingofs {
namespace mds {

class WarmupChunkTask : public TaskRunnable {
 public:
  WarmupChunkTask(uint32_t fs_id, Ino ino, const std::vector<uint32_t>& chunk_indexes, ChunkCache& chunk_cache,
                  OperationProcessorSPtr operation_processor)
      : fs_id_(fs_id),
        ino_(ino),
        chunk_indexes_(chunk_indexes),
        chunk_cache_(chunk_cache),
        operation_processor_(operation_processor) {}
  ~WarmupChunkTask() override = default;

  std::string Type() override { return "WARMUP_CHUNK"; }

  void Run() override;

 private:
  bool IsCached();

  Status WarmupChunk();

  uint32_t fs_id_;
  Ino ino_;

  std::vector<uint32_t> chunk_indexes_;

  ChunkCache& chunk_cache_;
  OperationProcessorSPtr operation_processor_;
};

class WarmupProcessor {
 public:
  WarmupProcessor(ChunkCache& chunk_cache, OperationProcessorSPtr operation_processor);
  virtual ~WarmupProcessor() = default;

  WarmupProcessor(const WarmupProcessor&) = delete;
  WarmupProcessor& operator=(const WarmupProcessor&) = delete;

  bool Init();

  void Execute(uint32_t fs_id, Ino ino, const std::vector<uint32_t>& chunk_indexes);

 private:
  WorkerSetUPtr worker_set_;

  ChunkCache& chunk_cache_;
  OperationProcessorSPtr operation_processor_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FILESYSTEM_WARMUP_H_
