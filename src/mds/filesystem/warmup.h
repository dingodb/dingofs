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

#include <atomic>
#include <cstdint>
#include <vector>

#include "mds/common/runnable.h"
#include "mds/common/status.h"
#include "mds/filesystem/store_operation.h"

namespace dingofs {
namespace mds {

class FileSystem;
using FileSystemSPtr = std::shared_ptr<FileSystem>;

class WarmupProcessor;
using WarmupProcessorSPtr = std::shared_ptr<WarmupProcessor>;

class WarmupChunkTask : public TaskRunnable {
 public:
  struct Param {
    FileSystemSPtr fs;
    std::vector<Ino> inoes;
  };

  WarmupChunkTask(const Param& param, WarmupProcessor& warmup_pocessor)
      : param_(param), warmup_pocessor_(warmup_pocessor) {}
  ~WarmupChunkTask() override = default;

  static TaskRunnablePtr New(const Param& param, WarmupProcessor& warmup_pocessor) {
    return std::make_shared<WarmupChunkTask>(param, warmup_pocessor);
  }

  std::string Type() override { return "WARMUP_CHUNK"; }

  void Run() override;

 private:
  Status WarmupChunk();
  Status WarmupBatchChunk(const std::vector<Ino>& inoes);

  Param param_;

  WarmupProcessor& warmup_pocessor_;
};

class WarmupProcessor {
 public:
  WarmupProcessor(OperationProcessorSPtr operation_processor) : operation_processor_(operation_processor) {}
  virtual ~WarmupProcessor() = default;

  WarmupProcessor(const WarmupProcessor&) = delete;
  WarmupProcessor& operator=(const WarmupProcessor&) = delete;

  static WarmupProcessorSPtr New(OperationProcessorSPtr operation_processor) {
    return std::make_shared<WarmupProcessor>(operation_processor);
  }

  bool Init();
  void Stop();

  void Execute(const WarmupChunkTask::Param& param);

 private:
  friend class WarmupChunkTask;

  OperationProcessorSPtr GetOperationProcessor() { return operation_processor_; }

  bool IsStopped() { return is_stopped_.load(std::memory_order_relaxed); }

  std::atomic<bool> is_stopped_{false};

  WorkerSetUPtr worker_set_;

  OperationProcessorSPtr operation_processor_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FILESYSTEM_WARMUP_H_
