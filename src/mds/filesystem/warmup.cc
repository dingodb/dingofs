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

#include "mds/filesystem/warmup.h"

namespace dingofs {
namespace mds {

static const std::string kWarmupWorkerSetName = "warmup_worker_set";

DEFINE_uint32(mds_warmup_worker_num, 4096, "number of warmup workers");
DEFINE_uint32(mds_warmup_worker_max_pending_num, 259072, "warmup worker max pending num");
DEFINE_bool(mds_warmup_worker_use_pthread, true, "warmup worker use pthread");

bool WarmupChunkTask::IsCached() { return chunk_cache_.IsExist(ino_); }

void WarmupChunkTask::Run() {
  auto status = WarmupChunk();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[warmup.{}.{}] warmup chunk fail, status({}).", fs_id_, ino_, status.error_str());
  }
}

Status WarmupChunkTask::WarmupChunk() {
  if (IsCached()) return Status::OK();

  class Trace trace;
  GetChunkOperation operation(trace, fs_id_, ino_, chunk_indexes_);
  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) return status;

  auto& result = operation.GetResult();

  for (const auto& chunk : result.chunks) {
    chunk_cache_.PutIf(ino_, chunk);
  }

  return Status::OK();
}

WarmupProcessor::WarmupProcessor(ChunkCache& chunk_cache, OperationProcessorSPtr operation_processor)
    : chunk_cache_(chunk_cache), operation_processor_(operation_processor) {}

bool WarmupProcessor::Init() {
  worker_set_ = ExecqWorkerSet::NewUnique(kWarmupWorkerSetName, FLAGS_mds_warmup_worker_num,
                                          FLAGS_mds_warmup_worker_max_pending_num, FLAGS_mds_warmup_worker_use_pthread);

  if (worker_set_ == nullptr) {
    LOG(ERROR) << "create warmup worker set fail.";
    return false;
  }

  return true;
}

void WarmupProcessor::Execute(uint32_t fs_id, Ino ino, const std::vector<uint32_t>& chunk_indexes) {
  auto task = std::make_shared<WarmupChunkTask>(fs_id, ino, chunk_indexes, chunk_cache_, operation_processor_);

  worker_set_->ExecuteHash(ino, task);
}

}  // namespace mds
}  // namespace dingofs