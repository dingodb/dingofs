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

#include "mds/filesystem/filesystem.h"

namespace dingofs {
namespace mds {

static const std::string kWarmupWorkerSetName = "warmup_worker_set";

DEFINE_uint32(mds_warmup_worker_num, 4096, "number of warmup workers");
DEFINE_uint32(mds_warmup_worker_max_pending_num, 259072, "warmup worker max pending num");
DEFINE_bool(mds_warmup_worker_use_pthread, true, "warmup worker use pthread");

void WarmupChunkTask::Run() {
  const uint32_t fs_id = param_.fs->FsId();
  const Ino ino = param_.ino;

  auto status = WarmupChunk();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[warmup.{}.{}] warmup chunk fail, status({}).", fs_id, ino, status.error_str());
  }
}

Status WarmupChunkTask::WarmupChunk() {
  const uint32_t fs_id = param_.fs->FsId();
  const Ino ino = param_.ino;
  const auto& chunk_indexes = param_.chunk_indexes;

  auto& chunk_cache = param_.fs->GetChunkCache();
  auto operation_processor = warmup_pocessor_.GetOperationProcessor();

  if (warmup_pocessor_.IsStopped()) return Status::OK();
  if (chunk_cache.IsExist(ino)) return Status::OK();

  class Trace trace;
  GetChunkOperation operation(trace, fs_id, ino, chunk_indexes);
  auto status = operation_processor->RunAlone(&operation);
  if (!status.ok()) return status;

  LOG_DEBUG << fmt::format("[warmup.{}.{}] warmup chunk success.", fs_id, ino);

  auto& result = operation.GetResult();

  for (const auto& chunk : result.chunks) {
    chunk_cache.PutIf(ino, chunk);
  }

  return Status::OK();
}

bool WarmupProcessor::Init() {
  worker_set_ = ExecqWorkerSet::NewUnique(kWarmupWorkerSetName, FLAGS_mds_warmup_worker_num,
                                          FLAGS_mds_warmup_worker_max_pending_num, FLAGS_mds_warmup_worker_use_pthread);

  if (worker_set_ == nullptr) {
    LOG(ERROR) << "create warmup worker set fail.";
    return false;
  }

  if (!worker_set_->Init()) {
    LOG(ERROR) << "init warmup worker set fail.";
    return false;
  }

  return true;
}

void WarmupProcessor::Stop() {
  is_stopped_.store(true);

  if (worker_set_ != nullptr) {
    worker_set_->Destroy();
  }
}

void WarmupProcessor::Execute(const WarmupChunkTask::Param& param) {
  const uint32_t fs_id = param.fs->FsId();
  const Ino ino = param.ino;

  auto task = WarmupChunkTask::New(param, *this);

  if (!worker_set_->ExecuteHash(ino, task)) {
    LOG(ERROR) << fmt::format("[warmup.{}.{}] execute warmup chunk task fail.", fs_id, ino);
  }
}

}  // namespace mds
}  // namespace dingofs