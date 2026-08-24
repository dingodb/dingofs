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

#include <vector>

#include "fmt/ranges.h"
#include "glog/logging.h"
#include "mds/filesystem/filesystem.h"

namespace dingofs {
namespace mds {

static const std::string kWarmupWorkerSetName = "warmup_worker_set";

DEFINE_uint32(mds_warmup_worker_num, 4096, "number of warmup workers");
DEFINE_uint32(mds_warmup_worker_max_pending_num, 259072, "warmup worker max pending num");
DEFINE_bool(mds_warmup_worker_use_pthread, true, "warmup worker use pthread");

DEFINE_uint32(mds_warmup_batch_size, 64, "warmup batch size");

void WarmupChunkTask::Run() {
  const uint32_t fs_id = param_.fs->FsId();

  auto status = WarmupChunk();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[warmup.{}] warmup chunk fail, ino({}) status({}).", fs_id, param_.inoes,
                              status.error_str());
  }
}

Status WarmupChunkTask::WarmupChunk() {
  if (param_.inoes.size() <= FLAGS_mds_warmup_batch_size) {
    return WarmupBatchChunk(param_.inoes);
  }

  std::vector<Ino> inoes;
  inoes.reserve(FLAGS_mds_warmup_batch_size);
  for (size_t i = 0; i < param_.inoes.size(); i += FLAGS_mds_warmup_batch_size) {
    size_t end = std::min(i + FLAGS_mds_warmup_batch_size, param_.inoes.size());
    inoes.insert(inoes.end(), param_.inoes.begin() + i, param_.inoes.begin() + end);

    Status status = WarmupBatchChunk(inoes);
    if (!status.ok()) return status;

    inoes.clear();
  }

  return Status::OK();
}

Status WarmupChunkTask::WarmupBatchChunk(const std::vector<Ino>& inoes) {
  const uint32_t fs_id = param_.fs->FsId();

  auto& chunk_cache = param_.fs->GetChunkCache();
  auto operation_processor = warmup_pocessor_.GetOperationProcessor();

  if (warmup_pocessor_.IsStopped()) return Status::OK();

  std::vector<Ino> miss_inoes;
  miss_inoes.reserve(inoes.size());
  for (const auto& ino : inoes) {
    if (!chunk_cache.IsExist(ino)) miss_inoes.push_back(ino);
  }
  if (miss_inoes.empty()) return Status::OK();

  class Trace trace;
  BatchGetFirstChunkOperation operation(trace, fs_id, miss_inoes);
  auto status = operation_processor->RunAlone(&operation);
  if (!status.ok()) return status;

  LOG_DEBUG << fmt::format("[warmup.{}] warmup chunk success, ino({}).", fs_id, miss_inoes);

  auto& result = operation.GetResult();

  CHECK(result.inoes.size() == result.chunks.size()) << "inoes size mismatch with chunks size";

  for (size_t i = 0; i < result.inoes.size(); ++i) {
    auto& ino = result.inoes[i];
    auto& chunk = result.chunks[i];

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
    worker_set_->Stop();
  }
}

void WarmupProcessor::Execute(const WarmupChunkTask::Param& param) {
  const uint32_t fs_id = param.fs->FsId();

  if (param.inoes.empty()) return;

  auto task = WarmupChunkTask::New(param, *this);

  if (!worker_set_->ExecuteHash(param.inoes.front(), task)) {
    LOG(ERROR) << fmt::format("[warmup.{}] execute warmup chunk task fail, ino({}).", fs_id, param.inoes);
  }
}

}  // namespace mds
}  // namespace dingofs