// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "warmup_manager.h"

#include <bthread/countdown_event.h>
#include <bthread/execution_queue.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <ctime>

#include "butil/memory/scope_guard.h"
#include "client/common/const.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/common/data_utils.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_fh.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "utils/concurrent/concurrent.h"
#include "utils/executor/executor.h"
#include "utils/executor/thread/executor_impl.h"
#include "utils/string_util.h"

namespace dingofs {
namespace client {
namespace vfs {

static const std::string kWarmupExecutorName = "vfs_warmup";

Status WarmupManager::Start(const uint32_t& threads) {
  CHECK_GT(threads, 0);
  CHECK_NOTNULL(metrics_);

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(
      &task_queue_id_, &queue_options, &WarmupManager::HandleWarmupTask, this);
  if (rc != 0) {
    LOG(ERROR) << "Start execution queue failed: rc = " << rc;
    return Status::Internal("start execution queue failed.");
  }

  warmup_executor_ =
      std::make_unique<ExecutorImpl>(kWarmupExecutorName, threads);

  auto ok = warmup_executor_->Start();
  if (!ok) {
    LOG(ERROR) << "Start warmup manager executor failed.";
    return Status::Internal("Start warmup manager executor failed.");
  }

  block_cache_ = vfs_hub_->GetBlockCache();
  CHECK_NOTNULL(block_cache_);

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << fmt::format("WarmupManager started with {} threads.", threads);

  return Status::OK();
}

Status WarmupManager::Stop() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  if (bthread::execution_queue_stop(task_queue_id_) != 0) {
    LOG(ERROR) << "Stop execution queue failed.";
    return Status::Internal("stop execution queue failed.");
  } else if (bthread::execution_queue_join(task_queue_id_) != 0) {
    LOG(ERROR) << "Join execution queue failed.";
    return Status::Internal("join execution queue failed.");
  }

  auto ok = warmup_executor_->Stop();
  if (!ok) {
    LOG(ERROR) << "Stop warmup executor failed.";
    return Status::Internal("Stop warmup executor failed.");
  }

  LOG(INFO) << "WarmupManager stopped.";
  return Status::OK();
}

void WarmupManager::SubmitTask(const WarmupTaskContext& context) {
  CHECK_EQ(0, bthread::execution_queue_execute(task_queue_id_, context));
}

std::string WarmupManager::GetWarmupTaskStatus(const Ino& task_key) {
  utils::ReadLockGuard lck(task_rwlock_);
  std::string result;
  auto iter = warmup_tasks_.find(task_key);
  if (iter != warmup_tasks_.end()) {
    // task already running
    const auto& task = iter->second;
    result = fmt::format("{}/{}/{}", task->GetFinished(), task->GetTotal(),
                         task->GetErrors());
  } else {
    // task is finished and removed from warmup task queue
    result = "0/0/0";
  }

  return result;
};

int WarmupManager::HandleWarmupTask(
    void* meta, bthread::TaskIterator<WarmupTaskContext>& iter) {
  if (iter.is_queue_stopped()) {
    LOG(INFO) << "WarmupManager Execution queue stopped.";
    return 0;
  }

  auto* self = static_cast<WarmupManager*>(meta);
  for (; iter; iter++) {
    WarmupTaskContext& context = *iter;
    if (self->NewWarmupTask(context)) {
      // Run in thread pool
      self->AsyncWarmupTask(context);
    }
  }

  return 0;
}

void WarmupManager::AsyncWarmupTask(const WarmupTaskContext& context) {
  auto* self = this;
  auto* task = self->GetWarmupTask(context.task_key);
  CHECK_NOTNULL(task);
  warmup_executor_->Execute([self, task]() { self->DoWarmupTask(task); });
}

void WarmupManager::DoWarmupTask(WarmupTask* task) {
  BRPC_SCOPE_EXIT { RemoveWarmupTask(task->GetKey()); };

  if (task->GetType() == WarmupType::kWarmupIntime) {
    ProcessIntimeWarmup(task);
  } else if (task->GetType() == WarmupType::kWarmupManual) {
    ProcessManualWarmup(task);
  } else {
    LOG(ERROR) << fmt::format("Warmup task[{}], has invalid warmup type.",
                              task->GetKey());
  }
}

void WarmupManager::ProcessIntimeWarmup(WarmupTask* task) {
  auto span = vfs_hub_->GetTracer()->StartSpan(kVFSDataMoudule, __func__);
  auto inode = task->GetKey();
  LOG(INFO) << "Intime warmup started for inode: " << task->GetKey();

  WarmupFile(inode, [inode, task](Status s) {
    LOG(INFO) << "Finish intime warmup file: " << inode
              << ", with status: " << s.ToString();
  });
}

void WarmupManager::ProcessManualWarmup(WarmupTask* task) {
  auto span = vfs_hub_->GetTracer()->StartSpan(kVFSDataMoudule, __func__);
  VLOG(3) << "Process manual warmup task, key: " << task->GetKey()
          << ", inodes: " << task->GetTaskInodes();
  std::vector<std::string> warmup_inodes;
  utils::SplitString(task->GetTaskInodes(), ",", &warmup_inodes);

  for (const auto& inode_str : warmup_inodes) {
    Ino ino;
    bool ok = utils::StringToUll(inode_str, &ino);
    if (!ok) {
      LOG(ERROR) << "Process manual warmup task failed, invalid inode: "
                 << inode_str;
      continue;
    }

    CHECK_NOTNULL(task);
    WalkFile(task, ino);
  }

  if (task->GetFileCount() == 0) {
    LOG(INFO) << fmt::format(
        "Process manual warmup task: {} finished with no file.",
        task->GetKey());
    return;
  }
  LOG(INFO) << fmt::format(
      "Start process manual warmup task: {}, scaned {} files.", task->GetKey(),
      task->GetFileCount());

  WarmupFiles(task);

  LOG(INFO) << fmt::format(
      "Finish process manual warmup task: {}, total: {}, finished: {}, "
      "errors: {}.",
      task->GetKey(), task->GetTotal(), task->GetFinished(), task->GetErrors());
}

void WarmupManager::WarmupFiles(WarmupTask* task) {
  for (const auto& inode : task->GetFileInodes()) {
    LOG(INFO) << "Start warmup file: " << inode;

    WarmupFile(inode, [inode, task](Status s) {
      LOG(INFO) << "Finish warmup file: " << inode
                << ", with status: " << s.ToString();
      if (s.ok()) {
        task->IncFinished();
      } else {
        task->IncErrors();
      }
    });
  }
}

Status WarmupManager::WalkFile(WarmupTask* task, Ino ino) {
  auto span = vfs_hub_->GetTracer()->StartSpan(kVFSDataMoudule, __func__);

  Attr attr;
  Status s = vfs_hub_->GetMetaSystem()->GetAttr(span->GetContext(), ino, &attr);
  if (!s.ok()) {
    LOG(ERROR) << "Failed to get attr for ino: " << ino
               << ", status: " << s.ToString();
    return s;
  }

  std::vector<Ino> parentDir;

  if (attr.type == FileType::kFile) {
    task->AddFileInode(ino);
    return Status::OK();
  } else if (attr.type == FileType::kDirectory) {
    parentDir.push_back(ino);
  } else {
    LOG(ERROR) << "Warmup task file ino: " << ino
               << " is symlink, skip warmup.";
    return Status::NotSupport("Unsupported file type");
  }

  Status openStatus;

  while (parentDir.size()) {
    std::vector<Ino> childDir;
    auto dirIt = parentDir.begin();
    while (dirIt != parentDir.end()) {
      uint64_t fh = vfs::FhGenerator::GenFh();
      openStatus =
          vfs_hub_->GetMetaSystem()->OpenDir(span->GetContext(), *dirIt, fh);
      if (!openStatus.ok()) {
        LOG(ERROR) << "Failed to open dir: " << *dirIt
                   << ", status: " << openStatus.ToString();
        ++dirIt;
        continue;
      }

      vfs_hub_->GetMetaSystem()->ReadDir(
          span->GetContext(), *dirIt, fh, 0, true,
          [task, &childDir, this](const DirEntry& entry, uint64_t offset) {
            (void)offset;
            Ino inoTmp = entry.ino;
            Attr attr = entry.attr;
            if (entry.attr.type == FileType::kFile) {
              task->AddFileInode(entry.ino);
            } else if (entry.attr.type == FileType::kDirectory) {
              childDir.push_back(entry.ino);
            } else {
              LOG(WARNING) << "name:" << entry.name << " ino:" << entry.ino
                           << " attr.type:" << entry.attr.type
                           << " not support.";
            }
            return true;  // Continue reading
          });
      vfs_hub_->GetMetaSystem()->ReleaseDir(span->GetContext(), *dirIt, fh);

      dirIt++;
    }
    parentDir = std::move(childDir);
  }

  return Status::OK();
}

void WarmupManager::WarmupFile(Ino ino, AsyncWarmupCb cb) {
  auto span = vfs_hub_->GetTracer()->StartSpan(kVFSDataMoudule, __func__);
  IncFileMetric(1);
  BRPC_SCOPE_EXIT { DecFileMetric(1); };

  Attr attr;
  auto status =
      vfs_hub_->GetMetaSystem()->GetAttr(span->GetContext(), ino, &attr);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Get attr failed, status: {}", status.ToString());
    cb(status);
    return;
  }

  auto blocks = FileRange2BlockKey(span->GetContext(), ino, 0, attr.length);
  // remove duplicate blocks
  auto unique_blocks = RemoveDuplicateBlocks(blocks);
  IncBlockMetric(unique_blocks.size());

  VLOG(6) << fmt::format("Download file: {} started, size: {}, blocks: {}.",
                         ino, attr.length, unique_blocks.size());

  Status donwload_status = Status::OK();
  bthread::CountdownEvent countdown(unique_blocks.size());
  for (auto& block : unique_blocks) {
    cache::BlockKey key = block.key;
    uint64_t len = block.len;

    VLOG(6) << fmt::format("Download block {}, len {}", key.Filename(), len);
    auto* self = this;
    block_cache_->AsyncPrefetch(
        cache::NewContext(), key, len,
        [self, key, len, &countdown, &donwload_status](Status status) {
          VLOG(6) << fmt::format("Download block {} finished, status: {}.",
                                 key.Filename(), status.ToString());
          BRPC_SCOPE_EXIT { self->DecBlockMetric(1); };

          if (!status.ok() && !status.IsExist()) {
            donwload_status = status;
          } else {
          }

          countdown.signal();
        });
  }

  countdown.wait();
  cb(donwload_status);
}

bool WarmupManager::NewWarmupTask(const WarmupTaskContext& context) {
  utils::WriteLockGuard lck(task_rwlock_);
  auto [_, ok] = warmup_tasks_.emplace(context.task_key,
                                       std::make_unique<WarmupTask>(context));
  if (ok) {
    LOG(INFO) << "New warmup task, key: " << context.task_key;
    IncTaskMetric(1);
    return true;
  } else {
    LOG(INFO) << "Warmup task already exist, skip it, key: "
              << context.task_key;
    return false;
  }
}

WarmupTask* WarmupManager::GetWarmupTask(const Ino& task_key) {
  utils::ReadLockGuard lck(task_rwlock_);
  auto iter = warmup_tasks_.find(task_key);
  if (iter != warmup_tasks_.end()) {
    return iter->second.get();
  }
  return nullptr;
}

void WarmupManager::RemoveWarmupTask(const Ino& task_key) {
  LOG(INFO) << "Remove warmup task, key: " << task_key;
  utils::WriteLockGuard lck(task_rwlock_);
  warmup_tasks_.erase(task_key);
  DecTaskMetric(1);
}

void WarmupManager::ClearWarmupTask() {
  utils::WriteLockGuard lck(task_rwlock_);
  warmup_tasks_.clear();
  ResetMetrics();
}

std::vector<ChunkContext> WarmupManager::File2Chunk(Ino ino, uint64_t offset,
                                                    uint64_t len) const {
  std::vector<ChunkContext> chunk_contexts;

  uint64_t chunk_idx = offset / chunk_size_;
  uint64_t chunk_offset = offset % chunk_size_;
  uint64_t prefetch_size;
  prefetch_size = len;

  while (prefetch_size > 0) {
    uint64_t chunk_fetch_size =
        std::min(prefetch_size, chunk_size_ - chunk_offset);
    ChunkContext chunk_context(ino, chunk_idx, chunk_offset, chunk_fetch_size);

    VLOG(9) << "Warmup ino: " << ino << ", " << chunk_context.ToString();
    chunk_contexts.push_back(chunk_context);

    chunk_idx++;
    chunk_offset = 0;
    prefetch_size -= chunk_fetch_size;
  }

  return chunk_contexts;
}

std::vector<BlockContext> WarmupManager::Chunk2Block(ContextSPtr ctx,
                                                     ChunkContext& req) {
  std::vector<Slice> slices;
  std::vector<BlockReadReq> block_reqs;
  std::vector<BlockContext> block_contexts;

  Status status = vfs_hub_->GetMetaSystem()->ReadSlice(
      ctx, req.ino, req.chunk_idx, 0, &slices);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "Read slice failed, ino: {}, chunk: {}, status: {}.", req.ino,
        req.chunk_idx, status.ToString());

    return block_contexts;
  }

  FileRange range = {(req.chunk_idx * chunk_size_) + req.offset, req.len};
  std::vector<SliceReadReq> slice_reqs = ProcessReadRequest(slices, range);

  for (auto& slice_req : slice_reqs) {
    VLOG(9) << "Read slice_seq : " << slice_req.ToString();

    if (slice_req.slice.has_value() && !slice_req.slice.value().is_zero) {
      std::vector<BlockReadReq> reqs = ConvertSliceReadReqToBlockReadReqs(
          slice_req, fs_id_, req.ino, chunk_size_, block_size_);

      block_reqs.insert(block_reqs.end(), std::make_move_iterator(reqs.begin()),
                        std::make_move_iterator(reqs.end()));
    }
  }

  for (auto& block_req : block_reqs) {
    cache::BlockKey key(fs_id_, req.ino, block_req.block.slice_id,
                        block_req.block.index, block_req.block.version);
    if (block_cache_->IsCached(key)) {
      VLOG(9) << fmt::format("Skip warmup block key: {}, already in cache.",
                             key.Filename());
      continue;
    }

    block_contexts.emplace_back(key, block_req.block.block_len);
  }

  return block_contexts;
}

std::vector<BlockContext> WarmupManager::FileRange2BlockKey(ContextSPtr ctx,
                                                            Ino ino,
                                                            uint64_t offset,
                                                            uint64_t len) {
  std::vector<ChunkContext> chunk_contexts = File2Chunk(ino, offset, len);

  std::vector<BlockContext> block_contexts;
  for (auto& chunk_context : chunk_contexts) {
    std::vector<BlockContext> block_contexts_temp =
        Chunk2Block(ctx, chunk_context);

    block_contexts.insert(block_contexts.end(),
                          std::make_move_iterator(block_contexts_temp.begin()),
                          std::make_move_iterator(block_contexts_temp.end()));
  }
  return block_contexts;
}

std::vector<BlockContext> WarmupManager::RemoveDuplicateBlocks(
    const std::vector<BlockContext>& blocks) {
  std::unordered_set<std::string> seen_filenames;
  std::vector<BlockContext> result;

  seen_filenames.reserve(blocks.size());
  result.reserve(blocks.size());

  for (const auto& block : blocks) {
    auto [_, ok] = seen_filenames.insert(block.key.Filename());
    if (ok) {
      result.push_back(block);
    }
  }

  return result;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
