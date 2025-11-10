#include "client/vfs/components/warmup_manager.h"

#include <fcntl.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

<<<<<<< HEAD
=======
#include <cstdint>
#include <ctime>

#include "butil/memory/scope_guard.h"
>>>>>>> c9dac3d2a ([chore][client] Add chunk version for readslice.)
#include "client/common/const.h"
#include "client/vfs/components/prefetch_manager.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_fh.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "options/client/option.h"
#include "utils/executor/thread/executor_impl.h"
#include "utils/string_util.h"

namespace dingofs {
namespace client {
namespace vfs {

Status WarmupManagerImpl::Start() {
  executor_ = std::make_unique<ExecutorImpl>(
      fLI::FLAGS_client_vfs_warmup_executor_thread);

  LOG(INFO) << fmt::format(
      "warmupmanager start with params:\n executor num:{} \n \
          intime enbale:{}\n mtime interval:{} \n trigger interval {}",
      FLAGS_client_vfs_warmup_executor_thread,
      FLAGS_client_vfs_intime_warmup_enable,
      FLAGS_client_vfs_warmup_mtime_restart_interval_secs,
      FLAGS_client_vfs_warmup_trigger_restart_interval_secs);

  if (!executor_->Start()) {
    LOG(ERROR) << "WarmupManagerImpl executor start failed";
    return Status::Internal("WarmupManagerImpl executor start failed");
  }

  return Status::OK();
}

void WarmupManagerImpl::Stop() {
  if (executor_) {
    executor_->Stop();
    executor_.reset();
  }
}
Status WarmupManagerImpl::GetWarmupStatus(Ino key, std::string& warmmupStatus) {
  std::lock_guard<std::mutex> lg(task_mutex_);

  auto it = inode_2_task_.find(key);
  if (it == inode_2_task_.end()) {
    LOG(ERROR) << fmt::format("cat't find inode:{} warmup info", key);
    warmmupStatus = "0/0/0";
    return Status::Internal(
        fmt::format("can't find inode:{} warmup info", key));
  }

  auto& task = it->second;

  warmmupStatus = task->ToQueryInfo();
  return Status::OK();
}

void WarmupManagerImpl::AsyncWarmupProcess(WarmupInfo& warmInfo) {
  executor_->Execute([this, warmInfo]() {
    WarmupInfo infoTmp = warmInfo;
    this->WarmupProcess(infoTmp);
  });
}

void WarmupManagerImpl::WarmupProcess(WarmupInfo& warmInfo) {
  if (warmInfo.type_ == WarmupTriggerType::kWarmupTriggerTypeIntime) {
    ProcessIntimeWarmup(warmInfo);
  } else if (warmInfo.type_ == WarmupTriggerType::kWarmupTriggerTypePassive) {
    ProcessPassiveWarmup(warmInfo);
  } else {
    LOG(ERROR) << fmt::format(
        "warmup ino: {} has invalid WarmupType {}", warmInfo.ino_,
        WarmupHelper::GetWarmupTypeString(warmInfo.type_));
  }
}

void WarmupManagerImpl::ProcessIntimeWarmup(WarmupInfo& warmInfo) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSDataMoudule, __func__);
  Attr attr;

  Status s = vfs_->GetAttr(span->GetContext(), warmInfo.ino_, &attr);
  if (!s.ok()) {
    LOG(ERROR) << "Failed to get attr for ino: " << warmInfo.ino_
               << ", status: " << s.ToString();
    return;
  }

  LOG(INFO) << "intime Warmup start for inode:" + std::to_string(warmInfo.ino_);
  WarmUpFile(warmInfo.ino_, [warmInfo](Status status, uint64_t len) {
    LOG(INFO) << fmt::format(
        "intime prefecth inode:{} finished with status:{} prefetch len{}",
        warmInfo.ino_, status.ToString(), len);
  });
}

void WarmupManagerImpl::ProcessPassiveWarmup(WarmupInfo& warmInfo) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSDataMoudule, __func__);
  std::lock_guard<std::mutex> lg(task_mutex_);

  auto it = inode_2_task_.find(warmInfo.ino_);
  if (it != inode_2_task_.end()) {
    if (!it->second->completed_.load()) {
      LOG(WARNING) << fmt::format("current warmup key {} is still in progress");
      return;
    }
    inode_2_task_.erase(it);
  }

  auto res = inode_2_task_.emplace(
      warmInfo.ino_, std::make_unique<WarmupTask>(warmInfo.ino_, warmInfo));
  auto& task = res.first->second;
  task->trigger_time_ = WarmupHelper::GetTimeSecs();
  std::vector<std::string> warmup_list;
  utils::AddSplitStringToResult(res.first->second->info_.attr_value_, ",",
                                &warmup_list);

  VLOG(6) << fmt::format("passive warmup {} triggererd",
                         task->ToStringWithoutRes());

  for (const auto& inode : warmup_list) {
    Ino ino;
    bool ok = utils::StringToUll(inode, &ino);
    if (!ok) {
      LOG(ERROR) << fmt::format("passive warmup inode:{} find invalid param:{}",
                                task->info_.ToString(), inode);
      continue;
    }

    WalkFile(*task, ino);
  }

  if (task->file_inodes_.empty()) {
    LOG(INFO) << fmt::format("passive warmup inode:{} finished with no file",
                             task->ino_);
    return;
  }

  LOG(INFO) << fmt::format("passive warmup inode:{} scaned {} files",
                           task->ino_, task->file_inodes_.size());
  task->total_files.fetch_add(task->file_inodes_.size());

  task->it_ = task->file_inodes_.begin();
  WarmUpFiles(*task);

  task->file_inodes_.clear();
  task->completed_.store(true);
}

Status WarmupManagerImpl::WalkFile(WarmupTask& task, Ino ino) {
  auto span = vfs_->GetTracer()->StartSpan(kVFSDataMoudule, __func__);

  Attr attr;
  Status s = vfs_->GetAttr(span->GetContext(), ino, &attr);

  if (!s.ok()) {
    LOG(ERROR) << "Failed to get attr for ino: " << ino
               << ", status: " << s.ToString();
    return s;
  }

  std::vector<Ino> parentDir;

  if (attr.type == FileType::kFile) {
    task.file_inodes_.push_back(ino);
    return Status::OK();
  } else if (attr.type == FileType::kDirectory) {
    parentDir.push_back(ino);
  } else {
    LOG(ERROR) << "ino: " << ino << "is symlink, skip warmup";
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
          [&task, &childDir, this](const DirEntry& entry, uint64_t offset) {
            (void)offset;
            Ino inoTmp = entry.ino;
            Attr attr = entry.attr;
            if (entry.attr.type == FileType::kFile) {
              task.file_inodes_.push_back(entry.ino);
            } else if (entry.attr.type == FileType::kDirectory) {
              childDir.push_back(entry.ino);
            } else {
              LOG(ERROR) << "name:" << entry.name << " ino:" << entry.ino
                         << " attr.type:" << entry.attr.type << " not support";
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

Status WarmupManagerImpl::WarmUpFiles(WarmupTask& task) {
  task.count_down.reset(task.file_inodes_.size());

  for (; task.it_ != task.file_inodes_.end(); task.it_++) {
    Ino ino = *task.it_;

    VLOG(6) << fmt::format("warmup submit key:{} ino:{} prefetch", task.ino_,
                           ino);
    WarmUpFile(*task.it_, [ino, &task](Status status, uint64_t len) {
      VLOG(6) << fmt::format("warmup file inode:{} finished with status:{}",
                             ino, status.ToString());
      if (!status.ok()) {
        task.errors_.fetch_add(1);
        LOG(ERROR) << "Ino file";
      } else {
        task.finished_.fetch_add(1);
        task.total_len_.fetch_add(len);
      }
      task.count_down.signal();
    });
  }

  task.count_down.wait();
  LOG(INFO) << fmt::format("warmup task key:{} finished {}", task.ino_,
                           task.ToStringWithRes());

  return Status::OK();
}

Status WarmupManagerImpl::WarmUpFile(Ino ino, AsyncPrefetchCb cb) {
  vfs_hub_->GetPrefetchManager()->AsyncPrefetch(ino, cb);

<<<<<<< HEAD
  return Status::OK();
=======
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

  uint64_t chunk_version = 0;
  Status status = vfs_hub_->GetMetaSystem()->ReadSlice(
      ctx, req.ino, req.chunk_idx, 0, &slices, chunk_version);
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
>>>>>>> c9dac3d2a ([chore][client] Add chunk version for readslice.)
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs