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

#include "client/common/const.h"
#include "client/vfs/components/prefetch_manager.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_fh.h"
#include "client/vfs/vfs_meta.h"
#include "common/options/client.h"
#include "common/status.h"
#include "fmt/format.h"
#include "glog/logging.h"
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

  return Status::OK();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs