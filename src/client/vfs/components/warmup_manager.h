#ifndef DINGOFS_CLIENT_VFS_WARMUP_WARMUP_MANAGER_H_
#define DINGOFS_CLIENT_VFS_WARMUP_WARMUP_MANAGER_H_

#include <bthread/countdown_event.h>
#include <utils/string.h>
#include <utils/string_util.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "butil/time.h"
#include "client/vfs/components/prefetch_manager.h"
#include "client/vfs/vfs.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "fmt/format.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace client {
namespace vfs {

using BCountDown = bthread::CountdownEvent;

class WarmupManagerImpl;

enum WarmupTriggerType {
  kWarmupTriggerTypeUnknown = 0,
  kWarmupTriggerTypeIntime = 1,
  kWarmupTriggerTypePassive = 2,
};

enum WarmupStatus {
  kWarmupStatusPending = 0,
  kWarmupStatusPrefetching = 1,
  kWarmupStatusDone = 2
};

class WarmupHelper {
 public:
  // Convert WarmupType to string
  static const std::string& GetWarmupTypeString(WarmupTriggerType type) {
    static const std::string kWarmupTriggerTypeIntimeStr = "intime";
    static const std::string kWarmupTriggerTypePassiveStr = "passive";
    static const std::string kWarmupTriggerTypeUnknownStr = "unknown";

    switch (type) {
      case kWarmupTriggerTypeIntime:
        return kWarmupTriggerTypeIntimeStr;
      case kWarmupTriggerTypePassive:
        return kWarmupTriggerTypePassiveStr;
      default:
        return kWarmupTriggerTypeUnknownStr;
    }
  }

  static uint64_t GetTimeSecs() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }
};

struct WarmupInfo {
  WarmupInfo() = default;
  WarmupInfo(const WarmupInfo& info) = default;
  WarmupInfo(Ino ino)
      : ino_(ino), type_(WarmupTriggerType::kWarmupTriggerTypeIntime) {}
  WarmupInfo(Ino ino, const std::string& attr)
      : ino_(ino),
        type_(WarmupTriggerType::kWarmupTriggerTypePassive),
        attr_value_(attr) {}
  Ino ino_;
  WarmupTriggerType type_;
  std::string attr_value_;

  std::string ToString() {
    return fmt::format("warmup info inode:{} type{}", ino_,
                       WarmupHelper::GetWarmupTypeString(type_));
  }
};

class WarmupTask {
 public:
  WarmupTask() = default;

  WarmupTask(Ino ino, WarmupInfo& info)
      : ino_(ino), info_(info), trigger_time_(0) {
    timer_.start();
  }

  ~WarmupTask() = default;

  std::string ToStringWithoutRes() {
    return fmt::format("warmup task ino:{} task:{} trigger_time:{}", ino_,
                       info_.ToString(), trigger_time_);
  }

  std::string ToStringWithRes() {
    return fmt::format(
        "warmup task ino:{} task:{} \n \
          finished:{} errors:{} total_len:{} cost:{}us",
        ino_, info_.ToString(), finished_.load(), errors_.load(),
        total_len_.load(), timer_.u_elapsed());
  }

  std::string ToQueryInfo() {
    return fmt::format("{}/{}/{}", finished_.load(), total_files.load(),
                       errors_.load());
  }

 private:
  Ino ino_;
  WarmupInfo info_;
  std::vector<Ino> file_inodes_;
  std::atomic<size_t> total_len_{0};
  std::atomic<size_t> total_files{0};
  std::atomic<size_t> finished_{0};
  std::atomic<size_t> errors_{0};
  std::atomic<bool> completed_{false};
  butil::Timer timer_;
  uint64_t trigger_time_;
  std::vector<Ino>::iterator it_;
  BCountDown count_down;

  friend class WarmupManagerImpl;
};

class WarmupManager {
 public:
  virtual ~WarmupManager() = default;

  // Start the warmup manager
  virtual Status Start() = 0;

  // Stop the warmup manager
  virtual void Stop() = 0;

  virtual void SetVFS(VFS* vfs) = 0;

  virtual void AsyncWarmupProcess(WarmupInfo& warmInfo) = 0;

  virtual Status GetWarmupStatus(Ino key, std::string& warmmupStatus) = 0;
};

class WarmupManagerImpl : public WarmupManager {
 public:
  WarmupManagerImpl(VFSHub* vfs_hub) : vfs_hub_(vfs_hub), started_(false) {}

  ~WarmupManagerImpl() override = default;

  Status Start() override;

  void Stop() override;

  void SetVFS(VFS* vfs) override { vfs_ = vfs; }

  void AsyncWarmupProcess(WarmupInfo& warmInfo) override;

  Status GetWarmupStatus(Ino key, std::string& warmmupStatus) override;

 private:
  void WarmupProcess(WarmupInfo& warmInfo);
  void ProcessPassiveWarmup(WarmupInfo& warmInfo);
  void ProcessIntimeWarmup(WarmupInfo& warmInfo);
  Status ProcessFileWarmup(const WarmupInfo& warmInfo);
  Status WalkFile(WarmupTask& task, Ino ino);
  Status WarmUpFiles(WarmupTask& task);
  Status WarmUpFile(Ino ino, AsyncPrefetchCb cb);

  std::unique_ptr<Executor> executor_;
  std::atomic<bool> started_;
  std::unordered_map<Ino, std::unique_ptr<WarmupTask>> inode_2_task_;
  std::mutex taskMutex_;
  std::mutex task_mutex_;
  VFS* vfs_;
  VFSHub* vfs_hub_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_WARMUP_WARMUP_MANAGER_H_