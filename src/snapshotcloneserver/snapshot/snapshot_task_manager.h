/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_TASK_MANAGER_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_TASK_MANAGER_H_

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <thread>  // NOLINT

#include "src/common/concurrent/rw_lock.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/snapshotcloneserver/common/snapshotclone_metric.h"
#include "src/snapshotcloneserver/common/thread_pool.h"
#include "src/snapshotcloneserver/snapshot/snapshot_core.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task.h"

using ::curve::common::Mutex;
using ::curve::common::ReadLockGuard;
using ::curve::common::RWLock;
using ::curve::common::WriteLockGuard;

namespace curve {
namespace snapshotcloneserver {

/**
 * @brief 快照任务管理器类
 */
class SnapshotTaskManager {
 public:
  /**
   * @brief 默认构造函数
   */
  SnapshotTaskManager(std::shared_ptr<SnapshotCore> core,
                      std::shared_ptr<SnapshotMetric> snapshotMetric)
      : isStop_(true),
        core_(core),
        snapshotMetric_(snapshotMetric),
        snapshotTaskManagerScanIntervalMs_(0) {}

  /**
   * @brief 析构函数
   */
  ~SnapshotTaskManager() { Stop(); }

  int Init(std::shared_ptr<ThreadPool> pool,
           const SnapshotCloneServerOptions& option) {
    snapshotTaskManagerScanIntervalMs_ =
        option.snapshotTaskManagerScanIntervalMs;
    threadpool_ = pool;
    return kErrCodeSuccess;
  }

  /**
   * @brief 启动
   *
   * @return 错误码
   */
  int Start();

  /**
   * @brief 停止服务
   *
   */
  void Stop();

  /**
   * @brief 添加任务
   *
   * @param task 快照任务
   *
   * @return 错误码
   */
  int PushTask(std::shared_ptr<SnapshotTask> task);

  /**
   * @brief 获取任务
   *
   * @param taskId 任务id
   *
   * @return 快照任务指针
   */
  std::shared_ptr<SnapshotTask> GetTask(const TaskIdType& taskId) const;

  /**
   * @brief 取消任务
   *
   * @param taskId 任务id
   *
   * @return 错误码
   */
  int CancelTask(const TaskIdType& taskId);

 private:
  /**
   * @brief 后台线程执行函数
   *
   * 定期执行扫描等待队列函数与扫描工作队列函数。
   */
  void BackEndThreadFunc();
  /**
   * @brief 扫描等待任务队列函数
   *
   * 扫描等待队列，判断工作队列中当前文件
   * 是否有正在执行的快照，若没有则放入工作队列
   *
   */
  void ScanWaitingTask();
  /**
   * @brief 扫描工作队列函数
   *
   * 扫描工作队列，判断工作队列中当前
   * 快照任务是否已完成，若完成则移出工作队列
   *
   */
  void ScanWorkingTask();

 private:
  // 后端线程
  std::thread backEndThread;

  // id->快照任务表
  std::map<TaskIdType, std::shared_ptr<SnapshotTask> > taskMap_;
  mutable RWLock taskMapLock_;

  // 快照等待队列
  std::list<std::shared_ptr<SnapshotTask> > waitingTasks_;
  mutable Mutex waitingTasksLock_;

  // 快照工作队列,实际是个map，其中key是文件名，以便于查询
  std::map<std::string, std::shared_ptr<SnapshotTask> > workingTasks_;
  mutable Mutex workingTasksLock_;

  std::shared_ptr<ThreadPool> threadpool_;

  // 当前任务管理是否停止，用于支持start，stop功能
  std::atomic_bool isStop_;

  // snapshot core
  std::shared_ptr<SnapshotCore> core_;

  // metric
  std::shared_ptr<SnapshotMetric> snapshotMetric_;

  // 快照后台线程扫描等待队列和工作队列的扫描周期(单位：ms)
  int snapshotTaskManagerScanIntervalMs_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_TASK_MANAGER_H_
