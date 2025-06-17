/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"){}
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
 * Project: dingo
 * Created Date: Tue Mar 29 2022
 * Author: lixiaocui
 */

#ifndef DINGOFS_SRC_CLIENT_LEASE_LEASE_EXCUTOR_H_
#define DINGOFS_SRC_CLIENT_LEASE_LEASE_EXCUTOR_H_

#include <brpc/periodic_task.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <memory>
#include <string>

#include "options/client/vfs_legacy/vfs_legacy_option.h"
#include "stub/rpcclient/mds_client.h"
#include "stub/rpcclient/metacache.h"

namespace dingofs {
namespace client {
class LeaseExecutorBase {
 public:
  virtual ~LeaseExecutorBase() = default;
  virtual bool RefreshLease() { return true; }
};

// RefreshSessin定期任务
// 利用brpc::PeriodicTaskManager进行管理
// 定时器触发时调用OnTriggeringTask，根据返回值决定是否继续定时触发
// 如果不再继续触发，调用OnDestroyingTask进行清理操作
class RefreshSessionTask : public brpc::PeriodicTask {
 public:
  using Task = std::function<bool(void)>;

  RefreshSessionTask(LeaseExecutorBase* leaseExecutor, uint64_t intervalUs)
      : leaseExecutor_(leaseExecutor),
        refreshIntervalUs_(intervalUs),
        stopped_(false),
        stopMtx_(),
        terminated_(false),
        terminatedMtx_(),
        terminatedCv_() {}

  RefreshSessionTask(const RefreshSessionTask& other)
      : leaseExecutor_(other.leaseExecutor_),
        refreshIntervalUs_(other.refreshIntervalUs_),
        stopped_(false),
        stopMtx_(),
        terminated_(false),
        terminatedMtx_(),
        terminatedCv_() {}

  virtual ~RefreshSessionTask() = default;

  /**
   * @brief 定时器超时后执行当前函数
   * @param next_abstime 任务下次执行的绝对时间
   * @return true 继续定期执行当前任务
   *         false 停止执行当前任务
   */
  bool OnTriggeringTask(timespec* next_abstime) override {
    std::lock_guard<bthread::Mutex> lk(stopMtx_);
    if (stopped_) {
      return false;
    }

    *next_abstime = butil::microseconds_from_now(refreshIntervalUs_);
    return leaseExecutor_->RefreshLease();
  }

  /**
   * @brief 停止再次执行当前任务
   */
  void Stop() {
    std::lock_guard<bthread::Mutex> lk(stopMtx_);
    stopped_ = true;
  }

  /**
   * @brief 任务停止后调用
   */
  void OnDestroyingTask() override {
    std::unique_lock<bthread::Mutex> lk(terminatedMtx_);
    terminated_ = true;
    terminatedCv_.notify_one();
  }

  /**
   * @brief 等待任务退出
   */
  void WaitTaskExit() {
    std::unique_lock<bthread::Mutex> lk(terminatedMtx_);
    while (terminated_ != true) {
      terminatedCv_.wait(lk);
    }
  }

  /**
   * @brief 获取refresh session时间间隔(us)
   * @return refresh session任务时间间隔(us)
   */
  uint64_t RefreshIntervalUs() const { return refreshIntervalUs_; }

 private:
  LeaseExecutorBase* leaseExecutor_;
  uint64_t refreshIntervalUs_;

  bool stopped_;
  bthread::Mutex stopMtx_;

  bool terminated_;
  bthread::Mutex terminatedMtx_;
  bthread::ConditionVariable terminatedCv_;
};

class LeaseExecutor : public LeaseExecutorBase {
 public:
  LeaseExecutor(const LeaseOpt& opt,
                std::shared_ptr<stub::rpcclient::MetaCache> metaCache,
                std::shared_ptr<stub::rpcclient::MdsClient> mdsCli)
      : opt_(opt), metaCache_(metaCache), mdsCli_(mdsCli) {}

  ~LeaseExecutor() override;

  bool Start();

  void Stop();

  /**
   * refresh lease with mds and update resource
   */
  bool RefreshLease() override;

  void SetFsName(const std::string& fsName) { fsName_ = fsName; }

  void SetMountPoint(const pb::mds::Mountpoint& mp) { mountpoint_ = mp; }

 private:
  LeaseOpt opt_;
  std::shared_ptr<stub::rpcclient::MetaCache> metaCache_;
  std::shared_ptr<stub::rpcclient::MdsClient> mdsCli_;
  std::unique_ptr<RefreshSessionTask> task_;
  std::string fsName_;
  pb::mds::Mountpoint mountpoint_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_LEASE_LEASE_EXCUTOR_H_
