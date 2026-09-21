// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#include "mds/common/crontab.h"

#include <glog/logging.h>

#include <mutex>
#include <utility>

#include "bthread/bthread.h"
#include "bthread/unstable.h"
#include "butil/time.h"
#include "fmt/core.h"

namespace dingofs {
namespace mds {

Crontab::Crontab(uint32_t id, CrontabConfig cfg) : id_(id), cfg_(std::move(cfg)) {
  CHECK_EQ(bthread_mutex_init(&mu_, nullptr), 0);
  CHECK_EQ(bthread_cond_init(&drained_cv_, nullptr), 0);
}

Crontab::~Crontab() {
  CHECK_EQ(pending_ops_, 0) << cfg_.name;
  bthread_cond_destroy(&drained_cv_);
  bthread_mutex_destroy(&mu_);
}

void Crontab::Launch() {
  std::lock_guard<bthread_mutex_t> lock(mu_);
  if (stop_requested_) return;
  if (cfg_.immediately) {
    StartBthreadLocked();
    if (cfg_.async) ArmLocked();
  } else {
    ArmLocked();
  }
}

void Crontab::RequestStop() {
  std::lock_guard<bthread_mutex_t> lock(mu_);
  stop_requested_ = true;
  // Only successful cancellation returns the reservation to us.
  // Otherwise the timer callback still owns it.
  if (has_timer_ && bthread_timer_del(timer_id_) == 0) {
    has_timer_ = false;
    ReleasePendingLocked();
  }
}

void Crontab::Join() {
  std::unique_lock<bthread_mutex_t> lock(mu_);
  while (pending_ops_ > 0) {
    bthread_cond_wait(&drained_cv_, &mu_);
  }
}

void Crontab::DescribeByJson(Json::Value& value) const {
  std::lock_guard<bthread_mutex_t> lock(mu_);
  value["id"] = id_;
  value["name"] = cfg_.name;
  value["interval_ms"] = static_cast<int64_t>(cfg_.interval_ms);
  value["max_times"] = cfg_.max_times;
  value["immediately"] = cfg_.immediately;  // Configuration, not mutable run state.
  value["run_count"] = run_count_;          // Admitted calls, not completed calls.
  value["pause"] = stop_requested_;         // Legacy key; stopping is permanent, not a resumable pause.
}

// Requires mu_. Reserve before publishing the timer.
void Crontab::ArmLocked() {
  if (stop_requested_) return;
  if (cfg_.max_times != 0 && run_count_ >= cfg_.max_times) return;
  timespec deadline = butil::milliseconds_from_now(cfg_.interval_ms);
  ++pending_ops_;
  has_timer_ = true;
  int rc = bthread_timer_add(&timer_id_, deadline, &Crontab::OnTimer, this);
  if (rc != 0) {
    LOG(ERROR) << fmt::format("[crontab.arm][id({}).name({})] bthread_timer_add failed: {}", id_, cfg_.name, rc);
    has_timer_ = false;
    ReleasePendingLocked();
  }
}

void Crontab::OnTimer(void* arg) { static_cast<Crontab*>(arg)->OnTimerFired(); }

void* Crontab::OnBthreadRun(void* arg) {
  static_cast<Crontab*>(arg)->RunOnce();
  return nullptr;
}

// Requires mu_. Reserve independently of the timer that dispatches this work.
void Crontab::StartBthreadLocked() {
  ++pending_ops_;
  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  if (bthread_start_background(&tid, &attr, &Crontab::OnBthreadRun, this) != 0) {
    LOG(ERROR) << fmt::format("[crontab.run][id({}).name({})] bthread_start_background failed", id_, cfg_.name);
    ReleasePendingLocked();
  }
}

void Crontab::OnTimerFired() {
  std::unique_lock<bthread_mutex_t> lock(mu_);
  has_timer_ = false;
  if (stop_requested_ || (cfg_.max_times != 0 && run_count_ >= cfg_.max_times)) {
    ReleasePendingLocked();
    return;
  }
  if (cfg_.async) {
    StartBthreadLocked();
    // Keep the submission cadence independent of callback duration.
    ArmLocked();
    ReleasePendingLocked();
  } else {
    lock.unlock();
    RunOnce();  // The synchronous invocation takes over the timer reservation.
  }
}

void Crontab::RunOnce() {
  std::unique_lock<bthread_mutex_t> lock(mu_);
  // Check at invocation admission: queued bthreads must not exceed max_times.
  if (!stop_requested_ && (cfg_.max_times == 0 || run_count_ < cfg_.max_times)) {
    ++run_count_;
    lock.unlock();
    try {
      cfg_.callback();
    } catch (...) {
      LOG(ERROR) << fmt::format("[crontab.run][id({}).name({})] exception in callback", id_, cfg_.name);
    }
    lock.lock();
    if (!cfg_.async) ArmLocked();
  }
  ReleasePendingLocked();
}

// Requires mu_.
void Crontab::ReleasePendingLocked() {
  --pending_ops_;
  CHECK_GE(pending_ops_, 0) << cfg_.name;
  if (pending_ops_ == 0) bthread_cond_broadcast(&drained_cv_);
}

CrontabManager::CrontabManager() { CHECK_EQ(bthread_mutex_init(&mu_, nullptr), 0); }

CrontabManager::~CrontabManager() {
  Stop();
  bthread_mutex_destroy(&mu_);
}

void CrontabManager::AddCrontab(std::vector<CrontabConfig> configs) {
  std::vector<std::shared_ptr<Crontab>> to_launch;
  to_launch.reserve(configs.size());
  {
    std::lock_guard<bthread_mutex_t> lock(mu_);
    if (stopped_) {
      LOG(WARNING) << fmt::format("[crontab.add] manager already stopped; dropping {} config(s)", configs.size());
      return;
    }
    for (auto& cfg : configs) {
      LOG(INFO) << fmt::format("[crontab.add][name({}).interval({}ms).async({})] added", cfg.name, cfg.interval_ms,
                               cfg.async);
      auto task = std::make_shared<Crontab>(next_id_++, std::move(cfg));
      tasks_.push_back(task);
      to_launch.push_back(task);
    }
  }
  // Keep each task alive if Stop races with launch; RequestStop makes Launch a no-op.
  for (const auto& task : to_launch) {
    task->Launch();
  }
}

void CrontabManager::Stop() {
  std::vector<std::shared_ptr<Crontab>> draining;
  {
    std::lock_guard<bthread_mutex_t> lock(mu_);
    stopped_ = true;
    // Keep tasks visible until drained so concurrent Stop callers also wait.
    draining = tasks_;
  }
  for (const auto& task : draining) {
    task->RequestStop();
  }
  for (const auto& task : draining) {
    task->Join();
  }
  std::lock_guard<bthread_mutex_t> lock(mu_);
  tasks_.clear();
}

void CrontabManager::DescribeByJson(Json::Value& value) {
  CHECK(value.isArray()) << "value is not array.";
  std::vector<std::shared_ptr<Crontab>> snapshot;
  {
    std::lock_guard<bthread_mutex_t> lock(mu_);
    snapshot = tasks_;
  }
  for (const auto& task : snapshot) {
    Json::Value entry;
    task->DescribeByJson(entry);
    value.append(entry);
  }
}

}  // namespace mds
}  // namespace dingofs
