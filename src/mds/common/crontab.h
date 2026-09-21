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

#ifndef DINGOFS_MDS_COMMON_CRONTAB_H_
#define DINGOFS_MDS_COMMON_CRONTAB_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "json/value.h"

namespace dingofs {
namespace mds {

// Configuration for a single crontab task.
struct CrontabConfig {
  // Human-readable name; used only for logging and JSON reports.
  std::string name;
  // Delay between async submissions, or after a synchronous callback returns.
  uint32_t interval_ms;
  // Async callbacks run on bthreads and may overlap; callers own synchronization.
  // Sync callbacks run on the shared timer thread and must not block.
  // An immediate first invocation always runs on a bthread.
  bool async;
  // Captured resources must outlive Join() / CrontabManager::Stop().
  std::function<void()> callback;
  // 0 means unlimited. Every invocation counts, including those that throw.
  uint32_t max_times{0};
  // If true the first invocation does not wait interval_ms.
  bool immediately{false};
};

// Launch at most once. RequestStop, then Join before destruction; never Join
// from this task's callback. The owner must join all callers before destruction.

class Crontab {
 public:
  Crontab(uint32_t id, CrontabConfig cfg);
  ~Crontab();

  Crontab(const Crontab&) = delete;
  Crontab& operator=(const Crontab&) = delete;

  void Launch();
  // Permanently close scheduling without waiting for admitted callbacks.
  void RequestStop();
  // Wait for all timer/bthread reservations to be released.
  // RequestStop() must be called first; Join() does not request stopping.
  void Join();
  void DescribeByJson(Json::Value& value) const;

 private:
  static void OnTimer(void* arg);
  static void* OnBthreadRun(void* arg);
  void OnTimerFired();
  void StartBthreadLocked();
  void RunOnce();
  void ArmLocked();
  void ReleasePendingLocked();

  const uint32_t id_;
  const CrontabConfig cfg_;

  // mu_ guards scheduling state. Each timer and each dispatched bthread owns
  // a reservation; Join waits until all reservations have been released.
  mutable bthread_mutex_t mu_;
  bthread_cond_t drained_cv_;
  bool stop_requested_{false};
  bool has_timer_{false};
  bthread_timer_t timer_id_{0};
  // Count admitted invocations, including callbacks still running or throwing.
  uint32_t run_count_{0};
  int32_t pending_ops_{0};
};

// Owns periodic tasks. Async invocations of the same task may overlap.
// AddCrontab takes ownership of the configs; after Stop it is a no-op.
// Stop closes scheduling for all tasks before waiting, including concurrent calls.
// Callbacks must not call Stop or destroy this manager (that would self-wait).
// The owner must join all callers before destruction; the destructor calls Stop.
class CrontabManager {
 public:
  CrontabManager();
  ~CrontabManager();

  CrontabManager(const CrontabManager&) = delete;
  CrontabManager& operator=(const CrontabManager&) = delete;

  void AddCrontab(std::vector<CrontabConfig> configs);

  void Stop();

  void DescribeByJson(Json::Value& value);

 private:
  bthread_mutex_t mu_;
  std::vector<std::shared_ptr<Crontab>> tasks_;
  uint32_t next_id_{1};
  bool stopped_{false};
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_CRONTAB_H_
