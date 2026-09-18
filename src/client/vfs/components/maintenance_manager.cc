/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "client/vfs/components/maintenance_manager.h"

#include <glog/logging.h>

#include <boost/range/algorithm/find_if.hpp>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <utility>
#include <vector>

#include "common/sync_point.h"
#include "utils/executor/executor.h"
#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace client {
namespace vfs {

constexpr size_t kRunOnceBudget = 64;

struct MaintenanceManager::Control {
  enum class State : uint8_t { kCreated, kRunning, kStopping, kStopped };
  std::mutex mutex;
  std::condition_variable cv;
  State state{State::kCreated};
  size_t active_tasks{0};  // queued/running batches and unregister cleanup
  std::vector<std::shared_ptr<Registration>> tasks;  // guarded by mutex
};

// Published metadata is immutable; scheduling flags share the owning Control
// lock.
struct MaintenanceManager::Registration {
  std::string name;
  std::shared_ptr<MaintenanceTask> task;
  Executor* executor;
  int interval_ms{0};
  bool scheduled{false};
  bool running{false};
  bool stopping{false};
};

MaintenanceManager::MaintenanceManager()
    : control_(std::make_shared<Control>()) {}

MaintenanceManager::~MaintenanceManager() { StopAndDrain(); }

Status MaintenanceManager::RegisterTask(std::string name,
                                        std::shared_ptr<MaintenanceTask> task,
                                        Executor* executor, int interval_ms) {
  if (name.empty() || !task || executor == nullptr || interval_ms <= 0) {
    return Status::InvalidParam(
        "maintenance registration requires name, task, executor and positive "
        "interval");
  }

  std::lock_guard<std::mutex> lock(control_->mutex);
  if (control_->state != Control::State::kCreated) {
    return Status::InvalidParam("register maintenance tasks before Start");
  }

  for (const auto& entry : control_->tasks) {
    if (entry->name == name || entry->task == task) {
      return Status::InvalidParam("maintenance task already registered");
    }
  }
  auto entry = std::make_shared<Registration>();
  entry->name = std::move(name);
  entry->task = std::move(task);
  entry->executor = executor;
  entry->interval_ms = interval_ms;
  control_->tasks.push_back(std::move(entry));
  return Status::OK();
}

Status MaintenanceManager::Start() {
  bool accepted = true;
  {
    std::lock_guard<std::mutex> lock(control_->mutex);
    if (control_->state != Control::State::kCreated) {
      return Status::Internal("maintenance already started or stopped");
    }
    control_->state = Control::State::kRunning;
    for (const auto& entry : control_->tasks) {
      if (!ScheduleNextTick(control_, entry)) {
        accepted = false;
        break;
      }
    }
  }

  if (!accepted) {
    StopAndDrain();
    return Status::Internal("initial maintenance wakeup rejected");
  }
  return Status::OK();
}

// Caller holds Control::mutex. Timers capture no manager/Hub pointer and only
// weakly reference the registration, so removed tasks do not wait for expiry.
bool MaintenanceManager::ScheduleNextTick(
    const std::shared_ptr<Control>& control,
    const std::shared_ptr<Registration>& entry) {
  std::weak_ptr<Registration> weak = entry;
  return entry->executor->Schedule(
      [control, weak] {
        if (auto entry = weak.lock()) OnTick(control, entry);
      },
      entry->interval_ms);
}

void MaintenanceManager::OnTick(const std::shared_ptr<Control>& control,
                                const std::shared_ptr<Registration>& entry) {
  bool submit = false;
  {
    std::lock_guard<std::mutex> lock(control->mutex);
    if (control->state != Control::State::kRunning || entry->stopping) return;
    CHECK(ScheduleNextTick(control, entry)) << "maintenance tick rejected";
    if (!entry->scheduled && !entry->running) {
      entry->scheduled = true;
      ++control->active_tasks;
      submit = true;
    }
  }
  if (submit) QueueRun(control, entry);
}

void MaintenanceManager::QueueRun(const std::shared_ptr<Control>& control,
                                  const std::shared_ptr<Registration>& entry) {
  CHECK(entry->executor->Execute([control, entry] { RunTask(control, entry); }))
      << "executor rejected an admitted maintenance task";
}

void MaintenanceManager::RunTask(const std::shared_ptr<Control>& control,
                                 const std::shared_ptr<Registration>& entry) {
  bool admitted = false;
  {
    std::lock_guard<std::mutex> lock(control->mutex);
    entry->scheduled = false;
    entry->running = true;
    admitted = control->state == Control::State::kRunning && !entry->stopping;
  }

  bool again = false;
  auto finish = MakeScopedCleanup([&] { FinishRun(control, entry, again); });

  if (admitted) again = entry->task->RunOnce(kRunOnceBudget);
}

void MaintenanceManager::FinishRun(const std::shared_ptr<Control>& control,
                                   const std::shared_ptr<Registration>& entry,
                                   bool again) {
  bool submit = false;
  {
    std::lock_guard<std::mutex> lock(control->mutex);
    entry->running = false;
    if (again && control->state == Control::State::kRunning &&
        !entry->stopping) {
      entry->scheduled = true;
      submit = true;  // transfer the counted unit to the continuation
    } else {
      CHECK_GT(control->active_tasks, 0u);
      --control->active_tasks;
    }
    control->cv.notify_all();
  }

  if (submit) QueueRun(control, entry);
}

Status MaintenanceManager::UnregisterTask(const std::string& name) {
  auto control = control_;
  std::shared_ptr<Registration> entry;
  {
    std::unique_lock<std::mutex> lock(control->mutex);
    auto it = boost::range::find_if(
        control->tasks, [&](const auto& task) { return task->name == name; });
    if (it == control->tasks.end()) {
      return Status::NotFound("maintenance task not registered");
    }
    if (control->state == Control::State::kStopping) {
      control->cv.wait(
          lock, [&] { return control->state == Control::State::kStopped; });
      return Status::OK();
    }
    entry = *it;
    entry->stopping = true;
    control->tasks.erase(it);
    ++control->active_tasks;  // Global Stop must include this local OnStop.
    TEST_SYNC_POINT("MaintenanceManager:unregister:cancelled");
    control->cv.wait(lock,
                     [&] { return !entry->scheduled && !entry->running; });
  }

  auto complete = MakeScopedCleanup([control] {
    std::lock_guard<std::mutex> lock(control->mutex);
    --control->active_tasks;
    control->cv.notify_all();
  });

  entry->task->OnStop();
  entry.reset();
  return Status::OK();
}

void MaintenanceManager::StopAndDrain() {
  std::vector<std::shared_ptr<Registration>> stopping;
  {
    std::unique_lock<std::mutex> lock(control_->mutex);
    if (control_->state == Control::State::kStopped) return;
    if (control_->state == Control::State::kStopping) {
      control_->cv.wait(
          lock, [this] { return control_->state == Control::State::kStopped; });
      return;
    }
    control_->state = Control::State::kStopping;
    for (const auto& entry : control_->tasks) entry->stopping = true;
    control_->cv.wait(lock, [&] { return control_->active_tasks == 0; });
    stopping = control_->tasks;
  }

  // OnStop may block on task-local cleanup. Never hold Control's mutex here.
  for (const auto& entry : stopping) entry->task->OnStop();

  stopping.clear();  // Make swap below leave the registry empty.
  {
    std::lock_guard<std::mutex> lock(control_->mutex);
    control_->tasks.swap(stopping);
  }
  stopping.clear();  // Destroy task objects outside the scheduler lock.

  {
    std::lock_guard<std::mutex> lock(control_->mutex);
    control_->state = Control::State::kStopped;
    control_->cv.notify_all();
  }
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
