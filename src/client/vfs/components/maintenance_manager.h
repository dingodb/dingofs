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
#ifndef DINGOFS_CLIENT_VFS_COMPONENTS_MAINTENANCE_MANAGER_H_
#define DINGOFS_CLIENT_VFS_COMPONENTS_MAINTENANCE_MANAGER_H_

#include <cstddef>
#include <memory>
#include <string>

#include "common/status.h"

namespace dingofs {
class Executor;
namespace client {
namespace vfs {

class MaintenanceTask {
 public:
  virtual ~MaintenanceTask() = default;
  // Process at most budget objects. True requests the next batch immediately;
  // false waits for the next tick. Async I/O is task-owned, not waited here.
  virtual bool RunOnce(size_t budget) = 0;
  // Called on the lifecycle thread after RunOnce calls drain. Release residual
  // snapshots and wait for this task's async work and actual holder cleanup.
  // Task methods must not re-enter their manager's lifecycle APIs.
  virtual void OnStop() = 0;
};

// Public scheduling only; components implement and register MaintenanceTask.
class MaintenanceManager final {
 public:
  MaintenanceManager();
  ~MaintenanceManager();
  MaintenanceManager(const MaintenanceManager&) = delete;
  MaintenanceManager& operator=(const MaintenanceManager&) = delete;

  // Register before Start with a positive task-specific interval. Names and
  // instances must be unique; executors enqueue, never inline. Dependencies
  // outlive OnStop. Executors must accept work until drain completes; rejection
  // after Start succeeds is a fatal contract violation. Stopped instances
  // cannot be reused; Control's mutex owns all lifecycle transitions, including
  // concurrent stop/unregister draining.
  Status RegisterTask(std::string name, std::shared_ptr<MaintenanceTask> task,
                      Executor* executor, int interval_ms);

  // Return NotFound if unregistered. Otherwise drain queued/running RunOnce
  // calls, invoke OnStop on this thread and wait for its cleanup. If global
  // Stop is already draining this task, wait for it instead. Never call from
  // a worker needed by the drain.
  Status UnregisterTask(const std::string& name);

  // Arm all tasks once. Return Internal if already started/stopped, or if an
  // initial wakeup is rejected; the latter fully drains to stopped first.
  Status Start();

  // Close admission, wait for queued/running RunOnce calls, then invoke each
  // task's OnStop outside the scheduler lock. Never call from a worker needed
  // by the drain. Idempotent; no restart and no wait for passive timer expiry.
  void StopAndDrain();

 private:
  struct Control;
  struct Registration;
  static bool ScheduleNextTick(const std::shared_ptr<Control>& control,
                               const std::shared_ptr<Registration>& entry);
  static void OnTick(const std::shared_ptr<Control>& control,
                     const std::shared_ptr<Registration>& entry);
  static void QueueRun(const std::shared_ptr<Control>& control,
                       const std::shared_ptr<Registration>& entry);
  static void RunTask(const std::shared_ptr<Control>& control,
                      const std::shared_ptr<Registration>& entry);
  static void FinishRun(const std::shared_ptr<Control>& control,
                        const std::shared_ptr<Registration>& entry, bool again);

  std::shared_ptr<Control> control_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
#endif
