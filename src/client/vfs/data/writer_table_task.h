/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
#ifndef DINGOFS_CLIENT_VFS_DATA_WRITER_TABLE_TASK_H_
#define DINGOFS_CLIENT_VFS_DATA_WRITER_TABLE_TASK_H_
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "client/vfs/components/maintenance_manager.h"
namespace dingofs {
class Executor;
namespace client {
namespace vfs {
class FileWriter;
class WriterTable;

// No global manager pointer. The table and cleanup executor outlive OnStop.
// A full slot budget returns to the scheduler; the next tick resumes next_.
class WriterTableTask final
    : public MaintenanceTask,
      public std::enable_shared_from_this<WriterTableTask> {
 public:
  WriterTableTask(WriterTable* table, Executor* cleanup_executor);
  bool RunOnce(size_t budget) override;
  void OnStop() override;

 private:
  static constexpr size_t kMaxInFlight = 64;
  struct Member {
    FileWriter* writer;
    bool owns_record;
  };
  void StartFlush(Member member);
  void QueueCleanup(Member member);
  void Cleanup(Member member);

  WriterTable* table_;
  Executor* cleanup_executor_;

  // Scan state: RunOnce and OnStop are manager-serialized, not mutex-guarded.
  size_t shard_{0};
  size_t next_{0};
  std::vector<FileWriter*> snapshot_;
  std::mutex mutex_;
  std::condition_variable drained_;
  // Async state: guarded by mutex_, including access from cleanup callbacks.
  size_t in_flight_{0};  // includes actual holder release
  bool stopping_{false};
  std::unordered_set<FileWriter*>
      flushing_;  // avoid duplicate cross-round work
};
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
#endif
