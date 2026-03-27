/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DINGOFS_UTILS_LOGCLEAN_MANAGER_H_
#define DINGOFS_UTILS_LOGCLEAN_MANAGER_H_

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <unordered_set>

#include "common/status.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace utils {

class LogCleanManager;
using LogCleanManagerUPtr = std::unique_ptr<LogCleanManager>;

class LogCleanManager {
 public:
  LogCleanManager(const std::string log_dir) : log_dir_(log_dir) {};

  ~LogCleanManager() = default;

  LogCleanManager(const LogCleanManager&) = delete;
  LogCleanManager& operator=(const LogCleanManager&) = delete;

  Status Start();

  Status Stop();

 private:
  std::unordered_set<std::string> GenerateProjectFiles() const;
  void CleanLogs();
  void ScheduleClean();

  std::unique_ptr<Executor> executor_;
  std::atomic<bool> running_{false};
  std::filesystem::path log_dir_;
};

}  // namespace utils
}  // namespace dingofs

#endif  // DINGOFS_UTILS_LOGCLEAN_MANAGER_H_
