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

#include "utils/logclean_manager.h"

#include <absl/strings/str_split.h>
#include <dirent.h>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <string>
#include <system_error>

#include "common/options/common.h"
#include "common/status.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "utils/executor/thread/executor_impl.h"
#include "utils/time.h"

namespace dingofs {
namespace utils {

namespace fs = std::filesystem;

static constexpr int kCleanIntervalMs = 60000;  // 60 seconds

Status LogCleanManager::Start() {
  CHECK(FLAGS_log_retention_seconds > 0)
      << "Log retention seconds must be greater than 0";

  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Log clean manager already running";
    return Status::Internal("Log clean manager already running");
  }

  executor_ = std::make_unique<ExecutorImpl>("log_clean", 1);
  auto ok = executor_->Start();
  if (!ok) {
    LOG(ERROR) << "Start log clean manager executor failed";
    return Status::Internal("Start log clean manager executor failed");
  }

  executor_->Schedule([this]() { ScheduleClean(); }, kCleanIntervalMs);

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "Log clean manager started, log retention: "
            << FLAGS_log_retention_seconds << " seconds,"
            << " log clean filter pattern: [" << FLAGS_log_clean_filter_pattern
            << "]";

  return Status::OK();
}

Status LogCleanManager::Stop() {
  if (!running_.load()) {
    return Status::OK();
  }

  auto ok = executor_->Stop();
  if (!ok) {
    LOG(ERROR) << "Stop log clean executor failed.";
    return Status::Internal("Stop log clean executor failed.");
  }
  executor_.reset();

  running_.store(false, std::memory_order_relaxed);

  LOG(INFO) << "Log clean manager stopped";
  return Status::OK();
}

void LogCleanManager::ScheduleClean() {
  if (!running_.load(std::memory_order_relaxed)) {
    return;
  }

  CleanLogs();
  executor_->Schedule([this]() { ScheduleClean(); }, kCleanIntervalMs);
}

void LogCleanManager::CleanLogs() {
  LOG(INFO) << "Start cleaning logs...";

  if (!FLAGS_log_clean_enable) {
    LOG(INFO) << "Log clean is disabled, skip cleaning logs.";
    return;
  }

  fs::path log_dir(log_dir_);
  std::error_code ec;

  if (!fs::exists(log_dir, ec) || !fs::is_directory(log_dir, ec)) {
    LOG(ERROR) << "Check log dir error: " << ec.message();
    return;
  }

  std::vector<std::string> filter_patterns =
      absl::StrSplit(FLAGS_log_clean_filter_pattern, ",");
  if (filter_patterns.empty()) {
    LOG(WARNING) << "No valid filter pattern for log cleaning";
    return;
  }

  auto now = utils::TimeNow();

  for (const auto& entry : fs::directory_iterator(log_dir, ec)) {
    if (ec) {
      LOG(ERROR) << "Scan directory error: " << ec.message();
      break;
    }

    const auto& path = entry.path();
    std::string filename = path.filename().string();

    if (std::none_of(filter_patterns.begin(), filter_patterns.end(),  // NOLINT
                     [&filename](const std::string& pattern) {
                       return filename.find(pattern) != std::string::npos;
                     })) {
      continue;
    }

    if (!entry.is_regular_file(ec)) {
      ec.clear();
      continue;
    }

    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
      continue;
    }

    int64_t age_seconds = now.seconds - st.st_mtime;
    if (age_seconds > FLAGS_log_retention_seconds) {
      if (fs::remove(path, ec)) {
        LOG(INFO) << "Removed log file: " << path;
      } else {
        LOG(ERROR) << "Failed to remove log file: " << path << ": "
                   << ec.message();
      }
    }
  }
}

}  // namespace utils
}  // namespace dingofs
