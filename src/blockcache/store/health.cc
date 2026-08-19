/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "blockcache/store/health.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>

#include "blockcache/core/fs/filesystem.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/store/local_filesystem.h"
#include "blockcache/utils/align.h"
#include "blockcache/utils/time.h"

namespace dingofs {
namespace blockcache {

DEFINE_uint32(disk_state_check_duration_ms, 3000, "probe interval");
DEFINE_uint32(disk_state_probe_timeout_ms, 5000, "probe timeout");
DEFINE_uint32(disk_state_tick_duration_s, 60, "state window length");
DEFINE_uint32(disk_state_normal2unstable_error_num, 3,
              "errors that turn a disk unstable");
DEFINE_uint32(disk_state_unstable2normal_succ_num, 10,
              "successes that turn a disk normal");
DEFINE_uint32(disk_state_unstable2down_s, 1800,
              "unstable seconds before a disk goes down");

HealthChecker::HealthChecker(const DiskCacheLayout& layout)
    : layout_(layout),
      health_state_(FLAGS_disk_state_tick_duration_s,
                    FLAGS_disk_state_normal2unstable_error_num,
                    FLAGS_disk_state_unstable2normal_succ_num,
                    FLAGS_disk_state_unstable2down_s) {
  write_buffer_ = CHECK_NOTNULL(AlignedAlloc(kProbeBytes));
  read_buffer_ = CHECK_NOTNULL(AlignedAlloc(kProbeBytes));
  for (uint32_t i = 0; i < kProbeBytes; ++i) {
    write_buffer_[i] = static_cast<char>(i);
  }
}

HealthChecker::~HealthChecker() {
  std::free(write_buffer_);
  std::free(read_buffer_);
}

Future<> HealthChecker::Start() {
  LOG(INFO) << "HealthChecker is starting...";

  running_ = true;
  Future<> worker = PeriodicCheckDisk();

  LOG(INFO) << "Successfully start HealthChecker{check_duration_ms="
            << FLAGS_disk_state_check_duration_ms
            << " probe_timeout_ms=" << FLAGS_disk_state_probe_timeout_ms
            << " tick_duration_s=" << FLAGS_disk_state_tick_duration_s
            << " normal2unstable_error_num="
            << FLAGS_disk_state_normal2unstable_error_num
            << " unstable2normal_succ_num="
            << FLAGS_disk_state_unstable2normal_succ_num
            << " unstable2down_s=" << FLAGS_disk_state_unstable2down_s << "}";
  return worker;
}

Future<> HealthChecker::Shutdown() {
  LOG(INFO) << "HealthChecker is shutting down...";

  running_ = false;
  co_await gate_.Close();

  LOG(INFO) << "Successfully shutdown HealthChecker";
}

Future<> HealthChecker::PeriodicCheckDisk() {
  Gate::Holder holder(gate_);
  CHECK(holder.ok()) << "HealthCheck is down";

  while (running_) {
    co_await SleepWhile([this] { return running_; },
                        FLAGS_disk_state_check_duration_ms);
    if (!running_) {
      break;
    }

    if (!co_await CheckLockFile()) {
      health_state_.TransitionToDown();
      continue;
    }

    if (co_await CheckDisk()) {
      health_state_.OnSuccess(1);
    } else {
      health_state_.OnError(1);
    }

    health_state_.Advance(TimestampSec());
  }
}

Future<bool> HealthChecker::CheckLockFile() const {
  const StatusOr<FileStat> stat =
      co_await FileSystem::StatPath(layout_.LockPath());
  const Status& status = stat.status();
  if (status.ok()) {
    co_return true;
  } else if (status.IsNotExist()) {
    LOG_FIRST_N(ERROR, 1) << "Fail to stat lock file=`" << layout_.LockPath()
                          << "' which is missing, mark disk down";
    co_return false;
  }

  LOG(WARNING) << "Fail to stat lock file=`" << layout_.LockPath()
               << "', skip this check: " << status.ToString();
  co_return true;
}

Future<bool> HealthChecker::CheckDisk() const {
  const std::string path = ProbePath();
  const uint64_t begin_ns = TimestampNs();

  if (!co_await WriteProbeFile(path)) {
    co_return false;
  }

  if (!co_await ReadProbeFile(path)) {
    co_return false;
  }

  if (!(co_await FileSystem::Unlink(path)).ok()) {
    co_return false;
  }

  const uint64_t elapsed_ms = (TimestampNs() - begin_ns) / 1000000ULL;
  co_return elapsed_ms <= FLAGS_disk_state_probe_timeout_ms;
}

Future<bool> HealthChecker::WriteProbeFile(std::string path) const {
  // open
  StatusOr<File> open =
      co_await FileSystem::Open(path, OpenFlags::kWrite | OpenFlags::kCreate,
                                LocalFileSystem::BlockOpenOption());
  if (!open.ok()) {
    co_return false;
  }

  // write
  File file = std::move(open).value();
  StatusOr<size_t> nwritten =
      co_await file.Write(0, write_buffer_, kProbeBytes);

  // close
  const Status close = co_await file.Close();

  co_return nwritten.ok() && *nwritten == kProbeBytes&& close.ok();
}

Future<bool> HealthChecker::ReadProbeFile(std::string path) const {
  // open
  StatusOr<File> open = co_await FileSystem::Open(
      path, OpenFlags::kRead, LocalFileSystem::BlockOpenOption());
  if (!open.ok()) {
    co_return false;
  }

  // read
  File file = std::move(open).value();
  StatusOr<size_t> nread = co_await file.Read(0, read_buffer_, kProbeBytes);

  // close
  (void)co_await file.Close();
  if (!nread.ok() || *nread != kProbeBytes) {
    co_return false;
  }

  co_return std::memcmp(read_buffer_, write_buffer_, kProbeBytes) == 0;
}

}  // namespace blockcache
}  // namespace dingofs
