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

#include "client/fuse/upgrade/handover_client.h"

#include <brpc/reloadable_flags.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <thread>

#include "absl/strings/str_format.h"
#include "client/fuse/fuse_common.h"
#include "client/fuse/upgrade/handover_channel.h"
#include "client/fuse/upgrade/state_store.h"
#include "common/const.h"
#include "fuse_lowlevel.h"
#include "glog/logging.h"
#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace client {
namespace fuse {

// Declared in common/options/client.h.
DEFINE_uint32(fuse_fd_get_max_retries, 100,
              "the max retries that get fuse fd from old dingo-client during "
              "smooth upgrade");
DEFINE_validator(fuse_fd_get_max_retries, brpc::PassValidate);

DEFINE_uint32(fuse_fd_get_retry_interval_ms, 100,
              "the interval in millseconds that get fuse fd from old "
              "dingo-client during smooth upgrade");
DEFINE_validator(fuse_fd_get_retry_interval_ms, brpc::PassValidate);

int HandoverClient::FindOldPid(const std::string& stats_file) {
  int pid = 0;
  uint32_t retry = 0;
  do {
    pid = GetDingoFusePid(stats_file);
    if (pid > 0) break;
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_fuse_fd_get_retry_interval_ms));
  } while (++retry <= FLAGS_fuse_fd_get_max_retries);
  return pid;
}

int HandoverClient::ReceiveFuseFd(const std::string& comm_path,
                                  struct fuse_buf* init_msg, int* comm_fd) {
  int fuse_fd = GetFuseFd(comm_path.c_str(), init_msg->mem, init_msg->mem_size,
                          &init_msg->size, comm_fd);
  for (int i = 0; i < FLAGS_fuse_fd_get_max_retries && fuse_fd <= 2; i++) {
    if (*comm_fd >= 0) {
      close(*comm_fd);
      *comm_fd = -1;
    }
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_fuse_fd_get_retry_interval_ms));
    fuse_fd = GetFuseFd(comm_path.c_str(), init_msg->mem, init_msg->mem_size,
                        &init_msg->size, comm_fd);
  }
  return fuse_fd;
}

bool HandoverClient::Handshake(int comm_fd, int old_pid) {
  // Announce the handover on the live UDS connection, then wake the old
  // process. The old drains only after reading this kPrepare, so a stray SIGHUP
  // (or a stale connection) never triggers a drain. Send before the signal so
  // the message is already buffered when the old process reads it.
  if (!SendHandoverMessage(comm_fd, HandoverMessage::kPrepare)) {
    LOG(ERROR) << "hot-upgrade: send kPrepare to old failed, error: "
               << std::strerror(errno);
    return false;
  }

  if (kill(old_pid, SIGHUP) != 0) {
    LOG(ERROR) << "hot-upgrade: send SIGHUP to old process failed, pid: "
               << old_pid << ", error: " << std::strerror(errno);
    return false;
  }

  HandoverMessage msg;
  if (!RecvHandoverMessage(comm_fd, &msg)) {
    LOG(ERROR) << "hot-upgrade: wait old handover message failed, error: "
               << std::strerror(errno);
    return false;
  }

  if (msg == HandoverMessage::kNack) {
    LOG(ERROR) << "hot-upgrade: old process rejected handover and resumed";
    return false;
  }

  if (msg != HandoverMessage::kReadyToExit) {
    LOG(ERROR) << "hot-upgrade: unexpected old handover message: "
               << static_cast<uint32_t>(msg);
    return false;
  }

  LOG(INFO) << "hot-upgrade: old process is ready to exit";
  return true;
}

int HandoverClient::TakeOver(const std::string& mountpoint,
                             struct fuse_buf* init_msg) {
  const std::string stats_file =
      absl::StrFormat("%s/%s", mountpoint, dingofs::kStatsName);

  int pid = FindOldPid(stats_file);
  if (pid <= 0) {
    LOG(ERROR) << "get pid fail, filepath=" << stats_file;
    return -1;
  }

  LOG(INFO) << "get pid success, pid=" << pid;
  UpgradeStateStore::GetInstance().SetOldFusePid(pid);

  const std::string comm_path = GetFdCommFileName(stats_file);
  CHECK(!comm_path.empty());
  LOG(INFO) << "get socket success, comm_path=" << comm_path;

  int comm_fd = -1;
  auto close_comm = MakeScopedCleanup([&]() {
    if (comm_fd >= 0) close(comm_fd);
  });

  int fuse_fd = ReceiveFuseFd(comm_path, init_msg, &comm_fd);
  if (fuse_fd <= 2) {
    LOG(ERROR) << "recv mount fd fail, comm_path=" << comm_path;
    return -1;
  }
  // The received /dev/fuse fd holds a reference to the kernel mount; close it
  // on any failure below, and hand it to the caller (cancel) only on success.
  auto close_fuse_fd = MakeScopedCleanup([&]() { close(fuse_fd); });

  LOG(INFO) << "recv data from " << comm_path << ", mount fd = " << fuse_fd
            << ", data size = " << init_msg->size;

  if (!Handshake(comm_fd, pid)) return -1;

  close_fuse_fd.cancel();
  return fuse_fd;
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
