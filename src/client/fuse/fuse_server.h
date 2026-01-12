/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_SRC_CLIENT_FUSE_FUSE_SERVER_H_
#define DINGOFS_SRC_CLIENT_FUSE_FUSE_SERVER_H_

#include <string>

#include "bvar/status.h"
#include "client/fuse/fuse_common.h"
#include "fuse3/fuse.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace fuse {

class FuseServer {
 public:
  explicit FuseServer();
  ~FuseServer();
  FuseServer(const FuseServer&) = delete;
  FuseServer& operator=(const FuseServer&) = delete;

  int Init(const std::string& program_name, struct MountOption* mount_option);

  int AddMountOptions();

  int CreateSession(void* userdata);

  void DestroySsesion();

  int SessionMount();

  void SessionUnmount();

  int Serve();

  void Shutdown();

  void MarkThenShutdown();

 private:
  void AllocateFuseInitBuf();

  void FreeFuseInitBuf();

  int GetDevFd() const;

  void UdsServerFunc();

  int SaveOpInitMsg();

  void ProcessInitMsg();

  int SessionLoop();

  bool ShutdownGracefully(const std::string& mountpoint);

  void UdsServerStart();

  void ExportMetrics(const std::string& key, const std::string& value);

  std::string program_name_;

  // libfuse
  struct fuse_args args_{0, nullptr, 0};
  struct fuse_session* session_{nullptr};
  struct fuse_loop_config* config_{nullptr};
  struct MountOption* mount_option_{nullptr};
  struct fuse_buf init_fbuf_{0};

  // Manager uds thread
  utils::Thread uds_thread_;
  utils::Atomic<bool> is_running_{false};

  // Manager smooth upgrade
  int fuse_fd_{-1};
  std::string fd_comm_file_;

  // manager metrics
  bvar::Status<std::string> fd_comm_metrics_;
};

}  // namespace fuse
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_FUSE_SERVER_H_
