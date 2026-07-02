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

#include <functional>
#include <memory>
#include <string>

#include "bvar/status.h"
#include "client/fuse/fuse_common.h"
#include "common/status.h"
#include "fuse3/fuse.h"

namespace dingofs {
namespace client {
namespace fuse {

class HandoverController;
class HandoverServer;
class HandoverSessionImpl;

class FuseServer {
 public:
  explicit FuseServer();

  ~FuseServer();

  FuseServer(const FuseServer&) = delete;
  FuseServer& operator=(const FuseServer&) = delete;

  int Init(const std::string& program_name, struct MountOption* mount_option);

  // FUSE session lifecycle: Create -> Mount -> Serve -> Unmount -> Destroy.
  int CreateSession(void* userdata);
  int SessionMount();
  int Serve();
  void SessionUnmount();
  void DestroySession();

  // Exit the session loop (fuse_session_exit).
  void Shutdown();

  // Hot-upgrade coordination (the handover server/client/controller do the
  // actual work, in their own files).
  void ArmHandoverController();
  // Register the fail-able handover checkpoint run on the old process after the
  // session is drained and before it exits. A non-OK return rolls the handover
  // back (resume receive, keep serving). Set once during FuseOpInit, before any
  // handover can be requested.
  void SetHandoverCheckpoint(std::function<Status()> checkpoint);
  // Wake the handover controller; called from the graceful signal thread.
  void TriggerHandover();

 private:
  int AddMountOptions();
  void AllocateFuseInitBuf();
  void FreeFuseInitBuf();

  // Serve() helpers.
  int SaveOpInitMsg();
  void ProcessInitMsg();
  int SessionLoop();
  void ExportMetrics(const std::string& key, const std::string& value);

  int GetDevFd() const;

  std::string program_name_;

  // libfuse
  struct fuse_args args_ {
    0, nullptr, 0
  };

  struct fuse_session* session_{nullptr};
  struct fuse_loop_config* config_{nullptr};
  struct MountOption* mount_option_{nullptr};
  struct fuse_buf init_fbuf_ {
    0
  };

  // Manager smooth upgrade
  std::string fd_comm_file_;

  std::unique_ptr<HandoverServer> handover_server_;
  std::unique_ptr<HandoverSessionImpl> session_handover_;
  std::unique_ptr<HandoverController> handover_controller_;

  // manager metrics
  bvar::Status<std::string> fd_comm_metrics_;
};

}  // namespace fuse
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_FUSE_SERVER_H_
