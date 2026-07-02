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

#ifndef DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_CONTROLLER_H_
#define DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_CONTROLLER_H_

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "client/fuse/upgrade/handover_peer.h"
#include "client/fuse/upgrade/handover_session.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace fuse {

struct HandoverOptions {
  std::string mountpoint;
  uint32_t drain_timeout_ms{0};
  uint32_t statfs_interval_ms{0};
};

class HandoverController {
 public:
  using HandoverCheckpoint = std::function<Status()>;

  HandoverController(HandoverOptions options, HandoverPeer* peer,
                     HandoverSession* session);
  ~HandoverController();

  HandoverController(const HandoverController&) = delete;
  HandoverController& operator=(const HandoverController&) = delete;

  void Start();

  void Stop();

  void SetCheckpoint(HandoverCheckpoint checkpoint);

  bool RequestHandover();

 private:
  class HandoverRequestEvent;

  bool WaitForRequest();

  void Run();

  Status DrainSessionForHandover();

  Status RunCheckpoint();

  void AbortHandover(const Status& status);

  void CommitHandover();

  HandoverOptions options_;
  HandoverPeer* peer_{nullptr};
  HandoverSession* session_{nullptr};

  std::mutex checkpoint_mutex_;
  HandoverCheckpoint checkpoint_;

  std::unique_ptr<HandoverRequestEvent> request_event_;
  std::thread thread_;
  std::atomic<bool> shutdown_{false};
  std::atomic<bool> started_{false};
};

}  // namespace fuse
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_CONTROLLER_H_
