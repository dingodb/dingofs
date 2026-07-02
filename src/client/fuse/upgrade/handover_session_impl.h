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

#ifndef DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_SESSION_IMPL_H_
#define DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_SESSION_IMPL_H_

#include <cstdint>

#include "client/fuse/upgrade/handover_session.h"

struct fuse_session;

namespace dingofs {
namespace client {
namespace fuse {

// HandoverSession backed by a real libfuse session. Thin adapter so the
// handover controller can drive the drain/exit without FuseServer having to
// inherit the interface, and without the controller touching libfuse.
class HandoverSessionImpl : public HandoverSession {
 public:
  explicit HandoverSessionImpl(struct fuse_session* session);

  void PauseReceive() override;
  void ResumeReceive() override;
  int WaitDrained(uint32_t timeout_ms) override;
  void Exit() override;

 private:
  struct fuse_session* session_;
};

}  // namespace fuse
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_SESSION_IMPL_H_
