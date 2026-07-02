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

#ifndef DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_SESSION_H_
#define DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_SESSION_H_

#include <cstdint>

namespace dingofs {
namespace client {
namespace fuse {

// The FUSE session operations the handover controller needs, abstracted so the
// controller does not call libfuse directly (and can be unit-tested with a
// mock). The production implementation is backed by a real fuse_session.
class HandoverSession {
 public:
  virtual ~HandoverSession() = default;

  // Stop reading new requests off the /dev/fuse fd(s) (reversible).
  virtual void PauseReceive() = 0;
  // Resume reading new requests (undo PauseReceive).
  virtual void ResumeReceive() = 0;
  // Wait until in-flight requests have been replied. Returns 0 on success,
  // non-zero on timeout/error.
  virtual int WaitDrained(uint32_t timeout_ms) = 0;
  // Exit the session loop (fuse_session_exit).
  virtual void Exit() = 0;
};

}  // namespace fuse
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_SESSION_H_
