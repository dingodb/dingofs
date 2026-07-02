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

#include "client/fuse/upgrade/handover_session_impl.h"

#include "client/fuse/fuse_common.h"
#include "fuse_lowlevel.h"

namespace dingofs {
namespace client {
namespace fuse {

HandoverSessionImpl::HandoverSessionImpl(struct fuse_session* session)
    : session_(session) {}

void HandoverSessionImpl::PauseReceive() {
  fuse_session_pause_receive(session_);
}

void HandoverSessionImpl::ResumeReceive() {
  fuse_session_resume_receive(session_);
}

int HandoverSessionImpl::WaitDrained(uint32_t timeout_ms) {
  return fuse_session_wait_drained(session_, timeout_ms);
}

void HandoverSessionImpl::Exit() { fuse_session_exit(session_); }

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
