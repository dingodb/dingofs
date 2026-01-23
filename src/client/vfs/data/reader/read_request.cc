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

#include "client/vfs/data/reader/read_request.h"

#include <mutex>

namespace dingofs {
namespace client {
namespace vfs {

void ReadRequest::ToStateUnLock(ReadRequestState new_state,
                                TransitionReason reason) {
  VLOG(9) << fmt::format("ReadRequest::ToState uuid: {} [{}-{}] reason: {}",
                         UUID(), ReadRequestStateToString(state),
                         ReadRequestStateToString(new_state),
                         TransitionReasonToString(reason));
  state = new_state;
}

std::string ReadRequest::ToStringUnlock() const {
  return fmt::format(
      "(uuid: {}, state: {}, readers: {}, access_sec: {},req:  {}, status: {})",
      UUID(), ReadRequestStateToString(state), readers, access_sec,
      req.ToString(), status.ToString());
}

std::string ReadRequest::ToString() const {
  std::unique_lock<std::mutex> lock(mutex);
  return ToStringUnlock();
}

std::string PartialReadRequest::ToString() const {
  return fmt::format("(uuid: {}, req_range: [{}-{}), req: {})", req->UUID(),
                     offset, (offset + len), req->ToString());
}

std::string PartialReadRequest::ToStringUnlock() const {
  return fmt::format("(uuid: {}, req_range: [{}-{}), req: {})", req->UUID(),
                     offset, (offset + len), req->ToStringUnlock());
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
