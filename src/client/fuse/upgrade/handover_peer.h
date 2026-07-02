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

#ifndef DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_PEER_H_
#define DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_PEER_H_

namespace dingofs {
namespace client {
namespace fuse {

class HandoverPeer {
 public:
  virtual ~HandoverPeer() = default;

  virtual bool WaitHandoverPrepare() = 0;
  virtual bool NotifyReadyAndWaitHandoverAck() = 0;
  virtual void NotifyHandoverAbort() = 0;
};

}  // namespace fuse
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_PEER_H_
