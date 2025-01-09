/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Date: Tuesday Nov 23 11:20:08 CST 2021
 * Author: wuhanqing
 */

#ifndef DINGOFS_SRC_METASERVER_COPYSET_COPYSET_CONF_CHANGE_H_
#define DINGOFS_SRC_METASERVER_COPYSET_COPYSET_CONF_CHANGE_H_

#include <braft/raft.h>

#include <utility>

#include "dingofs/common.pb.h"
#include "dingofs/heartbeat.pb.h"

namespace dingofs {
namespace metaserver {
namespace copyset {

struct OngoingConfChange {
  OngoingConfChange()
      : type(pb::mds::heartbeat::ConfigChangeType::NONE), alterPeer() {}

  OngoingConfChange(pb::mds::heartbeat::ConfigChangeType type,
                    const pb::common::Peer& peer)
      : type(type), alterPeer(peer) {}

  OngoingConfChange(pb::mds::heartbeat::ConfigChangeType type,
                    pb::common::Peer&& peer)
      : type(type), alterPeer(std::move(peer)) {}

  bool HasConfChange() const {
    return type != pb::mds::heartbeat::ConfigChangeType::NONE &&
           alterPeer.has_address();
  }

  void Reset() {
    type = pb::mds::heartbeat::ConfigChangeType::NONE;
    alterPeer.clear_address();
  }

  pb::mds::heartbeat::ConfigChangeType type;
  pb::common::Peer alterPeer;
};

class CopysetNode;

class OnConfChangeDone : public braft::Closure {
 public:
  OnConfChangeDone(CopysetNode* node, braft::Closure* done,
                   const OngoingConfChange& confChange)
      : node_(node), done_(done), confChange_(confChange) {}

  void Run() override;

 private:
  CopysetNode* node_;
  braft::Closure* done_;
  OngoingConfChange confChange_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_COPYSET_COPYSET_CONF_CHANGE_H_
