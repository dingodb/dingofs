// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDS_SERVICE_ACCESS_H_
#define DINGOFS_MDS_SERVICE_ACCESS_H_

#include <string>
#include <vector>

#include "brpc/channel.h"
#include "bthread/types.h"
#include "dingofs/mds.pb.h"
#include "mds/common/status.h"

namespace dingofs {
namespace mds {

// brpc::Channel pool for rpc request.
class ChannelPool {
 public:
  static ChannelPool& GetInstance();

  std::shared_ptr<brpc::Channel> GetChannel(const butil::EndPoint& endpoint);

 private:
  ChannelPool();
  ~ChannelPool();

  bthread_mutex_t mutex_;
  std::map<butil::EndPoint, std::shared_ptr<brpc::Channel>> channels_;
};

class ServiceAccess {
 public:
  static Status CheckAlive(const butil::EndPoint& endpoint);
  static Status NotifyBuddy(const butil::EndPoint& endpoint, const pb::mds::NotifyBuddyRequest& request);
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_SERVICE_ACCESS_H_