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

#include "blockcache/infiniband/base/completion_channel.h"

#include <fcntl.h>
#include <glog/logging.h>

#include <cerrno>
#include <cstring>

#include "blockcache/common/status.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

StatusOr<CompletionChannel> CompletionChannel::Create(Device& device) {
  ibv_comp_channel* channel = ibv_create_comp_channel(device.context());
  if (channel == nullptr) {
    return ToStatus(errno, "create completion channel");
  }

  int flags = fcntl(channel->fd, F_GETFL);
  if (flags == -1) {
    Status status = ToStatus(errno, "get completion channel flags");
    ibv_destroy_comp_channel(channel);
    return status;
  }
  if (fcntl(channel->fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    Status status = ToStatus(errno, "set completion channel flags");
    ibv_destroy_comp_channel(channel);
    return status;
  }

  LOG(INFO) << "Successfully create CompletionChannel{device=" << device.name()
            << " fd=" << channel->fd << "}";
  return CompletionChannel(channel);
}

void CompletionChannel::Reset() noexcept {
  if (channel_ == nullptr) {
    return;
  }

  const int rc = ibv_destroy_comp_channel(channel_);
  if (rc != 0) {
    LOG(ERROR) << "Fail to destroy completion channel: " << std::strerror(rc);
  }
  channel_ = nullptr;
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
