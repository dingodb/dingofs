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

/*
 * Project: DingoFS
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/heartbeat.h"

#include <atomic>

#include "common/options/cache.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(periodic_heartbeat_interval_s, 3,
              "interval to send heartbeat to mds in seconds");

Heartbeat::Heartbeat(MDSClientSPtr mds_client)
    : running_(false),
      mds_client_(mds_client),
      executor_(std::make_unique<BthreadExecutor>()) {}

void Heartbeat::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Heartbeat already started";
    return;
  }

  LOG(INFO) << "Heartbeat is starting...";

  CHECK(executor_->Start());
  executor_->Schedule([this] { PeriodicSendHeartbeat(); },
                      FLAGS_periodic_heartbeat_interval_s * 1000);

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "Heartbeat started";
}

void Heartbeat::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Heartbeat already shutdown";
    return;
  }

  LOG(INFO) << "Heartbeat is shutting down...";

  CHECK(executor_->Stop());

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "Heartbeat is down";
}

void Heartbeat::SendHeartbeat() {
  auto status =
      mds_client_->Heartbeat(FLAGS_id, FLAGS_listen_ip, FLAGS_listen_port);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to send heartbeat{id=" << FLAGS_id
               << " ip=" << FLAGS_listen_ip << " port=" << FLAGS_listen_port
               << "} to mds";
  } else {
    VLOG(3) << "Successfully send heartbeat{id=" << FLAGS_id
            << " ip=" << FLAGS_listen_ip << " port=" << FLAGS_listen_port
            << "} to mds";
  }
}

void Heartbeat::PeriodicSendHeartbeat() {
  SendHeartbeat();
  executor_->Schedule([this] { PeriodicSendHeartbeat(); },
                      FLAGS_periodic_heartbeat_interval_s * 1000);
}

}  // namespace cache
}  // namespace dingofs
