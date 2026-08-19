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

#include "blockcache/node/heartbeat.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>

#include "blockcache/common/flag_decls.h"
#include "blockcache/utils/thread.h"

namespace dingofs {
namespace blockcache {

DEFINE_uint32(periodic_heartbeat_interval_s, 3,
              "heartbeat interval in seconds");
DEFINE_validator(periodic_heartbeat_interval_s, brpc::PassValidate);

Heartbeat::Heartbeat(MDSClient* mds_client) : mds_client_(mds_client) {}

Heartbeat::~Heartbeat() { Shutdown(); }

void Heartbeat::Start() {
  LOG(INFO) << "Heartbeat is starting...";

  CHECK(!thread_.joinable()) << "Heartbeat started twice";

  running_ = true;

  thread_ = std::thread([this] {
    SetThreadName("heartbeat");
    PeriodicSendHeartbeat();
  });

  LOG(INFO) << "Successfully start Heartbeat{interval="
            << FLAGS_periodic_heartbeat_interval_s << "s}";
}

void Heartbeat::Shutdown() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) {
      return;
    }
    running_ = false;
  }
  cv_.notify_one();

  LOG(INFO) << "Heartbeat is shutting down...";

  thread_.join();

  LOG(INFO) << "Successfully shutdown Heartbeat";
}

void Heartbeat::PeriodicSendHeartbeat() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!cv_.wait_for(
      lock, std::chrono::seconds(FLAGS_periodic_heartbeat_interval_s),
      [this] { return !running_; })) {
    lock.unlock();
    SendHeartbeat();
    lock.lock();
  }
}

void Heartbeat::SendHeartbeat() {
  const Status status =
      mds_client_->Heartbeat(FLAGS_id, FLAGS_listen_ip, FLAGS_listen_port);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to send heartbeat{id=" << FLAGS_id
               << " ip=" << FLAGS_listen_ip << " port=" << FLAGS_listen_port
               << "} to mds: " << status.ToString();
  } else {
    LOG_EVERY_N(INFO, 60) << "Successfully send heartbeat{id=" << FLAGS_id
                          << " ip=" << FLAGS_listen_ip
                          << " port=" << FLAGS_listen_port << "} to mds";
  }
}

}  // namespace blockcache
}  // namespace dingofs
