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

#include "blockcache/node/membership.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "blockcache/common/flag_decls.h"
#include "blockcache/utils/string.h"

namespace dingofs {
namespace blockcache {

DEFINE_string(group_name, "default", "cache group to join");

DEFINE_uint32(group_weight, 100, "node weight in consistent hash");
DEFINE_validator(group_weight, brpc::PassValidate);

GroupMembership::GroupMembership(MDSClient* mds_client)
    : mds_client_(mds_client) {}

GroupMembership::~GroupMembership() { Shutdown(); }

Status GroupMembership::Start() {
  LOG(INFO) << "GroupMembership is starting...";

  FLAGS_listen_ip = TrimWhitespace(FLAGS_listen_ip);
  if (FLAGS_listen_ip.empty()) {
    LOG(ERROR) << "Fail to start GroupMembership: --listen_ip is required";
    return Status::InvalidParam("--listen_ip is required");
  }

  Status status = mds_client_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start MDSClient: " << status.ToString();
    return status;
  }

  status = JoinGroup();
  if (!status.ok()) {
    mds_client_->Shutdown();
    return status;
  }

  running_ = true;
  LOG(INFO) << "Successfully start GroupMembership";
  return Status::OK();
}

void GroupMembership::Shutdown() {
  if (!running_) {
    return;
  }

  LOG(INFO) << "GroupMembership is shutting down...";

  LeaveGroup();
  mds_client_->Shutdown();

  running_ = false;
  LOG(INFO) << "Successfully shutdown GroupMembership";
}

Status GroupMembership::JoinGroup() {
  Status status =
      mds_client_->JoinCacheGroup(FLAGS_id, FLAGS_listen_ip, FLAGS_listen_port,
                                  FLAGS_group_name, FLAGS_group_weight);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to send JoinCacheGroup rpc request: "
               << status.ToString();
    return status;
  }

  LOG(INFO) << "Successfully join node{id=" << FLAGS_id
            << " ip=" << FLAGS_listen_ip << " port=" << FLAGS_listen_port
            << " weight=" << FLAGS_group_weight
            << "} into cache group=" << FLAGS_group_name;
  return Status::OK();
}

void GroupMembership::LeaveGroup() {
  const Status status = mds_client_->LeaveCacheGroup(
      FLAGS_id, FLAGS_listen_ip, FLAGS_listen_port, FLAGS_group_name);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to send LeaveCacheGroup rpc request: "
               << status.ToString();
    return;
  }

  LOG(INFO) << "Successfully leave node{id=" << FLAGS_id
            << " ip=" << FLAGS_listen_ip << " port=" << FLAGS_listen_port
            << "} from cache group=" << FLAGS_group_name;
}

}  // namespace blockcache
}  // namespace dingofs
