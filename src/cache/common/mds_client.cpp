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
 * Created Date: 2025-08-18
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/mds_client.h"

#include <absl/strings/str_split.h>
#include <brpc/reloadable_flags.h>
#include <butil/fast_rand.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "cache/common/macro.h"
#include "cache/utils/helper.h"
#include "client/vfs/meta/v2/mds_discovery.h"
#include "client/vfs/meta/v2/rpc.h"
#include "common/status.h"
#include "dingofs/cachegroup.pb.h"
#include "dingofs/common.pb.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/mds/mds_meta.h"

namespace dingofs {
namespace cache {

DEFINE_string(mds_addrs, "",
              "Cache group member manager service rpc addresses");
DEFINE_validator(mds_addrs, Helper::NonEmptyString);

DEFINE_string(mds_version, "v2",
              "MDS version for member managment, 'v1' or 'v2'");

DEFINE_uint64(mdsv1_rpc_retry_total_ms, 16000, "");
DEFINE_uint64(mdsv1_rpc_max_timeout_ms, 2000, "");
DEFINE_uint64(mdsv1_rpc_timeout_ms, 500, "");
DEFINE_uint64(mdsv1_rpc_retry_interval_us, 50000, "");
DEFINE_uint64(mdsv1_rpc_max_failed_times_before_change_addr, 2, "");
DEFINE_uint64(mdsv1_rpc_normal_retry_times_before_trigger_wait, 3, "");
DEFINE_uint64(mdsv1_rpc_wait_sleep_ms, 1000, "");

DEFINE_int64(mdsv2_rpc_timeout_ms, 3000, "mdsv2 rpc timeout");
DEFINE_validator(mdsv2_rpc_timeout_ms, brpc::PassValidate);

DEFINE_int32(mdsv2_rpc_retry_times, 1, "mdsv2 rpc retry time");
DEFINE_validator(mdsv2_rpc_retry_times, brpc::PassValidate);

DEFINE_uint32(mdsv2_request_retry_times, 3, "mdsv2 rpc request retry time");
DEFINE_validator(mdsv2_request_retry_times, brpc::PassValidate);

static stub::common::MdsOption NewCommonMDSOption() {
  stub::common::MdsOption option;
  auto& retry = option.rpcRetryOpt;

  option.mdsMaxRetryMS = FLAGS_mdsv1_rpc_retry_total_ms;
  retry.addrs = absl::StrSplit(FLAGS_mds_addrs, ',', absl::SkipEmpty());
  retry.maxRPCTimeoutMS = FLAGS_mdsv1_rpc_max_timeout_ms;
  retry.rpcTimeoutMs = FLAGS_mdsv1_rpc_timeout_ms;
  retry.rpcRetryIntervalUS = FLAGS_mdsv1_rpc_retry_interval_us;
  retry.maxFailedTimesBeforeChangeAddr =
      FLAGS_mdsv1_rpc_max_failed_times_before_change_addr;
  retry.normalRetryTimesBeforeTriggerWait =
      FLAGS_mdsv1_rpc_normal_retry_times_before_trigger_wait;
  retry.waitSleepMs = FLAGS_mdsv1_rpc_wait_sleep_ms;

  return option;
}

MDSV1Client::MDSV1Client()
    : running_(false),
      mds_base_(std::make_unique<stub::rpcclient::MDSBaseClient>()),
      mds_client_(std::make_unique<stub::rpcclient::MdsClientImpl>()) {}

Status MDSV1Client::Start() {
  CHECK_NOTNULL(mds_base_);
  CHECK_NOTNULL(mds_client_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "MDS v1 client is starting...";

  auto rc = mds_client_->Init(NewCommonMDSOption(), mds_base_.get());
  if (rc != pb::mds::FSStatusCode::OK) {
    LOG(ERROR) << "Init MDS v1 client failed: rc = "
               << pb::mds::FSStatusCode_Name(rc);
    return Status::Internal("init mds client failed");
  }

  running_ = true;

  LOG(INFO) << "MDS v1 client is up.";

  CHECK_RUNNING("MDS v1 client");
  return Status::OK();
}

Status MDSV1Client::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "MDS v1 client is shutting down...";

  // do nothing

  LOG(INFO) << "MDS v1 client is down.";

  CHECK_DOWN("MDS v1 client");
  return Status::OK();
}

Status MDSV1Client::GetFSInfo(uint64_t fs_id,
                              pb::common::StorageInfo* storage_info) {
  CHECK_RUNNING("MDS v1 client");

  pb::mds::FsInfo fs_info;
  auto code = mds_client_->GetFsInfo(fs_id, &fs_info);
  if (code != pb::mds::FSStatusCode::OK) {
    LOG(ERROR) << "Get filesystem information failed: fs_id = " << fs_id
               << ", rc = " << pb::mds::FSStatusCode_Name(code);
    return Status::Internal("get filesystem information failed");
  } else if (!fs_info.has_storage_info()) {
    LOG(ERROR) << "The filesystem missing storage_info: fs_id = " << fs_id;
    return Status::Internal("filesystem missing storage info");
  }

  *storage_info = fs_info.storage_info();
  return Status::OK();
}

Status MDSV1Client::JoinCacheGroup(const std::string& id, const std::string& ip,
                                   uint32_t port, const std::string& group_name,
                                   uint32_t weight, std::string* member_id) {
  auto rc =
      mds_client_->JoinCacheGroup(group_name, ip, port, weight, member_id);
  if (rc != pb::mds::cachegroup::CacheGroupOk) {
    LOG(ERROR) << "Join cache group failed: group_name = " << group_name
               << ", ip = " << ip << ", port = " << port
               << ", weight = " << weight
               << ", rc = " << CacheGroupErrCode_Name(rc);
    return Status::Internal("join cache group failed");
  }
  return Status::OK();
}

Status MDSV1Client::LeaveCacheGroup(const std::string& /*member_id*/,
                                    const std::string& ip, uint32_t port,
                                    const std::string& group_name) {
  auto rc = mds_client_->LeaveCacheGroup(group_name, ip, port);
  if (rc != pb::mds::cachegroup::CacheGroupOk) {
    LOG(ERROR) << "Leave cache group failed: group_name = " << group_name
               << ", ip = " << ip << ", port = " << port
               << ", rc = " << CacheGroupErrCode_Name(rc);
    return Status::Internal("leave cache group failed");
  }
  return Status::OK();
}

Status MDSV1Client::Heartbeat(const std::string& member_id,
                              const std::string& ip, uint32_t port) {
  auto rc = mds_client_->SendCacheGroupHeartbeat(ip, port);
  if (rc != pb::mds::cachegroup::CacheGroupOk) {
    LOG(ERROR) << "Send cache group heartbeat failed: member_id = " << member_id
               << ", ip = " << ip << ", port = " << port
               << ", rc = " << CacheGroupErrCode_Name(rc);
    return Status::Internal("send heartbeat failed");
  }
  return Status::OK();
}

Status MDSV1Client::ListMembers(const std::string& group_name,
                                std::vector<CacheGroupMember>* members) {
  std::vector<pb::mds::cachegroup::CacheGroupMember> pb_members;
  auto rc = mds_client_->ListCacheGroupMembers(group_name, &pb_members);
  if (rc != pb::mds::cachegroup::CacheGroupOk) {
    LOG(ERROR) << "List cache group members failed: group_name = " << group_name
               << ", rc = " << CacheGroupErrCode_Name(rc);
    return Status::Internal("list cache group members failed");
  }

  CacheGroupMember member;
  for (const auto& pb_member : pb_members) {
    member.id = pb_member.id();
    member.ip = pb_member.ip();
    member.port = pb_member.port();
    member.weight = pb_member.weight();
    member.state = ToMemberState(pb_member.state());
    members->push_back(member);
  }
  return Status::OK();
}

CacheGroupMemberState MDSV1Client::ToMemberState(
    pb::mds::cachegroup::CacheGroupMemberState state) {
  switch (state) {
    case pb::mds::cachegroup::CacheGroupMemberStateOnline:
      return CacheGroupMemberState::kOnline;
    case pb::mds::cachegroup::CacheGroupMemberStateUnstable:
      return CacheGroupMemberState::kUnstable;
    case pb::mds::cachegroup::CacheGroupMemberStateOffline:
      return CacheGroupMemberState::kOffline;
    case pb::mds::cachegroup::CacheGroupMemberStateUnknown:
    default:
      return CacheGroupMemberState::kUnknown;
  }
}

MDSV2Client::MDSV2Client(const std::string& mds_addr)
    : running_(false),
      rpc_(client::vfs::v2::RPC::New(mds_addr)),
      mds_discovery_(std::make_unique<client::vfs::v2::MDSDiscovery>(rpc_)) {}

Status MDSV2Client::Start() {
  CHECK_NOTNULL(rpc_);
  CHECK_NOTNULL(mds_discovery_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "MDS v2 client is starting...";

  if (!mds_discovery_->Init()) {
    LOG(ERROR) << "Init MDS v2 discovery failed";
    return Status::Internal("init mds v2 discovery failed");
  }

  running_ = true;

  LOG(INFO) << "MDS v2 client is up.";

  CHECK_RUNNING("MDS v2 client");
  return Status::OK();
}

Status MDSV2Client::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "MDS v2 client is shutting down...";

  mds_discovery_->Destroy();
  rpc_->Destory();

  LOG(INFO) << "MDS v2 client is down.";

  CHECK_DOWN("MDS v2 client");
  return Status::OK();
}

Status MDSV2Client::GetFSInfo(uint64_t fs_id,
                              pb::common::StorageInfo* storage_info) {
  pb::mdsv2::GetFsInfoRequest request;
  pb::mdsv2::GetFsInfoResponse response;

  request.set_fs_id(fs_id);
  auto status = SendRequest("MDSService", "GetFsInfo", request, response);
  if (status.ok()) {
    *storage_info = ToCommonStorageInfo(response.fs_info());
  }
  return status;
}

Status MDSV2Client::JoinCacheGroup(const std::string& want_id,
                                   const std::string& ip, uint32_t port,
                                   const std::string& group_name,
                                   uint32_t weight, std::string* member_id) {
  pb::mdsv2::JoinCacheGroupRequest request;
  pb::mdsv2::JoinCacheGroupResponse response;

  request.set_member_id(want_id);
  request.set_ip(ip);
  request.set_port(port);
  request.set_group_name(group_name);
  request.set_weight(weight);

  auto status = SendRequest("MDSService", "JoinCacheGroup", request, response);
  if (!status.ok()) {
    LOG(ERROR) << "Join cache group failed: group_name = " << group_name
               << ", ip = " << ip << ", port = " << port
               << ", weight = " << weight << ", status = " << status.ToString();
    return Status::Internal("join cache group failed");
  }

  *member_id = want_id;
  LOG(INFO) << "Join cache group success: group_name = " << group_name
            << ", ip = " << ip << ", port = " << port << ", weight = " << weight
            << ", member_id = " << *member_id;
  return Status::OK();
}

Status MDSV2Client::LeaveCacheGroup(const std::string& member_id,
                                    const std::string& ip, uint32_t port,
                                    const std::string& group_name) {
  pb::mdsv2::LeaveCacheGroupRequest request;
  pb::mdsv2::LeaveCacheGroupResponse response;

  request.set_member_id(member_id);
  request.set_ip(ip);
  request.set_port(port);
  request.set_group_name(group_name);

  auto status = SendRequest("MDSService", "LeaveCacheGroup", request, response);
  if (!status.ok()) {
    LOG(ERROR) << "Leave cache group failed: group_name = " << group_name
               << ", ip = " << ip << ", port = " << port
               << ", member_id = " << member_id
               << ", status = " << status.ToString();
  }
  return status;
}

Status MDSV2Client::Heartbeat(const std::string& member_id,
                              const std::string& ip, uint32_t port) {
  pb::mdsv2::HeartbeatRequest request;
  pb::mdsv2::HeartbeatResponse response;

  request.set_role(pb::mdsv2::ROLE_CACHE_MEMBER);
  auto* member = request.mutable_cache_group_member();
  member->set_member_id(member_id);
  member->set_ip(ip);
  member->set_port(port);

  auto status = SendRequest("MDSService", "Heartbeat", request, response);
  if (!status.ok()) {
    LOG(ERROR) << "Send cache group member heartbeat failed: member_id = "
               << member_id << ", ip = " << ip << ", port = " << port
               << ", status = " << status.ToString();
  }
  return status;
}

Status MDSV2Client::ListMembers(const std::string& group_name,
                                std::vector<CacheGroupMember>* members) {
  pb::mdsv2::ListMembersRequest request;
  pb::mdsv2::ListMembersResponse response;

  request.set_group_name(group_name);
  auto status = SendRequest("MDSService", "ListMembers", request, response);
  if (!status.ok()) {
    LOG(ERROR) << "List cache group members failed: group_name = " << group_name
               << ", status = " << status.ToString();
    return status;
  }

  CacheGroupMember member;
  const auto& pb_members = response.members();
  for (const auto& pb_member : pb_members) {
    member.id = pb_member.member_id();
    member.ip = pb_member.ip();
    member.port = pb_member.port();
    member.weight = pb_member.weight();
    member.state = ToMemberState(pb_member.state());
    members->push_back(member);
  }
  return Status::OK();
}

pb::common::S3Info MDSV2Client::ToCommonS3Info(const pb::mdsv2::S3Info& in) {
  pb::common::S3Info out;
  out.set_ak(in.ak());
  out.set_sk(in.sk());
  out.set_endpoint(in.endpoint());
  out.set_bucketname(in.bucketname());

  return out;
}

pb::common::RadosInfo MDSV2Client::ToCommonRadosInfo(
    const pb::mdsv2::RadosInfo& in) {
  pb::common::RadosInfo out;
  out.set_user_name(in.user_name());
  out.set_key(in.key());
  out.set_mon_host(in.mon_host());
  out.set_pool_name(in.pool_name());
  out.set_cluster_name(in.cluster_name());

  return out;
}

pb::common::StorageInfo MDSV2Client::ToCommonStorageInfo(
    const pb::mdsv2::FsInfo& fs_info) {
  pb::common::StorageInfo out;
  if (fs_info.fs_type() == pb::mdsv2::FsType::S3) {
    out.set_type(pb::common::StorageType::TYPE_S3);
    *out.mutable_s3_info() = ToCommonS3Info(fs_info.extra().s3_info());
  } else if (fs_info.fs_type() == pb::mdsv2::FsType::RADOS) {
    out.set_type(pb::common::StorageType::TYPE_RADOS);
    *out.mutable_rados_info() = ToCommonRadosInfo(fs_info.extra().rados_info());
  } else {
    CHECK(false) << "Unsupported fs type: "
                 << pb::mdsv2::FsType_Name(fs_info.fs_type());
  }

  return out;
}

CacheGroupMemberState MDSV2Client::ToMemberState(
    pb::mdsv2::CacheGroupMemberState state) {
  switch (state) {
    case pb::mdsv2::CacheGroupMemberStateOnline:
      return CacheGroupMemberState::kOnline;
    case pb::mdsv2::CacheGroupMemberStateUnstable:
      return CacheGroupMemberState::kUnstable;
    case pb::mdsv2::CacheGroupMemberStateOffline:
      return CacheGroupMemberState::kOffline;
    case pb::mdsv2::CacheGroupMemberStateUnknown:
    default:
      return CacheGroupMemberState::kUnknown;
  }
}

mdsv2::MDSMeta MDSV2Client::GetRandomlyMDS(const mdsv2::MDSMeta& old_mds) {
  auto mdses = mds_discovery_->GetNormalMDS(true);
  CHECK(!mdses.empty()) << "No normal mds found";

  std::vector<mdsv2::MDSMeta> candidates;
  for (const auto& mds : mdses) {
    if (mds.ID() != old_mds.ID()) {
      candidates.emplace_back(mds);
    }
  }

  if (!candidates.empty()) {
    return candidates[butil::fast_rand_less_than(candidates.size())];
  }
  return old_mds;
}

bool MDSV2Client::ShouldRetry(Status status) {
  static std::unordered_map<int, bool> should_retry_errnos = {
      {pb::error::EROUTER_EPOCH_CHANGE, true},
      {pb::error::ENOT_SERVE, true},
      {pb::error::EINTERNAL, true},
      {pb::error::ENOT_CAN_CONNECTED, true},
  };
  return status.IsNetError() || should_retry_errnos.count(status.Errno()) != 0;
}

bool MDSV2Client::ShouldSetMDSAbormal(Status status) {
  return status.IsInternal() || status.IsNetError();
}

bool MDSV2Client::ShouldRefreshMDSList(Status status) {
  static std::unordered_map<int, bool> should_refresh_errnos = {
      {pb::error::EROUTER_EPOCH_CHANGE, true},
      {pb::error::ENOT_SERVE, true},
  };

  return should_refresh_errnos.count(status.Errno()) != 0;
}

template <typename Request, typename Response>
Status MDSV2Client::SendRequest(const std::string& service_name,
                                const std::string& api_name, Request& request,
                                Response& response) {
  mdsv2::MDSMeta mds, old_mds;
  client::vfs::v2::SendRequestOption rpc_option;
  rpc_option.timeout_ms = FLAGS_mdsv2_rpc_timeout_ms;
  rpc_option.max_retry = FLAGS_mdsv2_rpc_retry_times;

  for (int retry = 0; retry < FLAGS_mdsv2_request_retry_times; ++retry) {
    mds = GetRandomlyMDS(old_mds);
    auto endpoint = client::vfs::v2::StrToEndpoint(mds.Host(), mds.Port());

    auto status = rpc_->SendRequest(endpoint, service_name, api_name, request,
                                    response, rpc_option);
    if (status.ok() || !ShouldRetry(status)) {
      return status;
    }

    if (ShouldSetMDSAbormal(status)) {
      mds_discovery_->SetAbnormalMDS(mds.ID());
      LOG(INFO) << fmt::format(
          "[mds.client] set mds({}/{}:{}) as abnormal, status({}).", mds.ID(),
          mds.Host(), mds.Port(), status.ToString());
    }

    if (ShouldRefreshMDSList(status)) {
      CHECK(mds_discovery_->RefreshFullyMDSList()) << "Refresh mds list fail";
    }

    old_mds = mds;
  }

  return Status::Internal("send request fail");
}

MDSClientSPtr BuildSharedMDSClient() {
  if (FLAGS_mds_version == "v1") {
    return std::make_shared<MDSV1Client>();
  } else if (FLAGS_mds_version == "v2") {
    return std::make_shared<MDSV2Client>(FLAGS_mds_addrs);
  } else {
    CHECK(false) << "Unsupported MDS version: " << FLAGS_mds_version;
  }
}

MDSClientUPtr BuildUniqueMDSClient() {
  if (FLAGS_mds_version == "v1") {
    return std::make_unique<MDSV1Client>();
  } else if (FLAGS_mds_version == "v2") {
    return std::make_unique<MDSV2Client>(FLAGS_mds_addrs);
  } else {
    CHECK(false) << "Unsupported MDS version: " << FLAGS_mds_version;
  }
}

}  // namespace cache
}  // namespace dingofs
