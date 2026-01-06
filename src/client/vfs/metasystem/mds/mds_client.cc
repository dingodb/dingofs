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

#include "client/vfs/metasystem/mds/mds_client.h"

#include <cstdint>
#include <string>
#include <utility>

#include "client/vfs/common/helper.h"
#include "client/vfs/metasystem/mds/helper.h"
#include "client/vfs/metasystem/mds/rpc.h"
#include "client/vfs/vfs_meta.h"
#include "common/const.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/type.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

MDSClient::MDSClient(const ClientId& client_id, mds::FsInfoSPtr fs_info,
                     ParentMemoSPtr parent_memo, MDSDiscoverySPtr mds_discovery,
                     MDSRouterPtr mds_router, RPCPtr rpc,
                     TraceManager& trace_manager)
    : client_id_(client_id),
      fs_info_(fs_info),
      fs_id_(fs_info->GetFsId()),
      epoch_(fs_info->GetEpoch()),
      parent_memo_(parent_memo),
      mds_discovery_(mds_discovery),
      mds_router_(mds_router),
      rpc_(rpc),
      trace_manager_(trace_manager) {}

bool MDSClient::Init() {
  CHECK(parent_memo_ != nullptr) << "parent cache is null.";
  CHECK(mds_discovery_ != nullptr) << "mds discovery is null.";
  CHECK(mds_router_ != nullptr) << "mds router is null.";
  CHECK(rpc_ != nullptr) << "rpc is null.";

  return true;
}

void MDSClient::Destory() {}

bool MDSClient::Dump(Json::Value& value) {
  DumpOption options;
  options.parent_memo = true;
  options.mds_router = true;
  options.rpc = true;

  return Dump(options, value);
}

bool MDSClient::Dump(const DumpOption& options, Json::Value& value) {
  LOG(INFO) << "[meta.client] dump...";

  if (options.parent_memo && !parent_memo_->Dump(value)) {
    return false;
  }

  if (options.mds_router && !mds_router_->Dump(value)) {
    return false;
  }

  if (options.rpc && !rpc_->Dump(value)) {
    return false;
  }

  return true;
}

bool MDSClient::Load(const Json::Value& value) {
  LOG(INFO) << "[meta.client] load...";
  return parent_memo_->Load(value);
}

bool MDSClient::SetEndpoint(const std::string& ip, int port) {
  return rpc_->AddEndpoint(ip, port);
}

Status MDSClient::DoGetFsInfo(RPCPtr rpc, pb::mds::GetFsInfoRequest& request,
                              mds::FsInfoEntry& fs_info) {
  pb::mds::GetFsInfoResponse response;

  auto status = rpc->SendRequest("MDSService", "GetFsInfo", request, response);
  if (status.ok()) {
    fs_info = response.fs_info();
  }
  return status;
}

MDSMeta MDSClient::GetMds(Ino ino, bool& is_primary_mds) {
  is_primary_mds = true;
  mds::MDSMeta mds_meta;
  if (!mds_router_->GetMDS(ino, mds_meta)) {
    CHECK(mds_router_->GetRandomlyMDS(mds_meta))
        << fmt::format("get randomly mds fail for ino({}).", ino);
    is_primary_mds = false;
  }

  VLOG(1) << fmt::format("[meta.client] query mds({}|{}:{}) for ino({}).",
                         mds_meta.ID(), mds_meta.Host(), mds_meta.Port(), ino);
  return mds_meta;
}

MDSMeta MDSClient::GetMdsByParent(int64_t parent, bool& is_primary_mds) {
  is_primary_mds = true;
  mds::MDSMeta mds_meta;
  if (!mds_router_->GetMDSByParent(parent, mds_meta)) {
    CHECK(mds_router_->GetRandomlyMDS(mds_meta))
        << fmt::format("get randomly mds fail for ino({}).", parent);
    is_primary_mds = false;
  }

  VLOG(1) << fmt::format("[meta.client] query mds({}|{}:{}) for parent({}).",
                         mds_meta.ID(), mds_meta.Host(), mds_meta.Port(),
                         parent);
  return mds_meta;
}

uint64_t MDSClient::GetInodeVersion(Ino ino) {
  uint64_t version = 0;
  parent_memo_->GetVersion(ino, version);
  return version;
}

int32_t MDSClient::GetInodeRenameRefCount(Ino ino) {
  int32_t rename_ref_count = 0;
  parent_memo_->GetRenameRefCount(ino, rename_ref_count);
  return rename_ref_count;
}

Status MDSClient::GetFsInfo(RPCPtr rpc, const std::string& name,
                            mds::FsInfoEntry& fs_info) {
  pb::mds::GetFsInfoRequest request;
  request.set_fs_name(name);
  return DoGetFsInfo(rpc, request, fs_info);
}

Status MDSClient::GetFsInfo(RPCPtr rpc, uint32_t fs_id,
                            mds::FsInfoEntry& fs_info) {
  pb::mds::GetFsInfoRequest request;
  request.set_fs_id(fs_id);
  return DoGetFsInfo(rpc, request, fs_info);
}

RPCPtr MDSClient::GetRpc() { return rpc_; }

Status MDSClient::Heartbeat() {
  if (client_id_.ID().empty()) {
    return Status::InvalidParam("client id is empty");
  }

  auto get_mds_fn = [this](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(kRootIno, is_primary_mds);
  };

  pb::mds::HeartbeatRequest request;
  pb::mds::HeartbeatResponse response;

  request.set_role(pb::mds::ROLE_CLIENT);
  auto* client = request.mutable_client();
  client->set_id(client_id_.ID());
  client->set_hostname(client_id_.Hostname());
  client->set_ip(client_id_.IP());
  client->set_port(client_id_.Port());
  client->set_mountpoint(client_id_.Mountpoint());
  client->set_fs_name(fs_info_->GetName());
  client->set_create_time_ms(client_id_.CreateTimeMs());

  auto status = SendRequest(nullptr, get_mds_fn, "MDSService", "Heartbeat",
                            request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::MountFs(const std::string& name,
                          const pb::mds::MountPoint& mount_point) {
  auto span = trace_manager_.StartSpan("MDSClient::MountFs");

  pb::mds::MountFsRequest request;
  pb::mds::MountFsResponse response;

  request.set_fs_name(name);
  request.mutable_mount_point()->CopyFrom(mount_point);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = rpc_->SendRequest("MDSService", "MountFs", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  return Status::OK();
}

Status MDSClient::UmountFs(const std::string& name,
                           const std::string& client_id) {
  auto span = trace_manager_.StartSpan("MDSClient::UmountFs");

  pb::mds::UmountFsRequest request;
  pb::mds::UmountFsResponse response;

  request.set_fs_name(name);
  request.set_client_id(client_id);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = rpc_->SendRequest("MDSService", "UmountFs", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  return Status::OK();
}

Status MDSClient::Lookup(ContextSPtr& ctx, Ino parent, const std::string& name,
                         AttrEntry& attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(parent, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::Lookup", ctx->GetTraceSpan());

  auto span_ctx = SpanScope::GetContext(span);

  pb::mds::LookupRequest request;
  pb::mds::LookupResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent));

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(span_ctx, get_mds_fn, "MDSService", "Lookup",
                            request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  const auto& inode = response.inode();

  if (fs_info_->IsHashPartition() && mds::IsDir(inode.ino())) {
    uint64_t last_version;
    if (parent_memo_->GetVersion(inode.ino(), last_version) &&
        inode.version() < last_version) {
      // fetch last inode
      status = GetAttr(span_ctx, inode.ino(), attr_entry);
      if (status.ok()) {
        parent_memo_->Upsert(inode.ino(), parent);
        return Status::OK();

      } else {
        LOG(WARNING) << fmt::format(
            "[meta.client.{}] lookup({}/{}) get last inode fail, error: {}.",
            fs_id_, parent, name, status.ToString());
      }
    }
  }

  // save ino to parent mapping
  parent_memo_->Upsert(inode.ino(), parent, inode.version());

  attr_entry.Swap(response.mutable_inode());

  return Status::OK();
}

Status MDSClient::Create(ContextSPtr& ctx, Ino parent, const std::string& name,
                         uint32_t uid, uint32_t gid, uint32_t mode, int flag,
                         AttrEntry& attr_entry, AttrEntry& parent_attr_entry,
                         std::vector<std::string>& session_ids) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(parent, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::Create", ctx->GetTraceSpan());

  pb::mds::BatchCreateRequest request;
  pb::mds::BatchCreateResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent));
  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));
  auto* param = request.add_params();

  param->set_name(name);
  param->set_mode(mode);
  param->set_flag(flag);
  param->set_uid(uid);
  param->set_gid(gid);
  param->set_rdev(0);
  param->set_length(0);

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "BatchCreate", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  CHECK(!response.inodes().empty()) << "inodes is empty.";
  const auto& inode = response.inodes().at(0);

  parent_memo_->Upsert(inode.ino(), parent, inode.version());
  parent_memo_->UpsertVersion(parent, response.parent_inode().version());

  session_ids = mds::Helper::PbRepeatedToVector(response.session_ids());

  attr_entry.Swap(response.mutable_inodes()->Mutable(0));
  parent_attr_entry.Swap(response.mutable_parent_inode());

  return Status::OK();
}

Status MDSClient::MkNod(ContextSPtr& ctx, Ino parent, const std::string& name,
                        uint32_t uid, uint32_t gid, mode_t mode, dev_t rdev,
                        AttrEntry& attr_entry, AttrEntry& parent_attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  CHECK(ctx != nullptr) << "context is nullptr.";

  auto get_mds_fn = [this, parent](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(parent, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::MkNod", ctx->GetTraceSpan());

  pb::mds::MkNodRequest request;
  pb::mds::MkNodResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent));
  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);
  request.set_mode(mode);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_rdev(rdev);

  request.set_length(0);

  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "MkNod", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  parent_memo_->Upsert(response.inode().ino(), parent,
                       response.inode().version());
  parent_memo_->UpsertVersion(parent, response.parent_inode().version());

  attr_entry.Swap(response.mutable_inode());
  parent_attr_entry.Swap(response.mutable_parent_inode());

  return Status::OK();
}

Status MDSClient::BatchMkNod(ContextSPtr& ctx, Ino parent,
                             const std::vector<MkNodParam>& params,
                             std::vector<AttrEntry>& attr_entries,
                             AttrEntry& parent_attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(parent, is_primary_mds);
  };

  pb::mds::BatchMkNodRequest request;
  pb::mds::BatchMkNodResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent));
  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  for (const auto& param : params) {
    auto* mut_param = request.add_params();
    mut_param->set_parent(parent);
    mut_param->set_name(param.name);
    mut_param->set_mode(param.mode);
    mut_param->set_uid(param.uid);
    mut_param->set_gid(param.gid);
    mut_param->set_rdev(param.rdev);
  }

  auto status = SendRequest(ctx, get_mds_fn, "MDSService", "BatchMkNod",
                            request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->UpsertVersion(parent, response.parent_inode().version());

  attr_entries.reserve(response.inodes_size());
  for (auto& mut_inode : *response.mutable_inodes()) {
    parent_memo_->Upsert(mut_inode.ino(), parent, mut_inode.version());

    attr_entries.push_back(std::move(mut_inode));
  }

  parent_attr_entry.Swap(response.mutable_parent_inode());

  return Status::OK();
}

Status MDSClient::MkDir(ContextSPtr& ctx, Ino parent, const std::string& name,
                        uint32_t uid, uint32_t gid, mode_t mode, dev_t rdev,
                        AttrEntry& attr_entry, AttrEntry& parent_attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  CHECK(ctx != nullptr) << "context is nullptr.";

  auto get_mds_fn = [this, parent](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(parent, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::MkDir", ctx->GetTraceSpan());

  pb::mds::MkDirRequest request;
  pb::mds::MkDirResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent));
  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);
  request.set_mode(mode);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_rdev(rdev);

  request.set_length(0);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "MkDir", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  parent_memo_->Upsert(response.inode().ino(), parent,
                       response.inode().version());
  parent_memo_->UpsertVersion(parent, response.parent_inode().version());

  attr_entry.Swap(response.mutable_inode());
  parent_attr_entry.Swap(response.mutable_parent_inode());

  return Status::OK();
}

Status MDSClient::BatchMkDir(ContextSPtr& ctx, Ino parent,
                             const std::vector<MkDirParam>& params,
                             std::vector<AttrEntry>& attr_entries,
                             AttrEntry& parent_attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(parent, is_primary_mds);
  };

  pb::mds::BatchMkDirRequest request;
  pb::mds::BatchMkDirResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent));
  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  for (const auto& param : params) {
    auto* mut_param = request.add_params();
    mut_param->set_parent(parent);
    mut_param->set_name(param.name);
    mut_param->set_mode(param.mode);
    mut_param->set_uid(param.uid);
    mut_param->set_gid(param.gid);
    mut_param->set_rdev(param.rdev);
  }

  auto status = SendRequest(ctx, get_mds_fn, "MDSService", "BatchMkDir",
                            request, response);
  if (!status.ok()) return status;

  parent_memo_->UpsertVersion(parent, response.parent_inode().version());

  attr_entries.reserve(response.inodes_size());
  for (auto& mut_inode : *response.mutable_inodes()) {
    parent_memo_->Upsert(mut_inode.ino(), parent, mut_inode.version());

    attr_entries.push_back(std::move(mut_inode));
  }

  parent_attr_entry.Swap(response.mutable_parent_inode());

  return Status::OK();
}

Status MDSClient::RmDir(ContextSPtr& ctx, Ino parent, const std::string& name,
                        Ino& ino, AttrEntry& parent_attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(parent, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::RmDir", ctx->GetTraceSpan());

  pb::mds::RmDirRequest request;
  pb::mds::RmDirResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent));
  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "RmDir", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  ino = response.ino();

  parent_memo_->Delete(ino);
  parent_memo_->UpsertVersion(parent, response.parent_inode().version());

  parent_attr_entry.Swap(response.mutable_parent_inode());

  return Status::OK();
}

Status MDSClient::ReadDir(ContextSPtr& ctx, Ino ino, uint64_t fh,
                          const std::string& last_name, uint32_t limit,
                          bool with_attr, std::vector<DirEntry>& entries) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(ino, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::ReadDir", ctx->GetTraceSpan());

  pb::mds::ReadDirRequest request;
  pb::mds::ReadDirResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));
  request.mutable_context()->set_use_base_version(GetInodeRenameRefCount(ino) >
                                                  0);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_last_name(last_name);
  request.set_limit(limit);
  request.set_with_attr(with_attr);
  request.set_fh(fh);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "ReadDir", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  parent_memo_->DecRenameRefCount(ino);

  entries.reserve(response.entries_size());
  for (const auto& entry : response.entries()) {
    parent_memo_->Upsert(entry.ino(), ino, entry.inode().version());
    entries.push_back(Helper::ToDirEntry(entry));
  }

  return Status::OK();
}

Status MDSClient::Open(
    ContextSPtr& ctx, Ino ino, int flags, std::string& session_id,
    bool is_prefetch_chunk,
    const std::vector<mds::ChunkDescriptor>& chunk_descriptors,
    AttrEntry& attr_entry, std::vector<mds::ChunkEntry>& chunks) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return GetMds(ino, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::Open", ctx->GetTraceSpan());

  pb::mds::OpenRequest request;
  pb::mds::OpenResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_flags(flags);
  request.set_prefetch_chunk(is_prefetch_chunk);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));
  if (is_prefetch_chunk) {
    *request.mutable_chunk_descriptors() = {chunk_descriptors.begin(),
                                            chunk_descriptors.end()};
  }

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "Open", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  session_id = response.session_id();
  chunks = mds::Helper::PbRepeatedToVector(response.chunks());
  attr_entry.Swap(response.mutable_inode());

  parent_memo_->UpsertVersion(ino, response.inode().version());

  return Status::OK();
}

Status MDSClient::Release(ContextSPtr& ctx, Ino ino,
                          const std::string& session_id) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return GetMds(ino, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::Release", ctx->GetTraceSpan());

  pb::mds::ReleaseRequest request;
  pb::mds::ReleaseResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_session_id(session_id);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "Release", request, response);

  SpanScope::SetStatus(span, status);
  return status;
}

Status MDSClient::Link(ContextSPtr& ctx, Ino ino, Ino new_parent,
                       const std::string& new_name, AttrEntry& attr_entry,
                       AttrEntry& parent_attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, new_parent](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(new_parent, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::Link", ctx->GetTraceSpan());

  pb::mds::LinkRequest request;
  pb::mds::LinkResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(new_parent));
  SetAncestorInContext(request, new_parent);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_new_parent(new_parent);
  request.set_new_name(new_name);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "Link", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  parent_memo_->Upsert(response.inode().ino(), new_parent,
                       response.inode().version());
  parent_memo_->UpsertVersion(new_parent, response.parent_inode().version());

  attr_entry.Swap(response.mutable_inode());
  parent_attr_entry.Swap(response.mutable_parent_inode());

  return Status::OK();
}

Status MDSClient::UnLink(ContextSPtr& ctx, Ino parent, const std::string& name,
                         AttrEntry& attr_entry, AttrEntry& parent_attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  CHECK(ctx != nullptr) << "context is nullptr.";

  auto get_mds_fn = [this, parent](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(parent, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::UnLink", ctx->GetTraceSpan());

  pb::mds::UnLinkRequest request;
  pb::mds::UnLinkResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent));

  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "UnLink", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  const auto& inode = response.inode();
  parent_memo_->UpsertVersion(inode.ino(), inode.version());
  parent_memo_->UpsertVersion(parent, response.parent_inode().version());

  attr_entry.Swap(response.mutable_inode());
  parent_attr_entry.Swap(response.mutable_parent_inode());

  return Status::OK();
}

Status MDSClient::BatchUnLink(ContextSPtr& ctx, Ino parent,
                              const std::vector<std::string>& names,
                              std::vector<AttrEntry>& attr_entries,
                              AttrEntry& parent_attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(parent, is_primary_mds);
  };

  pb::mds::BatchUnLinkRequest request;
  pb::mds::BatchUnLinkResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent));

  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  for (const auto& name : names) {
    request.add_names(name);
  }

  auto status = SendRequest(ctx, get_mds_fn, "MDSService", "BatchUnLink",
                            request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->UpsertVersion(parent, response.parent_inode().version());

  attr_entries.reserve(response.inodes_size());
  for (auto& mut_inode : *response.mutable_inodes()) {
    parent_memo_->UpsertVersion(mut_inode.ino(), mut_inode.version());

    attr_entries.push_back(std::move(mut_inode));
  }

  parent_attr_entry.Swap(response.mutable_parent_inode());

  return Status::OK();
}

Status MDSClient::Symlink(ContextSPtr& ctx, Ino parent, const std::string& name,
                          uint32_t uid, uint32_t gid,
                          const std::string& symlink, AttrEntry& attr_entry,
                          AttrEntry& parent_attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(parent, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::Symlink", ctx->GetTraceSpan());

  pb::mds::SymlinkRequest request;
  pb::mds::SymlinkResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent));
  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_symlink(symlink);

  request.set_new_parent(parent);
  request.set_new_name(name);
  request.set_uid(uid);
  request.set_gid(gid);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "Symlink", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  parent_memo_->Upsert(response.inode().ino(), parent,
                       response.inode().version());

  parent_memo_->UpsertVersion(parent, response.parent_inode().version());

  attr_entry.Swap(response.mutable_inode());
  parent_attr_entry.Swap(response.mutable_parent_inode());

  return Status::OK();
}

Status MDSClient::ReadLink(ContextSPtr& ctx, Ino ino, std::string& symlink) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return GetMds(ino, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::ReadLink", ctx->GetTraceSpan());

  pb::mds::ReadLinkRequest request;
  pb::mds::ReadLinkResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "ReadLink", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  symlink = response.symlink();

  return Status::OK();
}

Status MDSClient::GetAttr(ContextSPtr& ctx, Ino ino, AttrEntry& attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return mds::IsDir(ino) ? GetMdsByParent(ino, is_primary_mds)
                           : GetMds(ino, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::GetAttr", ctx->GetTraceSpan());

  pb::mds::GetAttrRequest request;
  pb::mds::GetAttrResponse response;

  auto* mut_ctx = request.mutable_context();
  mut_ctx->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "GetAttr", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  parent_memo_->UpsertVersion(ino, response.inode().version());

  attr_entry.Swap(response.mutable_inode());

  return Status::OK();
}

Status MDSClient::SetAttr(ContextSPtr& ctx, Ino ino, const Attr& attr,
                          int to_set, AttrEntry& attr_entry,
                          bool& shrink_file) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return mds::IsDir(ino) ? GetMdsByParent(ino, is_primary_mds)
                           : GetMds(ino, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::SetAttr", ctx->GetTraceSpan());

  pb::mds::SetAttrRequest request;
  pb::mds::SetAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  uint32_t temp_to_set = 0;
  if (to_set & kSetAttrMode) {
    request.set_mode(attr.mode);
    temp_to_set |= kSetAttrMode;
  }
  if (to_set & kSetAttrUid) {
    request.set_uid(attr.uid);
    temp_to_set |= kSetAttrUid;
  }
  if (to_set & kSetAttrGid) {
    request.set_gid(attr.gid);
    temp_to_set |= kSetAttrGid;
  }

  struct timespec now;
  CHECK(clock_gettime(CLOCK_REALTIME, &now) == 0) << "get current time fail.";

  if (to_set & kSetAttrAtime) {
    request.set_atime(attr.atime);
    temp_to_set |= kSetAttrAtime;

  } else if (to_set & kSetAttrAtimeNow) {
    request.set_atime(ToTimestamp(now));
    temp_to_set |= kSetAttrAtime;
  }

  if (to_set & kSetAttrMtime) {
    request.set_mtime(attr.mtime);
    temp_to_set |= kSetAttrMtime;

  } else if (to_set & kSetAttrMtimeNow) {
    request.set_mtime(ToTimestamp(now));
    temp_to_set |= kSetAttrMtime;
  }

  if (to_set & kSetAttrCtime) {
    request.set_ctime(attr.ctime);
    temp_to_set |= kSetAttrCtime;
  } else {
    // always update ctime
    request.set_ctime(ToTimestamp(now));
    temp_to_set |= kSetAttrCtime;
  }

  if (to_set & kSetAttrSize) {
    request.set_length(attr.length);
    temp_to_set |= kSetAttrSize;
  }

  if (to_set & kSetAttrFlags) {
    request.set_flags(attr.flags);
    temp_to_set |= kSetAttrFlags;
  }

  request.set_to_set(temp_to_set);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "SetAttr", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  parent_memo_->UpsertVersion(ino, response.inode().version());

  attr_entry.Swap(response.mutable_inode());
  shrink_file = response.shrink_file();

  return Status::OK();
}

Status MDSClient::GetXAttr(ContextSPtr& ctx, Ino ino, const std::string& name,
                           std::string& value) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return mds::IsDir(ino) ? GetMdsByParent(ino, is_primary_mds)
                           : GetMds(ino, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::GetXAttr", ctx->GetTraceSpan());

  pb::mds::GetXAttrRequest request;
  pb::mds::GetXAttrResponse response;

  auto* mut_ctx = request.mutable_context();
  mut_ctx->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_name(name);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "GetXAttr", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  value = response.value();

  return Status::OK();
}

Status MDSClient::SetXAttr(ContextSPtr& ctx, Ino ino, const std::string& name,
                           const std::string& value, AttrEntry& attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return mds::IsDir(ino) ? GetMdsByParent(ino, is_primary_mds)
                           : GetMds(ino, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::SetXAttr", ctx->GetTraceSpan());

  pb::mds::SetXAttrRequest request;
  pb::mds::SetXAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.mutable_xattrs()->insert({name, value});
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "SetXAttr", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  parent_memo_->UpsertVersion(ino, response.inode().version());

  attr_entry.Swap(response.mutable_inode());

  return Status::OK();
}

Status MDSClient::RemoveXAttr(ContextSPtr& ctx, Ino ino,
                              const std::string& name, AttrEntry& attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return mds::IsDir(ino) ? GetMdsByParent(ino, is_primary_mds)
                           : GetMds(ino, is_primary_mds);
  };

  auto span = trace_manager_.StartChildSpan("MDSClient::RemoveXAttr",
                                            ctx->GetTraceSpan());

  pb::mds::RemoveXAttrRequest request;
  pb::mds::RemoveXAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_name(name);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "RemoveXAttr", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  parent_memo_->UpsertVersion(ino, response.inode().version());

  attr_entry.Swap(response.mutable_inode());

  return Status::OK();
}

Status MDSClient::ListXAttr(ContextSPtr& ctx, Ino ino,
                            std::map<std::string, std::string>& xattrs) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return mds::IsDir(ino) ? GetMdsByParent(ino, is_primary_mds)
                           : GetMds(ino, is_primary_mds);
  };

  auto span = trace_manager_.StartChildSpan("MDSClient::ListXAttr",
                                            ctx->GetTraceSpan());
  pb::mds::ListXAttrRequest request;
  pb::mds::ListXAttrResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "ListXAttr", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  for (const auto& [name, value] : response.xattrs()) {
    xattrs[name] = value;
  }

  return Status::OK();
}

Status MDSClient::Rename(ContextSPtr& ctx, Ino old_parent,
                         const std::string& old_name, Ino new_parent,
                         const std::string& new_name,
                         std::vector<Ino>& effected_inos) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, new_parent](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(new_parent, is_primary_mds);
  };

  auto span =
      trace_manager_.StartChildSpan("MDSClient::Rename", ctx->GetTraceSpan());

  pb::mds::RenameRequest request;
  pb::mds::RenameResponse response;

  if (fs_info_->IsHashPartition()) {
    auto old_ancestors = parent_memo_->GetAncestors(old_parent);
    for (auto& ancestor : old_ancestors) {
      request.add_old_ancestors(ancestor);
    }

    auto new_ancestors = parent_memo_->GetAncestors(new_parent);
    for (auto& ancestor : new_ancestors) {
      request.add_new_ancestors(ancestor);
    }
  }

  request.set_fs_id(fs_id_);
  request.set_old_parent(old_parent);
  request.set_old_name(old_name);
  request.set_new_parent(new_parent);
  request.set_new_name(new_name);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "Rename", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  parent_memo_->UpsertVersionAndRenameRefCount(old_parent,
                                               response.old_parent_version());
  parent_memo_->UpsertVersionAndRenameRefCount(new_parent,
                                               response.new_parent_version());

  effected_inos = mds::Helper::PbRepeatedToVector(response.effected_inos());

  return Status::OK();
}

Status MDSClient::NewSliceId(ContextSPtr& ctx, uint32_t num, uint64_t* id) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(kRootIno, is_primary_mds);
  };

  auto span = trace_manager_.StartChildSpan("MDSClient::NewSliceId",
                                            ctx->GetTraceSpan());

  pb::mds::AllocSliceIdRequest request;
  pb::mds::AllocSliceIdResponse response;
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  request.set_alloc_num(num);

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "AllocSliceId", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  *id = response.slice_id();

  return Status::OK();
}

Status MDSClient::ReadSlice(
    ContextSPtr& ctx, Ino ino,
    const std::vector<ChunkDescriptor>& chunk_descriptors,
    std::vector<mds::ChunkEntry>& chunks) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  CHECK(ino != 0) << "ino is zero.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return GetMds(ino, is_primary_mds);
  };

  auto span = trace_manager_.StartChildSpan("MDSClient::ReadSlice",
                                            ctx->GetTraceSpan());

  pb::mds::ReadSliceRequest request;
  pb::mds::ReadSliceResponse response;

  auto* mut_ctx = request.mutable_context();
  mut_ctx->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  mds::Helper::VectorToPbRepeated(chunk_descriptors,
                                  request.mutable_chunk_descriptors());

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "ReadSlice", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  chunks = mds::Helper::PbRepeatedToVector(*response.mutable_chunks());

  return Status::OK();
}

Status MDSClient::WriteSlice(
    ContextSPtr& ctx, Ino ino,
    const std::vector<mds::DeltaSliceEntry>& delta_slices,
    std::vector<ChunkDescriptor>& chunk_descriptors) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  CHECK(ctx != nullptr) << "context is nullptr.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return GetMds(ino, is_primary_mds);
  };

  auto span = trace_manager_.StartChildSpan("MDSClient::WriteSlice",
                                            ctx->GetTraceSpan());
  pb::mds::WriteSliceRequest request;
  pb::mds::WriteSliceResponse response;

  SetAncestorInContext(request, ino);

  Ino parent = 0;
  CHECK(parent_memo_->GetParent(ino, parent))
      << "get parent fail from parent cache.";

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_ino(ino);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  mds::Helper::VectorToPbRepeated(delta_slices, request.mutable_delta_slices());

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "WriteSlice", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  chunk_descriptors =
      mds::Helper::PbRepeatedToVector(response.chunk_descriptors());

  return Status::OK();
}

Status MDSClient::Fallocate(ContextSPtr& ctx, Ino ino, int32_t mode,
                            uint64_t offset, uint64_t length) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return GetMds(ino, is_primary_mds);
  };

  auto span = trace_manager_.StartChildSpan("MDSClient::Fallocate",
                                            ctx->GetTraceSpan());

  pb::mds::FallocateRequest request;
  pb::mds::FallocateResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_mode(mode);
  request.set_offset(offset);
  request.set_len(length);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "Fallocate", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  const auto& attr = response.inode();

  parent_memo_->UpsertVersion(attr.ino(), attr.version());

  return Status::OK();
}

Status MDSClient::GetFsQuota(ContextSPtr& ctx, FsStat& fs_stat) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto span = trace_manager_.StartChildSpan("MDSClient::GetFsQuota",
                                            ctx->GetTraceSpan());

  pb::mds::GetFsQuotaRequest request;
  pb::mds::GetFsQuotaResponse response;

  request.set_fs_id(fs_id_);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status =
      rpc_->SendRequest("MDSService", "GetFsQuota", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  const auto& quota = response.quota();
  fs_stat.max_bytes = quota.max_bytes();
  fs_stat.used_bytes = quota.used_bytes();
  fs_stat.max_inodes = quota.max_inodes();
  fs_stat.used_inodes = quota.used_inodes();

  return Status::OK();
}

Status MDSClient::GetDirQuota(ContextSPtr& ctx, Ino ino, FsStat& fs_stat) {
  auto get_mds_fn = [this, ino](bool& is_primary_mds) -> MDSMeta {
    return GetMdsByParent(ino, is_primary_mds);
  };

  auto span = trace_manager_.StartChildSpan("MDSClient::GetDirQuota",
                                            ctx->GetTraceSpan());

  pb::mds::GetDirQuotaRequest request;
  pb::mds::GetDirQuotaResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.mutable_info()->set_trace_id(SpanScope::GetTraceID(span));
  request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));

  auto status = SendRequest(SpanScope::GetContext(span), get_mds_fn,
                            "MDSService", "GetDirQuota", request, response);
  if (!status.ok()) {
    SpanScope::SetStatus(span, status);
    return status;
  }

  const auto& quota = response.quota();
  fs_stat.max_bytes = quota.max_bytes();
  fs_stat.used_bytes = quota.used_bytes();
  fs_stat.max_inodes = quota.max_inodes();
  fs_stat.used_inodes = quota.used_inodes();

  return Status::OK();
}

bool MDSClient::UpdateRouter() {
  mds::FsInfoEntry new_fs_info;
  auto status = MDSClient::GetFsInfo(rpc_, fs_info_->GetName(), new_fs_info);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.client] get fs info fail, {}.",
                              status.ToString());
    return false;
  }

  epoch_ = new_fs_info.partition_policy().epoch();

  fs_info_->Update(new_fs_info);

  if (!mds_discovery_->RefreshFullyMDSList()) {
    LOG(ERROR) << "[meta.client] refresh mds discovery fail.";
    return false;
  }

  if (!mds_router_->UpdateRouter(new_fs_info.partition_policy())) {
    LOG(ERROR) << "[meta.client] update mds router fail.";
    return false;
  }

  return true;
}

// process epoch change
// 1. updatge fs info
// 2. update mds router
void MDSClient::ProcessEpochChange() {
  if (!UpdateRouter()) {
    LOG(ERROR) << "[meta.client] process epoch change fail.";
  }
}

void MDSClient::ProcessNotServe() {
  if (!UpdateRouter()) {
    LOG(ERROR) << "[meta.client] process not serve fail.";
  }
}

void MDSClient::ProcessNetError(MDSMeta& mds_meta) {
  // set the current mds as abnormal
  mds_discovery_->SetAbnormalMDS(mds_meta.ID());

  // get a normal mds
  auto mdses = mds_discovery_->GetNormalMDS();
  for (auto& mds : mdses) {
    if (mds.ID() != mds_meta.ID()) {
      LOG(INFO) << fmt::format(
          "[meta.client] process net error, transfer {}->{}.", mds_meta.ID(),
          mds.ID());
      mds_meta = mds;
      break;
    }
  }
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs