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

#include "client/vfs/meta/v2/mds_client.h"

#include <cstdint>
#include <string>

#include "client/meta/vfs_meta.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/meta/v2/helper.h"
#include "client/vfs/meta/v2/rpc.h"
#include "dingofs/mdsv2.pb.h"
#include "dingofs/metaserver.pb.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mdsv2/common/constant.h"
#include "mdsv2/common/type.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

MDSClient::MDSClient(const ClientId& client_id, mdsv2::FsInfoSPtr fs_info,
                     ParentMemoSPtr parent_memo, MDSDiscoverySPtr mds_discovery,
                     MDSRouterPtr mds_router, RPCPtr rpc)
    : client_id_(client_id),
      fs_info_(fs_info),
      fs_id_(fs_info->GetFsId()),
      epoch_(fs_info->GetEpoch()),
      parent_memo_(parent_memo),
      mds_discovery_(mds_discovery),
      mds_router_(mds_router),
      rpc_(rpc) {}

bool MDSClient::Init() {
  CHECK(parent_memo_ != nullptr) << "parent cache is null.";
  CHECK(mds_discovery_ != nullptr) << "mds discovery is null.";
  CHECK(mds_router_ != nullptr) << "mds router is null.";
  CHECK(rpc_ != nullptr) << "rpc is null.";

  return true;
}

void MDSClient::Destory() {}

bool MDSClient::Dump(Json::Value& value) { return parent_memo_->Dump(value); }

bool MDSClient::Load(const Json::Value& value) {
  return parent_memo_->Load(value);
}

bool MDSClient::SetEndpoint(const std::string& ip, int port) {
  return rpc_->AddEndpoint(ip, port);
}

Status MDSClient::DoGetFsInfo(RPCPtr rpc, pb::mdsv2::GetFsInfoRequest& request,
                              mdsv2::FsInfoEntry& fs_info) {
  pb::mdsv2::GetFsInfoResponse response;

  auto status = rpc->SendRequest("MDSService", "GetFsInfo", request, response);
  if (status.ok()) {
    fs_info = response.fs_info();
  }
  return status;
}

Status MDSClient::GetFsInfo(RPCPtr rpc, const std::string& name,
                            mdsv2::FsInfoEntry& fs_info) {
  pb::mdsv2::GetFsInfoRequest request;
  request.set_fs_name(name);
  return DoGetFsInfo(rpc, request, fs_info);
}

Status MDSClient::GetFsInfo(RPCPtr rpc, uint32_t fs_id,
                            mdsv2::FsInfoEntry& fs_info) {
  pb::mdsv2::GetFsInfoRequest request;
  request.set_fs_id(fs_id);
  return DoGetFsInfo(rpc, request, fs_info);
}

RPCPtr MDSClient::GetRpc() { return rpc_; }

Status MDSClient::Heartbeat() {
  auto get_mds_fn = [this]() -> MDSMeta {
    mdsv2::MDSMeta mds_meta;
    mds_discovery_->PickFirstMDS(mds_meta);
    return mds_meta;
  };

  pb::mdsv2::HeartbeatRequest request;
  pb::mdsv2::HeartbeatResponse response;

  request.set_role(pb::mdsv2::ROLE_CLIENT);
  auto* client = request.mutable_client();
  client->set_id(client_id_.ID());
  client->set_hostname(client_id_.Hostname());
  client->set_ip(client_id_.IP());
  client->set_port(client_id_.Port());
  client->set_mountpoint(client_id_.Mountpoint());
  client->set_fs_name(fs_info_->GetName());

  auto status = SendRequest(nullptr, get_mds_fn, "MDSService", "Heartbeat",
                            request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::MountFs(const std::string& name,
                          const pb::mdsv2::MountPoint& mount_point) {
  pb::mdsv2::MountFsRequest request;
  pb::mdsv2::MountFsResponse response;

  request.set_fs_name(name);
  request.mutable_mount_point()->CopyFrom(mount_point);

  auto status = rpc_->SendRequest("MDSService", "MountFs", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::UmountFs(const std::string& name,
                           const std::string& client_id) {
  pb::mdsv2::UmountFsRequest request;
  pb::mdsv2::UmountFsResponse response;

  request.set_fs_name(name);
  request.set_client_id(client_id);

  auto status = rpc_->SendRequest("MDSService", "UmountFs", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

MDSMeta MDSClient::GetMds(Ino ino) {
  mdsv2::MDSMeta mds_meta;
  CHECK(mds_router_->GetMDS(ino, mds_meta))
      << fmt::format("get mds fail for ino({}).", ino);

  VLOG(1) << fmt::format("[meta.client] query mds({}|{}:{}) for ino({}).",
                         mds_meta.ID(), mds_meta.Host(), mds_meta.Port(), ino);

  return mds_meta;
}

MDSMeta MDSClient::GetMdsByParent(int64_t parent) {
  mdsv2::MDSMeta mds_meta;
  CHECK(mds_router_->GetMDSByParent(parent, mds_meta))
      << fmt::format("get mds fail for parent({}).", parent);

  VLOG(1) << fmt::format("[meta.client] query mds({}|{}:{}) for parent({}).",
                         mds_meta.ID(), mds_meta.Host(), mds_meta.Port(),
                         parent);
  return mds_meta;
}

MDSMeta MDSClient::GetMdsWithFallback(Ino ino, bool& is_fallback) {
  is_fallback = false;
  mdsv2::MDSMeta mds_meta;
  if (!mds_router_->GetMDS(ino, mds_meta)) {
    CHECK(mds_router_->GetRandomlyMDS(mds_meta))
        << fmt::format("get randomly mds fail for ino({}).", ino);
    is_fallback = true;
  }

  VLOG(1) << fmt::format("[meta.client] query mds({}|{}:{}) for ino({}).",
                         mds_meta.ID(), mds_meta.Host(), mds_meta.Port(), ino);
  return mds_meta;
}

MDSMeta MDSClient::GetMdsByParentWithFallback(int64_t parent,
                                              bool& is_fallback) {
  is_fallback = false;
  mdsv2::MDSMeta mds_meta;
  if (!mds_router_->GetMDSByParent(parent, mds_meta)) {
    CHECK(mds_router_->GetRandomlyMDS(mds_meta))
        << fmt::format("get randomly mds fail for ino({}).", parent);
    is_fallback = true;
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

Status MDSClient::Lookup(ContextSPtr ctx, Ino parent, const std::string& name,
                         Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent]() -> MDSMeta {
    return GetMdsByParent(parent);
  };

  pb::mdsv2::LookupRequest request;
  pb::mdsv2::LookupResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent));

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "Lookup", request, response);
  if (!status.ok()) {
    return status;
  }

  const auto& inode = response.inode();

  if (fs_info_->IsHashPartition() && mdsv2::IsDir(inode.ino())) {
    uint64_t last_version;
    if (parent_memo_->GetVersion(inode.ino(), last_version) &&
        inode.version() < last_version) {
      // fetch last inode
      status = GetAttr(ctx, inode.ino(), out_attr);
      if (status.ok()) {
        parent_memo_->Upsert(out_attr.ino, parent);
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

  out_attr = Helper::ToAttr(inode);

  return Status::OK();
}

Status MDSClient::MkNod(ContextSPtr ctx, Ino parent, const std::string& name,
                        uint32_t uid, uint32_t gid, mode_t mode, dev_t rdev,
                        Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent]() -> MDSMeta {
    return GetMdsByParent(parent);
  };

  pb::mdsv2::MkNodRequest request;
  pb::mdsv2::MkNodResponse response;

  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);
  request.set_mode(mode);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_rdev(rdev);

  request.set_length(0);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "MkNod", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->Upsert(response.inode().ino(), parent,
                       response.inode().version());
  parent_memo_->UpsertVersion(parent, response.parent_version());

  out_attr = Helper::ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::MkDir(ContextSPtr ctx, Ino parent, const std::string& name,
                        uint32_t uid, uint32_t gid, mode_t mode, dev_t rdev,
                        Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent]() -> MDSMeta {
    return GetMdsByParent(parent);
  };

  pb::mdsv2::MkDirRequest request;
  pb::mdsv2::MkDirResponse response;

  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);
  request.set_mode(mode);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_rdev(rdev);

  request.set_length(0);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "MkDir", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->Upsert(response.inode().ino(), parent,
                       response.inode().version());
  parent_memo_->UpsertVersion(parent, response.parent_version());

  out_attr = Helper::ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::RmDir(ContextSPtr ctx, Ino parent, const std::string& name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent]() -> MDSMeta {
    return GetMdsByParent(parent);
  };

  pb::mdsv2::RmDirRequest request;
  pb::mdsv2::RmDirResponse response;

  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "RmDir", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::ReadDir(ContextSPtr ctx, Ino ino,
                          const std::string& last_name, uint32_t limit,
                          bool with_attr, std::vector<DirEntry>& entries) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino]() -> MDSMeta { return GetMdsByParent(ino); };

  pb::mdsv2::ReadDirRequest request;
  pb::mdsv2::ReadDirResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_last_name(last_name);
  request.set_limit(limit);
  request.set_with_attr(with_attr);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "ReadDir", request, response);
  if (!status.ok()) {
    return status;
  }

  entries.reserve(response.entries_size());
  for (const auto& entry : response.entries()) {
    parent_memo_->Upsert(entry.ino(), ino, entry.inode().version());
    entries.push_back(Helper::ToDirEntry(entry));
  }

  return Status::OK();
}

Status MDSClient::Open(ContextSPtr ctx, Ino ino, int flags,
                       bool is_prefetch_chunk, std::string& session_id,
                       AttrEntry& attr_entry,
                       std::vector<mdsv2::ChunkEntry>& chunks) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino]() -> MDSMeta { return GetMds(ino); };

  pb::mdsv2::OpenRequest request;
  pb::mdsv2::OpenResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_flags(flags);
  request.set_prefetch_chunk(is_prefetch_chunk);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "Open", request, response);
  if (!status.ok()) {
    return status;
  }

  session_id = response.session_id();
  chunks = mdsv2::Helper::PbRepeatedToVector(response.chunks());
  attr_entry.Swap(response.mutable_inode());

  parent_memo_->UpsertVersion(ino, response.inode().version());

  return Status::OK();
}

Status MDSClient::Release(ContextSPtr ctx, Ino ino,
                          const std::string& session_id) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino]() -> MDSMeta { return GetMds(ino); };

  pb::mdsv2::ReleaseRequest request;
  pb::mdsv2::ReleaseResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_session_id(session_id);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "Release", request, response);

  return status;
}

Status MDSClient::Link(ContextSPtr ctx, Ino ino, Ino new_parent,
                       const std::string& new_name, Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, new_parent]() -> MDSMeta {
    return GetMdsByParent(new_parent);
  };

  pb::mdsv2::LinkRequest request;
  pb::mdsv2::LinkResponse response;

  SetAncestorInContext(request, new_parent);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_new_parent(new_parent);
  request.set_new_name(new_name);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "Link", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->Upsert(response.inode().ino(), new_parent,
                       response.inode().version());
  parent_memo_->UpsertVersion(new_parent, response.parent_version());

  out_attr = Helper::ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::UnLink(ContextSPtr ctx, Ino parent, const std::string& name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent]() -> MDSMeta {
    return GetMdsByParent(parent);
  };

  pb::mdsv2::UnLinkRequest request;
  pb::mdsv2::UnLinkResponse response;

  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "UnLink", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::Symlink(ContextSPtr ctx, Ino parent, const std::string& name,
                          uint32_t uid, uint32_t gid,
                          const std::string& symlink, Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, parent]() -> MDSMeta {
    return GetMdsByParent(parent);
  };

  pb::mdsv2::SymlinkRequest request;
  pb::mdsv2::SymlinkResponse response;

  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_symlink(symlink);

  request.set_new_parent(parent);
  request.set_new_name(name);
  request.set_uid(uid);
  request.set_gid(gid);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "Symlink", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->Upsert(response.inode().ino(), parent,
                       response.inode().version());

  parent_memo_->UpsertVersion(parent, response.parent_version());

  out_attr = Helper::ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::ReadLink(ContextSPtr ctx, Ino ino, std::string& symlink) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino]() -> MDSMeta { return GetMds(ino); };

  pb::mdsv2::ReadLinkRequest request;
  pb::mdsv2::ReadLinkResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "ReadLink", request, response);
  if (!status.ok()) {
    return status;
  }

  symlink = response.symlink();

  return Status::OK();
}

Status MDSClient::GetAttr(ContextSPtr ctx, Ino ino, Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  bool is_fallback = false;
  auto get_mds_fn = [this, ino, &is_fallback]() -> MDSMeta {
    return mdsv2::IsDir(ino) ? GetMdsByParentWithFallback(ino, is_fallback)
                             : GetMdsWithFallback(ino, is_fallback);
  };

  pb::mdsv2::GetAttrRequest request;
  pb::mdsv2::GetAttrResponse response;

  auto* mut_ctx = request.mutable_context();
  mut_ctx->set_is_bypass_cache(is_fallback);
  mut_ctx->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "GetAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->UpsertVersion(ino, response.inode().version());

  out_attr = Helper::ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::SetAttr(ContextSPtr ctx, Ino ino, const Attr& attr,
                          int to_set, Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino]() -> MDSMeta {
    return mdsv2::IsDir(ino) ? GetMdsByParent(ino) : GetMds(ino);
  };

  pb::mdsv2::SetAttrRequest request;
  pb::mdsv2::SetAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  uint32_t temp_to_set = 0;
  if (to_set & kSetAttrMode) {
    request.set_mode(attr.mode);
    temp_to_set |= mdsv2::kSetAttrMode;
  }
  if (to_set & kSetAttrUid) {
    request.set_uid(attr.uid);
    temp_to_set |= mdsv2::kSetAttrUid;
  }
  if (to_set & kSetAttrGid) {
    request.set_gid(attr.gid);
    temp_to_set |= mdsv2::kSetAttrGid;
  }

  struct timespec now;
  CHECK(clock_gettime(CLOCK_REALTIME, &now) == 0) << "get current time fail.";

  if (to_set & kSetAttrAtime) {
    request.set_atime(attr.atime);
    temp_to_set |= mdsv2::kSetAttrAtime;

  } else if (to_set & kSetAttrAtimeNow) {
    request.set_atime(ToTimestamp(now));
    temp_to_set |= mdsv2::kSetAttrAtime;
  }

  if (to_set & kSetAttrMtime) {
    request.set_mtime(attr.mtime);
    temp_to_set |= mdsv2::kSetAttrMtime;

  } else if (to_set & kSetAttrMtimeNow) {
    request.set_mtime(ToTimestamp(now));
    temp_to_set |= mdsv2::kSetAttrMtime;
  }

  if (to_set & kSetAttrCtime) {
    request.set_ctime(attr.ctime);
    temp_to_set |= mdsv2::kSetAttrCtime;
  } else {
    request.set_ctime(ToTimestamp(now));
    temp_to_set |= mdsv2::kSetAttrCtime;
  }

  if (to_set & kSetAttrSize) {
    request.set_length(attr.length);
    temp_to_set |= mdsv2::kSetAttrLength;
  }

  request.set_to_set(temp_to_set);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "SetAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  out_attr = Helper::ToAttr(response.inode());

  parent_memo_->UpsertVersion(ino, response.inode().version());

  return Status::OK();
}

Status MDSClient::GetXAttr(ContextSPtr ctx, Ino ino, const std::string& name,
                           std::string& value) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  bool is_fallback = false;
  auto get_mds_fn = [this, ino, &is_fallback]() -> MDSMeta {
    return mdsv2::IsDir(ino) ? GetMdsByParentWithFallback(ino, is_fallback)
                             : GetMdsWithFallback(ino, is_fallback);
  };

  pb::mdsv2::GetXAttrRequest request;
  pb::mdsv2::GetXAttrResponse response;

  auto* mut_ctx = request.mutable_context();
  mut_ctx->set_is_bypass_cache(is_fallback);
  mut_ctx->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_name(name);

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "GetXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  value = response.value();

  return Status::OK();
}

Status MDSClient::SetXAttr(ContextSPtr ctx, Ino ino, const std::string& name,
                           const std::string& value, AttrEntry& attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino]() -> MDSMeta {
    return mdsv2::IsDir(ino) ? GetMdsByParent(ino) : GetMds(ino);
  };

  pb::mdsv2::SetXAttrRequest request;
  pb::mdsv2::SetXAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.mutable_xattrs()->insert({name, value});

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "SetXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->UpsertVersion(ino, response.inode().version());

  attr_entry.Swap(response.mutable_inode());

  return Status::OK();
}

Status MDSClient::RemoveXAttr(ContextSPtr ctx, Ino ino, const std::string& name,
                              AttrEntry& attr_entry) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino]() -> MDSMeta {
    return mdsv2::IsDir(ino) ? GetMdsByParent(ino) : GetMds(ino);
  };

  pb::mdsv2::RemoveXAttrRequest request;
  pb::mdsv2::RemoveXAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_name(name);

  auto status = SendRequest(ctx, get_mds_fn, "MDSService", "RemoveXAttr",
                            request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->UpsertVersion(ino, response.inode().version());

  attr_entry.Swap(response.mutable_inode());

  return Status::OK();
}

Status MDSClient::ListXAttr(ContextSPtr ctx, Ino ino,
                            std::map<std::string, std::string>& xattrs) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino]() -> MDSMeta {
    return mdsv2::IsDir(ino) ? GetMdsByParent(ino) : GetMds(ino);
  };

  pb::mdsv2::ListXAttrRequest request;
  pb::mdsv2::ListXAttrResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status = SendRequest(ctx, get_mds_fn, "MDSService", "ListXAttr", request,
                            response);
  if (!status.ok()) {
    return status;
  }

  for (const auto& [name, value] : response.xattrs()) {
    xattrs[name] = value;
  }

  return Status::OK();
}

Status MDSClient::Rename(ContextSPtr ctx, Ino old_parent,
                         const std::string& old_name, Ino new_parent,
                         const std::string& new_name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, new_parent]() -> MDSMeta {
    return GetMdsByParent(new_parent);
  };

  pb::mdsv2::RenameRequest request;
  pb::mdsv2::RenameResponse response;

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

  auto status =
      SendRequest(ctx, get_mds_fn, "MDSService", "Rename", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->UpsertVersion(old_parent, response.old_parent_version());
  parent_memo_->UpsertVersion(new_parent, response.new_parent_version());

  return Status::OK();
}

Status MDSClient::NewSliceId(ContextSPtr ctx, uint32_t num, uint64_t* id) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this]() -> MDSMeta {
    mdsv2::MDSMeta mds_meta;
    mds_discovery_->PickFirstMDS(mds_meta);
    return mds_meta;
  };

  pb::mdsv2::AllocSliceIdRequest request;
  pb::mdsv2::AllocSliceIdResponse response;

  request.set_alloc_num(num);

  auto status = SendRequest(ctx, get_mds_fn, "MDSService", "AllocSliceId",
                            request, response);
  if (!status.ok()) {
    return status;
  }

  *id = response.slice_id();

  return Status::OK();
}

Status MDSClient::ReadSlice(ContextSPtr ctx, Ino ino,
                            const std::vector<uint64_t>& chunk_indexes,
                            std::vector<mdsv2::ChunkEntry>& chunks) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  CHECK(ino != 0) << "ino is zero.";

  bool is_fallback = false;
  auto get_mds_fn = [this, ino, &is_fallback]() -> MDSMeta {
    return GetMdsWithFallback(ino, is_fallback);
  };

  pb::mdsv2::ReadSliceRequest request;
  pb::mdsv2::ReadSliceResponse response;

  auto* mut_ctx = request.mutable_context();
  mut_ctx->set_is_bypass_cache(is_fallback);
  mut_ctx->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  mdsv2::Helper::VectorToPbRepeated(chunk_indexes,
                                    request.mutable_chunk_indexes());

  auto status = SendRequest(ctx, get_mds_fn, "MDSService", "ReadSlice", request,
                            response);
  if (!status.ok()) {
    return status;
  }

  chunks = mdsv2::Helper::PbRepeatedToVector(*response.mutable_chunks());

  return Status::OK();
}

Status MDSClient::WriteSlice(
    ContextSPtr ctx, Ino ino,
    const std::vector<mdsv2::DeltaSliceEntry>& delta_slices) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino]() -> MDSMeta { return GetMds(ino); };

  pb::mdsv2::WriteSliceRequest request;
  pb::mdsv2::WriteSliceResponse response;

  SetAncestorInContext(request, ino);

  Ino parent = 0;
  CHECK(parent_memo_->GetParent(ino, parent))
      << "get parent fail from parent cache.";

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_ino(ino);
  mdsv2::Helper::VectorToPbRepeated(delta_slices,
                                    request.mutable_delta_slices());

  auto status = SendRequest(ctx, get_mds_fn, "MDSService", "WriteSlice",
                            request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::Fallocate(ContextSPtr ctx, Ino ino, int32_t mode,
                            uint64_t offset, uint64_t length) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_mds_fn = [this, ino]() -> MDSMeta { return GetMds(ino); };

  pb::mdsv2::FallocateRequest request;
  pb::mdsv2::FallocateResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_mode(mode);
  request.set_offset(offset);
  request.set_len(length);

  auto status = SendRequest(ctx, get_mds_fn, "MDSService", "Fallocate", request,
                            response);
  if (!status.ok()) {
    return status;
  }

  const auto& attr = response.inode();

  parent_memo_->UpsertVersion(attr.ino(), attr.version());

  return Status::OK();
}

Status MDSClient::GetFsQuota(ContextSPtr ctx, FsStat& fs_stat) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  pb::mdsv2::GetFsQuotaRequest request;
  pb::mdsv2::GetFsQuotaResponse response;

  request.set_fs_id(fs_id_);
  request.mutable_info()->set_request_id(ctx->TraceId());

  auto status =
      rpc_->SendRequest("MDSService", "GetFsQuota", request, response);
  if (!status.ok()) {
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
  mdsv2::FsInfoEntry new_fs_info;
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

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs