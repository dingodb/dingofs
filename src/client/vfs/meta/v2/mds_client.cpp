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
#include <utility>

#include "client/meta/vfs_meta.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/meta/v2/rpc.h"
#include "dingofs/mdsv2.pb.h"
#include "dingofs/metaserver.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mdsv2/common/constant.h"
#include "mdsv2/common/type.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

static FileType ToFileType(pb::mdsv2::FileType type) {
  switch (type) {
    case pb::mdsv2::FileType::FILE:
      return FileType::kFile;

    case pb::mdsv2::FileType::DIRECTORY:
      return FileType::kDirectory;

    case pb::mdsv2::FileType::SYM_LINK:
      return FileType::kSymlink;

    default:
      CHECK(false) << "unknown file type: " << type;
  }
}

static Attr ToAttr(const pb::mdsv2::Inode& inode) {
  Attr out_attr;

  out_attr.ino = inode.ino();
  out_attr.mode = inode.mode();
  out_attr.nlink = inode.nlink();
  out_attr.uid = inode.uid();
  out_attr.gid = inode.gid();
  out_attr.length = inode.length();
  out_attr.rdev = inode.rdev();
  out_attr.atime = inode.atime();
  out_attr.mtime = inode.mtime();
  out_attr.ctime = inode.ctime();
  out_attr.type = ToFileType(inode.type());

  return out_attr;
}

static DirEntry ToDirEntry(const pb::mdsv2::ReadDirResponse::Entry& entry) {
  DirEntry out_entry;
  out_entry.name = entry.name();
  out_entry.ino = entry.ino();
  out_entry.attr = ToAttr(entry.inode());

  return std::move(out_entry);
}

MDSClient::MDSClient(const ClientId& client_id, mdsv2::FsInfoPtr fs_info,
                     ParentMemoSPtr parent_memo, MDSDiscoveryPtr mds_discovery,
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

bool MDSClient::SetEndpoint(const std::string& ip, int port, bool is_default) {
  return rpc_->AddEndpoint(ip, port, is_default);
}

Status MDSClient::DoGetFsInfo(RPCPtr rpc, pb::mdsv2::GetFsInfoRequest& request,
                              pb::mdsv2::FsInfo& fs_info) {
  pb::mdsv2::GetFsInfoResponse response;

  auto status = rpc->SendRequest("MDSService", "GetFsInfo", request, response);
  if (status.ok()) {
    fs_info = response.fs_info();
  }
  return status;
}

Status MDSClient::GetFsInfo(RPCPtr rpc, const std::string& name,
                            pb::mdsv2::FsInfo& fs_info) {
  pb::mdsv2::GetFsInfoRequest request;
  request.set_fs_name(name);
  return DoGetFsInfo(rpc, request, fs_info);
}

Status MDSClient::GetFsInfo(RPCPtr rpc, uint32_t fs_id,
                            pb::mdsv2::FsInfo& fs_info) {
  pb::mdsv2::GetFsInfoRequest request;
  request.set_fs_id(fs_id);
  return DoGetFsInfo(rpc, request, fs_info);
}

Status MDSClient::Heartbeat() {
  pb::mdsv2::HeartbeatRequest request;
  pb::mdsv2::HeartbeatResponse response;

  request.set_role(pb::mdsv2::ROLE_CLIENT);
  auto* client = request.mutable_client();
  client->set_id(client_id_.ID());
  client->set_hostname(client_id_.Hostname());
  client->set_port(client_id_.Port());
  client->set_mountpoint(client_id_.Mountpoint());

  auto status = rpc_->SendRequest("MDSService", "Heartbeat", request, response);
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

EndPoint MDSClient::GetEndpoint(Ino ino) {
  mdsv2::MDSMeta mds_meta;
  CHECK(mds_router_->GetMDS(ino, mds_meta))
      << fmt::format("get mds fail for ino({}).", ino);

  LOG(INFO) << fmt::format("[meta] query target mds({}:{}) for ino({}).",
                           mds_meta.Host(), mds_meta.Port(), ino);
  return StrToEndpoint(mds_meta.Host(), mds_meta.Port());
}

EndPoint MDSClient::GetEndpointByParent(int64_t parent) {
  mdsv2::MDSMeta mds_meta;
  CHECK(mds_router_->GetMDSByParent(parent, mds_meta))
      << fmt::format("get mds fail for parent({}).", parent);

  LOG(INFO) << fmt::format("[meta] query target mds({}:{}) for parent({}).",
                           mds_meta.Host(), mds_meta.Port(), parent);
  return StrToEndpoint(mds_meta.Host(), mds_meta.Port());
}

EndPoint MDSClient::GetEndpointWithFallback(Ino ino, bool& is_fallback) {
  is_fallback = false;
  mdsv2::MDSMeta mds_meta;
  if (!mds_router_->GetMDS(ino, mds_meta)) {
    CHECK(mds_router_->GetRandomlyMDS(mds_meta))
        << fmt::format("get randomly mds fail for ino({}).", ino);
    is_fallback = true;
  }

  LOG(INFO) << fmt::format("[meta] query target mds({}:{}) for ino({}).",
                           mds_meta.Host(), mds_meta.Port(), ino);
  return StrToEndpoint(mds_meta.Host(), mds_meta.Port());
}

EndPoint MDSClient::GetEndpointByParentWithFallback(int64_t parent,
                                                    bool& is_fallback) {
  is_fallback = false;
  mdsv2::MDSMeta mds_meta;
  if (!mds_router_->GetMDSByParent(parent, mds_meta)) {
    CHECK(mds_router_->GetRandomlyMDS(mds_meta))
        << fmt::format("get randomly mds fail for ino({}).", parent);
    is_fallback = true;
  }

  LOG(INFO) << fmt::format("[meta] query target mds({}:{}) for parent({}).",
                           mds_meta.Host(), mds_meta.Port(), parent);
  return StrToEndpoint(mds_meta.Host(), mds_meta.Port());
}

uint64_t MDSClient::GetInodeVersion(Ino ino) {
  uint64_t version = 0;
  parent_memo_->GetVersion(ino, version);
  return version;
}

Status MDSClient::Lookup(Ino parent, const std::string& name, Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, parent]() -> EndPoint {
    return GetEndpointByParent(parent);
  };

  pb::mdsv2::LookupRequest request;
  pb::mdsv2::LookupResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent));

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  auto status =
      SendRequest(get_endpoint_fn, "MDSService", "Lookup", request, response);
  if (!status.ok()) {
    return status;
  }

  const auto& inode = response.inode();

  if (fs_info_->IsHashPartition() && mdsv2::IsDir(inode.ino())) {
    uint64_t last_version;
    if (parent_memo_->GetVersion(inode.ino(), last_version) &&
        inode.version() < last_version) {
      // fetch last inode
      status = GetAttr(inode.ino(), out_attr);
      if (status.ok()) {
        parent_memo_->Upsert(out_attr.ino, parent);
        return Status::OK();

      } else {
        LOG(WARNING) << fmt::format(
            "[meta.{}] lookup({}/{}) get last inode fail, error: {}.", fs_id_,
            parent, name, status.ToString());
      }
    }
  }

  // save ino to parent mapping
  parent_memo_->Upsert(inode.ino(), parent, inode.version());

  out_attr = ToAttr(inode);

  return Status::OK();
}

Status MDSClient::MkNod(Ino parent, const std::string& name, uint32_t uid,
                        uint32_t gid, mode_t mode, dev_t rdev, Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, parent]() -> EndPoint {
    return GetEndpointByParent(parent);
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
      SendRequest(get_endpoint_fn, "MDSService", "MkNod", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->Upsert(response.inode().ino(), parent,
                       response.inode().version());
  parent_memo_->UpsertVersion(parent, response.parent_version());

  out_attr = ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::MkDir(Ino parent, const std::string& name, uint32_t uid,
                        uint32_t gid, mode_t mode, dev_t rdev, Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, parent]() -> EndPoint {
    return GetEndpointByParent(parent);
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
      SendRequest(get_endpoint_fn, "MDSService", "MkDir", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->Upsert(response.inode().ino(), parent,
                       response.inode().version());
  parent_memo_->UpsertVersion(parent, response.parent_version());

  out_attr = ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::RmDir(Ino parent, const std::string& name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, parent]() -> EndPoint {
    return GetEndpointByParent(parent);
  };

  pb::mdsv2::RmDirRequest request;
  pb::mdsv2::RmDirResponse response;

  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  auto status =
      SendRequest(get_endpoint_fn, "MDSService", "RmDir", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::ReadDir(Ino ino, const std::string& last_name, uint32_t limit,
                          bool with_attr, std::vector<DirEntry>& entries) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, ino]() -> EndPoint { return GetEndpoint(ino); };

  pb::mdsv2::ReadDirRequest request;
  pb::mdsv2::ReadDirResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_last_name(last_name);
  request.set_limit(limit);
  request.set_with_attr(with_attr);

  auto status =
      SendRequest(get_endpoint_fn, "MDSService", "ReadDir", request, response);
  if (!status.ok()) {
    return status;
  }

  entries.reserve(response.entries_size());
  for (const auto& entry : response.entries()) {
    parent_memo_->Upsert(entry.ino(), ino, entry.inode().version());
    entries.push_back(ToDirEntry(entry));
  }

  return Status::OK();
}

Status MDSClient::Open(Ino ino, int flags, std::string& session_id) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, ino]() -> EndPoint { return GetEndpoint(ino); };

  pb::mdsv2::OpenRequest request;
  pb::mdsv2::OpenResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_flags(flags);

  auto status =
      SendRequest(get_endpoint_fn, "MDSService", "Open", request, response);
  if (!status.ok()) {
    return status;
  }

  session_id = response.session_id();

  parent_memo_->UpsertVersion(ino, response.version());

  return Status::OK();
}

Status MDSClient::Release(Ino ino, const std::string& session_id) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, ino]() -> EndPoint { return GetEndpoint(ino); };

  pb::mdsv2::ReleaseRequest request;
  pb::mdsv2::ReleaseResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_session_id(session_id);

  auto status =
      SendRequest(get_endpoint_fn, "MDSService", "Release", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::Link(Ino ino, Ino new_parent, const std::string& new_name,
                       Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, new_parent]() -> EndPoint {
    return GetEndpointByParent(new_parent);
  };

  pb::mdsv2::LinkRequest request;
  pb::mdsv2::LinkResponse response;

  SetAncestorInContext(request, new_parent);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_new_parent(new_parent);
  request.set_new_name(new_name);

  auto status =
      SendRequest(get_endpoint_fn, "MDSService", "Link", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->Upsert(response.inode().ino(), new_parent,
                       response.inode().version());
  parent_memo_->UpsertVersion(new_parent, response.parent_version());

  out_attr = ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::UnLink(Ino parent, const std::string& name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, parent]() -> EndPoint {
    return GetEndpointByParent(parent);
  };

  pb::mdsv2::UnLinkRequest request;
  pb::mdsv2::UnLinkResponse response;

  SetAncestorInContext(request, parent);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  auto status =
      SendRequest(get_endpoint_fn, "MDSService", "UnLink", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::Symlink(Ino parent, const std::string& name, uint32_t uid,
                          uint32_t gid, const std::string& symlink,
                          Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, parent]() -> EndPoint {
    return GetEndpointByParent(parent);
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
      SendRequest(get_endpoint_fn, "MDSService", "Symlink", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->Upsert(response.inode().ino(), parent,
                       response.inode().version());

  parent_memo_->UpsertVersion(parent, response.parent_version());

  out_attr = ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::ReadLink(Ino ino, std::string& symlink) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, ino]() -> EndPoint { return GetEndpoint(ino); };

  pb::mdsv2::ReadLinkRequest request;
  pb::mdsv2::ReadLinkResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status =
      SendRequest(get_endpoint_fn, "MDSService", "ReadLink", request, response);
  if (!status.ok()) {
    return status;
  }

  symlink = response.symlink();

  return Status::OK();
}

Status MDSClient::GetAttr(Ino ino, Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  bool is_fallback = false;
  auto get_endpoint_fn = [this, ino, &is_fallback]() -> EndPoint {
    return mdsv2::IsDir(ino) ? GetEndpointByParentWithFallback(ino, is_fallback)
                             : GetEndpointWithFallback(ino, is_fallback);
  };

  pb::mdsv2::GetAttrRequest request;
  pb::mdsv2::GetAttrResponse response;

  auto* ctx = request.mutable_context();
  ctx->set_is_bypass_cache(is_fallback);
  ctx->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status =
      SendRequest(get_endpoint_fn, "MDSService", "GetAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->UpsertVersion(ino, response.inode().version());

  out_attr = ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::SetAttr(Ino ino, const Attr& attr, int to_set,
                          Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, ino]() -> EndPoint {
    return mdsv2::IsDir(ino) ? GetEndpointByParent(ino) : GetEndpoint(ino);
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
      SendRequest(get_endpoint_fn, "MDSService", "SetAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  out_attr = ToAttr(response.inode());

  parent_memo_->UpsertVersion(ino, response.inode().version());

  return Status::OK();
}

Status MDSClient::GetXAttr(Ino ino, const std::string& name,
                           std::string& value) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  bool is_fallback = false;
  auto get_endpoint_fn = [this, ino, &is_fallback]() -> EndPoint {
    return mdsv2::IsDir(ino) ? GetEndpointByParentWithFallback(ino, is_fallback)
                             : GetEndpointWithFallback(ino, is_fallback);
  };

  pb::mdsv2::GetXAttrRequest request;
  pb::mdsv2::GetXAttrResponse response;

  auto* ctx = request.mutable_context();
  ctx->set_is_bypass_cache(is_fallback);
  ctx->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_name(name);

  auto status =
      SendRequest(get_endpoint_fn, "MDSService", "GetXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  value = response.value();

  return Status::OK();
}

Status MDSClient::SetXAttr(Ino ino, const std::string& name,
                           const std::string& value) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, ino]() -> EndPoint {
    return mdsv2::IsDir(ino) ? GetEndpointByParent(ino) : GetEndpoint(ino);
  };

  pb::mdsv2::SetXAttrRequest request;
  pb::mdsv2::SetXAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.mutable_xattrs()->insert({name, value});

  auto status =
      SendRequest(get_endpoint_fn, "MDSService", "SetXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->UpsertVersion(ino, response.inode_version());

  return Status::OK();
}

Status MDSClient::RemoveXAttr(Ino ino, const std::string& name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, ino]() -> EndPoint {
    return mdsv2::IsDir(ino) ? GetEndpointByParent(ino) : GetEndpoint(ino);
  };

  pb::mdsv2::RemoveXAttrRequest request;
  pb::mdsv2::RemoveXAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_name(name);

  auto status = SendRequest(get_endpoint_fn, "MDSService", "RemoveXAttr",
                            request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->UpsertVersion(ino, response.inode_version());

  return Status::OK();
}

Status MDSClient::ListXAttr(Ino ino,
                            std::map<std::string, std::string>& xattrs) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, ino]() -> EndPoint {
    return mdsv2::IsDir(ino) ? GetEndpointByParent(ino) : GetEndpoint(ino);
  };

  pb::mdsv2::ListXAttrRequest request;
  pb::mdsv2::ListXAttrResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status = SendRequest(get_endpoint_fn, "MDSService", "ListXAttr", request,
                            response);
  if (!status.ok()) {
    return status;
  }

  for (const auto& [name, value] : response.xattrs()) {
    xattrs[name] = value;
  }

  return Status::OK();
}

Status MDSClient::Rename(Ino old_parent, const std::string& old_name,
                         Ino new_parent, const std::string& new_name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, new_parent]() -> EndPoint {
    return GetEndpointByParent(new_parent);
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
      SendRequest(get_endpoint_fn, "MDSService", "Rename", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_memo_->UpsertVersion(old_parent, response.old_parent_version());
  parent_memo_->UpsertVersion(new_parent, response.new_parent_version());

  return Status::OK();
}

static Slice ToSlice(const pb::mdsv2::Slice& slice) {
  Slice out_slice;

  out_slice.id = slice.id();
  out_slice.offset = slice.offset();
  out_slice.length = slice.len();
  out_slice.compaction = slice.compaction_version();
  out_slice.is_zero = slice.zero();
  out_slice.size = slice.size();

  return out_slice;
}

static pb::mdsv2::Slice ToSlice(const Slice& slice) {
  pb::mdsv2::Slice out_slice;

  out_slice.set_id(slice.id);
  out_slice.set_offset(slice.offset);
  out_slice.set_len(slice.length);
  out_slice.set_compaction_version(slice.compaction);
  out_slice.set_zero(slice.is_zero);
  out_slice.set_size(slice.size);

  return out_slice;
}

Status MDSClient::NewSliceId(Ino ino, uint64_t* id) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, ino]() -> EndPoint { return GetEndpoint(ino); };

  pb::mdsv2::AllocSliceIdRequest request;
  pb::mdsv2::AllocSliceIdResponse response;

  request.set_alloc_num(1);

  auto status = SendRequest(get_endpoint_fn, "MDSService", "AllocSliceId",
                            request, response);
  if (!status.ok()) {
    return status;
  }

  *id = response.slice_id();

  return Status::OK();
}

Status MDSClient::ReadSlice(Ino ino, uint64_t index,
                            std::vector<Slice>* slices) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  CHECK(slices != nullptr) << "slices is nullptr.";

  bool is_fallback = false;
  auto get_endpoint_fn = [this, ino, &is_fallback]() -> EndPoint {
    return GetEndpointWithFallback(ino, is_fallback);
  };

  pb::mdsv2::ReadSliceRequest request;
  pb::mdsv2::ReadSliceResponse response;

  auto* ctx = request.mutable_context();
  ctx->set_is_bypass_cache(is_fallback);
  ctx->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_chunk_index(index);

  auto status = SendRequest(get_endpoint_fn, "MDSService", "ReadSlice", request,
                            response);
  if (!status.ok()) {
    return status;
  }

  for (const auto& slice : response.slices()) {
    slices->push_back(ToSlice(slice));
  }

  return Status::OK();
}

Status MDSClient::WriteSlice(Ino ino, uint64_t index,
                             const std::vector<Slice>& slices) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, ino]() -> EndPoint { return GetEndpoint(ino); };

  pb::mdsv2::WriteSliceRequest request;
  pb::mdsv2::WriteSliceResponse response;

  SetAncestorInContext(request, ino);

  Ino parent = 0;
  CHECK(parent_memo_->GetParent(ino, parent))
      << "get parent fail from parent cache.";

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_ino(ino);
  request.set_chunk_index(index);

  for (const auto& slice : slices) {
    *request.add_slices() = ToSlice(slice);
  }

  auto status = SendRequest(get_endpoint_fn, "MDSService", "WriteSlice",
                            request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::Fallocate(Ino ino, int32_t mode, uint64_t offset,
                            uint64_t length) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto get_endpoint_fn = [this, ino]() -> EndPoint { return GetEndpoint(ino); };

  pb::mdsv2::FallocateRequest request;
  pb::mdsv2::FallocateResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_mode(mode);
  request.set_offset(offset);
  request.set_len(length);

  auto status = SendRequest(get_endpoint_fn, "MDSService", "Fallocate", request,
                            response);
  if (!status.ok()) {
    return status;
  }

  const auto& attr = response.inode();

  parent_memo_->UpsertVersion(attr.ino(), attr.version());

  return Status::OK();
}

Status MDSClient::GetFsQuota(FsStat& fs_stat) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  pb::mdsv2::GetFsQuotaRequest request;
  pb::mdsv2::GetFsQuotaResponse response;

  request.set_fs_id(fs_id_);

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
  pb::mdsv2::FsInfo new_fs_info;
  auto status = MDSClient::GetFsInfo(rpc_, fs_info_->GetName(), new_fs_info);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta] get fs info fail, {}.",
                              status.ToString());
    return false;
  }

  epoch_ = new_fs_info.partition_policy().epoch();

  fs_info_->Update(new_fs_info);

  if (!mds_router_->UpdateRouter(new_fs_info.partition_policy())) {
    LOG(ERROR) << "[meta] update mds router fail.";
  }

  return true;
}

// process epoch change
// 1. updatge fs info
// 2. update mds router
bool MDSClient::ProcessEpochChange() {
  LOG(INFO) << "[meta] process epoch change.";
  return UpdateRouter();
}

bool MDSClient::ProcessNotServe() {
  LOG(INFO) << "[meta] process not serve.";
  return UpdateRouter();
}

bool MDSClient::ProcessNetError(EndPoint& endpoint) {
  LOG(INFO) << "[meta] process net error.";

  auto mdses = mds_discovery_->GetMDSByState(mdsv2::MDSMeta::State::kNormal);
  for (auto& mds : mdses) {
    if (mds.Host() != TakeIp(endpoint) || mds.Port() != endpoint.port) {
      endpoint = StrToEndpoint(mds.Host(), mds.Port());
      return true;
    }
  }

  return false;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs