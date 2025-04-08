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

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include <cstdint>
#include <string>
#include <utility>

#include "client/vfs/common/helper.h"
#include "client/vfs/vfs_meta.h"
#include "dingofs/mdsv2.pb.h"
#include "dingofs/metaserver.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mdsv2/common/constant.h"
#include "mdsv2/common/logging.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

DEFINE_uint32(client_send_request_retry, 3, "client send request retry times");

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
                     ParentCachePtr parent_cache, MDSDiscoveryPtr mds_discovery,
                     MDSRouterPtr mds_router, RPCPtr rpc)
    : client_id_(client_id),
      fs_info_(fs_info),
      fs_id_(fs_info->GetFsId()),
      epoch_(fs_info->GetEpoch()),
      parent_cache_(parent_cache),
      mds_discovery_(mds_discovery),
      mds_router_(mds_router),
      rpc_(rpc) {}

bool MDSClient::Init() {
  CHECK(parent_cache_ != nullptr) << "parent cache is null.";
  CHECK(mds_discovery_ != nullptr) << "mds discovery is null.";
  CHECK(mds_router_ != nullptr) << "mds router is null.";
  CHECK(rpc_ != nullptr) << "rpc is null.";

  return true;
}

void MDSClient::Destory() {}

bool MDSClient::SetEndpoint(const std::string& ip, int port, bool is_default) {
  return rpc_->AddEndpoint(ip, port, is_default);
}

Status MDSClient::GetFsInfo(RPCPtr rpc, const std::string& name,
                            pb::mdsv2::FsInfo& fs_info) {
  pb::mdsv2::GetFsInfoRequest request;
  pb::mdsv2::GetFsInfoResponse response;

  request.set_fs_name(name);

  auto status = rpc->SendRequest("MDSService", "GetFsInfo", request, response);
  if (!status.ok()) {
    return status;
  }

  fs_info = response.fs_info();

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
                           const pb::mdsv2::MountPoint& mount_point) {
  pb::mdsv2::UmountFsRequest request;
  pb::mdsv2::UmountFsResponse response;

  request.set_fs_name(name);
  request.mutable_mount_point()->CopyFrom(mount_point);

  auto status = rpc_->SendRequest("MDSService", "UmountFs", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

EndPoint MDSClient::GetEndPointByIno(int64_t ino) {
  auto mds_meta = mds_router_->GetMDSByIno(ino);
  DINGO_LOG(INFO) << fmt::format("query target mds({}:{}) for ino({}).",
                                 mds_meta.Host(), mds_meta.Port(), ino);
  return StrToEndpoint(mds_meta.Host(), mds_meta.Port());
}

EndPoint MDSClient::GetEndPointByParentIno(int64_t parent_ino) {
  auto mds_meta = mds_router_->GetMDSByParentIno(parent_ino);
  DINGO_LOG(INFO) << fmt::format("query target mds({}:{}) for parent({}).",
                                 mds_meta.Host(), mds_meta.Port(), parent_ino);
  return StrToEndpoint(mds_meta.Host(), mds_meta.Port());
}

uint64_t MDSClient::GetInodeVersion(int64_t ino) {
  uint64_t version = 0;
  parent_cache_->GetVersion(ino, version);
  return version;
}

Status MDSClient::Lookup(uint64_t parent_ino, const std::string& name,
                         Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByParentIno(parent_ino);

  pb::mdsv2::LookupRequest request;
  pb::mdsv2::LookupResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(parent_ino));

  request.set_fs_id(fs_id_);
  request.set_parent_ino(parent_ino);
  request.set_name(name);

  auto status =
      SendRequest(endpoint, "MDSService", "Lookup", request, response);
  if (!status.ok()) {
    return status;
  }

  // save ino to parent mapping
  parent_cache_->Upsert(response.inode().ino(), parent_ino,
                        response.inode().version());

  out_attr = ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::MkNod(uint64_t parent_ino, const std::string& name,
                        uint32_t uid, uint32_t gid, mode_t mode, dev_t rdev,
                        Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  auto endpoint = GetEndPointByParentIno(parent_ino);

  pb::mdsv2::MkNodRequest request;
  pb::mdsv2::MkNodResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent_ino(parent_ino);
  request.set_name(name);
  request.set_mode(mode);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_rdev(rdev);

  request.set_length(0);

  auto status = SendRequest(endpoint, "MDSService", "MkNod", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_cache_->Upsert(response.inode().ino(), parent_ino,
                        response.inode().version());

  out_attr = ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::MkDir(uint64_t parent_ino, const std::string& name,
                        uint32_t uid, uint32_t gid, mode_t mode, dev_t rdev,
                        Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByParentIno(parent_ino);

  pb::mdsv2::MkDirRequest request;
  pb::mdsv2::MkDirResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent_ino(parent_ino);
  request.set_name(name);
  request.set_mode(mode);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_rdev(rdev);

  request.set_length(0);

  auto status = SendRequest(endpoint, "MDSService", "MkDir", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_cache_->Upsert(response.inode().ino(), parent_ino,
                        response.inode().version());

  out_attr = ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::RmDir(uint64_t parent_ino, const std::string& name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByParentIno(parent_ino);

  pb::mdsv2::RmDirRequest request;
  pb::mdsv2::RmDirResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent_ino(parent_ino);
  request.set_name(name);

  auto status = SendRequest(endpoint, "MDSService", "RmDir", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::ReadDir(uint64_t ino, const std::string& last_name,
                          uint32_t limit, bool with_attr,
                          std::vector<DirEntry>& entries) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByIno(ino);

  pb::mdsv2::ReadDirRequest request;
  pb::mdsv2::ReadDirResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_last_name(last_name);
  request.set_limit(limit);
  request.set_with_attr(with_attr);

  auto status =
      SendRequest(endpoint, "MDSService", "ReadDir", request, response);
  if (!status.ok()) {
    return status;
  }

  entries.reserve(response.entries_size());
  for (const auto& entry : response.entries()) {
    parent_cache_->Upsert(entry.ino(), ino, entry.inode().version());
    entries.push_back(ToDirEntry(entry));
  }

  return Status::OK();
}

Status MDSClient::Open(uint64_t ino) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByIno(ino);

  pb::mdsv2::OpenRequest request;
  pb::mdsv2::OpenResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status = SendRequest(endpoint, "MDSService", "Open", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::Release(uint64_t ino) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByIno(ino);

  pb::mdsv2::ReleaseRequest request;
  pb::mdsv2::ReleaseResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status =
      SendRequest(endpoint, "MDSService", "Release", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::Link(uint64_t ino, uint64_t new_parent_ino,
                       const std::string& new_name, Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByParentIno(new_parent_ino);

  pb::mdsv2::LinkRequest request;
  pb::mdsv2::LinkResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_new_parent_ino(new_parent_ino);
  request.set_new_name(new_name);

  auto status = SendRequest(endpoint, "MDSService", "Link", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_cache_->Upsert(response.inode().ino(), new_parent_ino,
                        response.inode().version());

  out_attr = ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::UnLink(uint64_t parent_ino, const std::string& name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByParentIno(parent_ino);

  pb::mdsv2::UnLinkRequest request;
  pb::mdsv2::UnLinkResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent_ino(parent_ino);
  request.set_name(name);

  auto status =
      SendRequest(endpoint, "MDSService", "UnLink", request, response);
  if (!status.ok()) {
    return status;
  }
  return Status::OK();
}

Status MDSClient::Symlink(uint64_t parent_ino, const std::string& name,
                          uint32_t uid, uint32_t gid,
                          const std::string& symlink, Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByParentIno(parent_ino);

  pb::mdsv2::SymlinkRequest request;
  pb::mdsv2::SymlinkResponse response;

  request.set_fs_id(fs_id_);
  request.set_symlink(symlink);

  request.set_new_parent_ino(parent_ino);
  request.set_new_name(name);
  request.set_uid(uid);
  request.set_gid(gid);

  auto status =
      SendRequest(endpoint, "MDSService", "Symlink", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_cache_->Upsert(response.inode().ino(), parent_ino,
                        response.inode().version());

  out_attr = ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::ReadLink(uint64_t ino, std::string& symlink) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByIno(ino);

  pb::mdsv2::ReadLinkRequest request;
  pb::mdsv2::ReadLinkResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status =
      SendRequest(endpoint, "MDSService", "ReadLink", request, response);
  if (!status.ok()) {
    return status;
  }

  symlink = response.symlink();

  return Status::OK();
}

Status MDSClient::GetAttr(uint64_t ino, Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByIno(ino);

  pb::mdsv2::GetAttrRequest request;
  pb::mdsv2::GetAttrResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status =
      SendRequest(endpoint, "MDSService", "GetAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  out_attr = ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::SetAttr(uint64_t ino, const Attr& attr, int to_set,
                          Attr& out_attr) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByIno(ino);

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
  clock_gettime(CLOCK_REALTIME, &now);

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
    // todo: Truncate data
    request.set_length(attr.length);
  }

  request.set_to_set(temp_to_set);

  auto status =
      SendRequest(endpoint, "MDSService", "SetAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  out_attr = ToAttr(response.inode());

  return Status::OK();
}

Status MDSClient::GetXAttr(uint64_t ino, const std::string& name,
                           std::string& value) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByIno(ino);

  pb::mdsv2::GetXAttrRequest request;
  pb::mdsv2::GetXAttrResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_name(name);

  auto status =
      SendRequest(endpoint, "MDSService", "GetXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  value = response.value();

  return Status::OK();
}

Status MDSClient::SetXAttr(uint64_t ino, const std::string& name,
                           const std::string& value) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByIno(ino);

  pb::mdsv2::SetXAttrRequest request;
  pb::mdsv2::SetXAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.mutable_xattrs()->insert({name, value});

  auto status =
      SendRequest(endpoint, "MDSService", "SetXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::ListXAttr(uint64_t ino,
                            std::map<std::string, std::string>& xattrs) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByIno(ino);

  pb::mdsv2::ListXAttrRequest request;
  pb::mdsv2::ListXAttrResponse response;

  request.mutable_context()->set_inode_version(GetInodeVersion(ino));

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status =
      SendRequest(endpoint, "MDSService", "ListXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  for (const auto& [name, value] : response.xattrs()) {
    xattrs[name] = value;
  }

  return Status::OK();
}

Status MDSClient::Rename(uint64_t old_parent_ino, const std::string& old_name,
                         uint64_t new_parent_ino, const std::string& new_name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByParentIno(new_parent_ino);

  pb::mdsv2::RenameRequest request;
  pb::mdsv2::RenameResponse response;

  request.set_fs_id(fs_id_);
  request.set_old_parent_ino(old_parent_ino);
  request.set_old_name(old_name);
  request.set_new_parent_ino(new_parent_ino);
  request.set_new_name(new_name);

  auto status =
      SendRequest(endpoint, "MDSService", "Rename", request, response);
  if (!status.ok()) {
    return status;
  }

  parent_cache_->Upsert(old_parent_ino, response.old_parent_version());
  parent_cache_->Upsert(new_parent_ino, response.new_parent_version());

  return Status::OK();
}

static Slice ToSlice(const pb::mdsv2::Slice& slice) {
  Slice out_slice;

  out_slice.id = slice.id();
  out_slice.offset = slice.offset();
  out_slice.length = slice.len();
  out_slice.compaction = slice.compaction();
  out_slice.is_zero = slice.zero();
  out_slice.size = slice.size();

  return out_slice;
}

static pb::mdsv2::Slice ToSlice(const Slice& slice) {
  pb::mdsv2::Slice out_slice;

  out_slice.set_id(slice.id);
  out_slice.set_offset(slice.offset);
  out_slice.set_len(slice.length);
  out_slice.set_compaction(slice.compaction);
  out_slice.set_zero(slice.is_zero);
  out_slice.set_size(slice.size);

  return out_slice;
}

Status MDSClient::ReadSlice(Ino ino, uint64_t index,
                            std::vector<Slice>* slices) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  CHECK(slices != nullptr) << "slices is nullptr.";

  auto endpoint = GetEndPointByIno(ino);

  pb::mdsv2::ReadSliceRequest request;
  pb::mdsv2::ReadSliceResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_chunk_index(index);

  auto status =
      SendRequest(endpoint, "MDSService", "ReadSlice", request, response);
  if (!status.ok()) {
    return status;
  }

  for (const auto& slice : response.slice_list().slices()) {
    slices->push_back(ToSlice(slice));
  }

  return Status::OK();
}

Status MDSClient::NewSliceId(uint64_t* id) {
  auto endpoint = GetEndPointByParentIno(1);

  pb::mdsv2::AllocSliceIdRequest request;
  pb::mdsv2::AllocSliceIdResponse response;

  request.set_alloc_num(1);

  auto status =
      SendRequest(endpoint, "MDSService", "AllocSliceId", request, response);
  if (!status.ok()) {
    return status;
  }

  if (!response.slice_ids().empty()) {
    *id = response.slice_ids().at(0);
  }

  return Status::OK();
}

Status MDSClient::WriteSlice(Ino ino, uint64_t index,
                             const std::vector<Slice>& slices) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  auto endpoint = GetEndPointByIno(ino);

  pb::mdsv2::WriteSliceRequest request;
  pb::mdsv2::WriteSliceResponse response;

  request.set_fs_id(fs_id_);
  request.set_chunk_index(index);

  for (const auto& slice : slices) {
    *request.mutable_slice_list()->add_slices() = ToSlice(slice);
  }

  auto status =
      SendRequest(endpoint, "MDSService", "WriteSlice", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

bool MDSClient::UpdateRouter() {
  pb::mdsv2::FsInfo new_fs_info;
  auto status = MDSClient::GetFsInfo(rpc_, fs_info_->GetName(), new_fs_info);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("get fs info fail, {}.", status.ToString());
    return false;
  }

  epoch_ = std::max(new_fs_info.partition_policy().mono().epoch(),
                    new_fs_info.partition_policy().parent_hash().epoch());

  fs_info_->Update(new_fs_info);

  if (!mds_router_->UpdateRouter(new_fs_info.partition_policy())) {
    LOG(ERROR) << "update mds router fail.";
  }

  return true;
}

// process epoch change
// 1. updatge fs info
// 2. update mds router
bool MDSClient::ProcessEpochChange() {
  DINGO_LOG(INFO) << "process epoch change.";
  return UpdateRouter();
}

bool MDSClient::ProcessNotServe() {
  DINGO_LOG(INFO) << "process not serve.";
  return UpdateRouter();
}

bool MDSClient::ProcessNetError(EndPoint& endpoint) {
  DINGO_LOG(INFO) << "process net error.";

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