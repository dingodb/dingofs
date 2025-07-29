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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_MDS_CLIENT_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_MDS_CLIENT_H_

#include <cstdint>
#include <memory>
#include <string>

#include "client/meta/vfs_meta.h"
#include "client/vfs/meta/v2/client_id.h"
#include "client/vfs/meta/v2/mds_router.h"
#include "client/vfs/meta/v2/rpc.h"
#include "common/status.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/filesystem/fs_info.h"
#include "options/client/vfs/meta/v2_option.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class MDSClient;
using MDSClientPtr = std::shared_ptr<MDSClient>;

class MDSClient {
 public:
  MDSClient(const ClientId& client_id, mdsv2::FsInfoPtr fs_info,
            ParentMemoSPtr parent_memo, MDSDiscoveryPtr mds_discovery,
            MDSRouterPtr mds_router, RPCPtr rpc);
  virtual ~MDSClient() = default;

  static MDSClientPtr New(const ClientId& client_id, mdsv2::FsInfoPtr fs_info,
                          ParentMemoSPtr parent_memo,
                          MDSDiscoveryPtr mds_discovery,
                          MDSRouterPtr mds_router, RPCPtr rpc) {
    return std::make_shared<MDSClient>(client_id, fs_info, parent_memo,
                                       mds_discovery, mds_router, rpc);
  }

  bool Init();
  void Destory();

  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

  bool SetEndpoint(const std::string& ip, int port, bool is_default);

  static Status GetFsInfo(RPCPtr rpc, const std::string& name,
                          pb::mdsv2::FsInfo& fs_info);
  static Status GetFsInfo(RPCPtr rpc, uint32_t fs_id,
                          pb::mdsv2::FsInfo& fs_info);

  Status Heartbeat();

  Status MountFs(const std::string& name,
                 const pb::mdsv2::MountPoint& mount_point);
  Status UmountFs(const std::string& name, const std::string& client_id);

  Status Lookup(Ino parent, const std::string& name, Attr& out_attr);

  Status MkNod(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               mode_t mode, dev_t rdev, Attr& out_attr);
  Status MkDir(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               mode_t mode, dev_t rdev, Attr& out_attr);
  Status RmDir(Ino parent, const std::string& name);

  Status ReadDir(Ino ino, const std::string& last_name, uint32_t limit,
                 bool with_attr, std::vector<DirEntry>& entries);

  Status Open(Ino ino, int flags, std::string& session_id);
  Status Release(Ino ino, const std::string& session_id);

  Status Link(Ino ino, Ino new_parent, const std::string& new_name,
              Attr& out_attr);
  Status UnLink(Ino parent, const std::string& name);
  Status Symlink(Ino parent, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& symlink, Attr& out_attr);
  Status ReadLink(Ino ino, std::string& symlink);

  Status GetAttr(Ino ino, Attr& out_attr);
  Status SetAttr(Ino ino, const Attr& attr, int to_set, Attr& out_attr);
  Status GetXAttr(Ino ino, const std::string& name, std::string& value);
  Status SetXAttr(Ino ino, const std::string& name, const std::string& value);
  Status RemoveXAttr(Ino ino, const std::string& name);
  Status ListXAttr(Ino ino, std::map<std::string, std::string>& xattrs);

  Status Rename(Ino old_parent, const std::string& old_name, Ino new_parent,
                const std::string& new_name);

  Status NewSliceId(Ino ino, uint64_t* id);
  Status ReadSlice(Ino ino, uint64_t index, std::vector<Slice>* slices);
  Status WriteSlice(Ino ino, uint64_t index, const std::vector<Slice>& slices);

  Status Fallocate(Ino ino, int32_t mode, uint64_t offset, uint64_t length);

  Status GetFsQuota(FsStat& fs_stat);

 private:
  static Status DoGetFsInfo(RPCPtr rpc, pb::mdsv2::GetFsInfoRequest& request,
                            pb::mdsv2::FsInfo& fs_info);

  EndPoint GetEndpoint(Ino ino);
  EndPoint GetEndpointByParent(int64_t parent);

  EndPoint GetEndpointWithFallback(Ino ino, bool& is_fallback);
  EndPoint GetEndpointByParentWithFallback(int64_t parent, bool& is_fallback);

  uint64_t GetInodeVersion(Ino ino);

  bool UpdateRouter();

  bool ProcessEpochChange();
  bool ProcessNotServe();
  bool ProcessNetError(EndPoint& endpoint);

  template <typename Request>
  void SetAncestorInContext(Request& request, Ino ino);

  template <typename Request, typename Response>
  Status SendRequest(EndPoint& endpoint, const std::string& service_name,
                     const std::string& api_name, Request& request,
                     Response& response);

  uint32_t fs_id_{0};
  uint64_t epoch_{0};

  const ClientId client_id_;
  mdsv2::FsInfoPtr fs_info_;

  ParentMemoSPtr parent_memo_;

  MDSRouterPtr mds_router_;

  MDSDiscoveryPtr mds_discovery_;

  RPCPtr rpc_;
};

template <typename Request>
void MDSClient::SetAncestorInContext(Request& request, Ino ino) {
  if (fs_info_->IsMonoPartition()) {
    return;
  }

  auto ancestors = parent_memo_->GetAncestors(ino);
  for (auto& ancestor : ancestors) {
    request.mutable_context()->add_ancestors(ancestor);
  }
}

template <typename Request, typename Response>
Status MDSClient::SendRequest(EndPoint& endpoint,
                              const std::string& service_name,
                              const std::string& api_name, Request& request,
                              Response& response) {
  request.mutable_info()->set_request_id(mdsv2::Helper::TimestampNs());
  request.mutable_context()->set_client_id(client_id_.ID());
  for (int retry = 0; retry < FLAGS_client_send_request_retry; ++retry) {
    request.mutable_context()->set_epoch(epoch_);
    auto status =
        rpc_->SendRequest(endpoint, service_name, api_name, request, response);
    if (!status.ok()) {
      if (status.Errno() == pb::error::EROUTER_EPOCH_CHANGE) {
        if (!ProcessEpochChange()) {
          return Status::Internal("process epoch change fail");
        }
        continue;

      } else if (status.Errno() == pb::error::ENOT_SERVE) {
        if (!ProcessNotServe()) {
          return Status::Internal("process not serve fail");
        }
        continue;

      } else if (status.IsNetError()) {
        if (!ProcessNetError(endpoint)) {
          return Status::Internal("process net error fail");
        }
        continue;
      }
    }

    return status;
  }

  return Status::Internal("send request fail");
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_MDS_CLIENT_H_
