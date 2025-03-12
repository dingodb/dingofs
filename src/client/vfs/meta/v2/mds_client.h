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

#include "client/vfs/meta/v2/mds_router.h"
#include "client/vfs/meta/v2/rpc.h"
#include "client/vfs/vfs_meta.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/filesystem/fs_info.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

DECLARE_uint32(client_send_request_retry);

class MDSClient;
using MDSClientPtr = std::shared_ptr<MDSClient>;

class MDSClient {
 public:
  MDSClient(mdsv2::FsInfoPtr fs_info, ParentCachePtr parent_cache,
            MDSDiscoveryPtr mds_discovery, MDSRouterPtr mds_router, RPCPtr rpc);
  virtual ~MDSClient() = default;

  static MDSClientPtr New(mdsv2::FsInfoPtr fs_info, ParentCachePtr parent_cache,
                          MDSDiscoveryPtr mds_discovery,
                          MDSRouterPtr mds_router, RPCPtr rpc) {
    return std::make_shared<MDSClient>(fs_info, parent_cache, mds_discovery,
                                       mds_router, rpc);
  }

  bool Init();
  void Destory();

  bool SetEndpoint(const std::string& ip, int port, bool is_default);

  static Status GetFsInfo(RPCPtr rpc, const std::string& name,
                          pb::mdsv2::FsInfo& fs_info);
  Status MountFs(const std::string& name,
                 const pb::mdsv2::MountPoint& mount_point);
  Status UmountFs(const std::string& name,
                  const pb::mdsv2::MountPoint& mount_point);

  Status Lookup(uint64_t parent_ino, const std::string& name, Attr& out_attr);

  Status MkNod(uint64_t parent_ino, const std::string& name, uint32_t uid,
               uint32_t gid, mode_t mode, dev_t rdev, Attr& out_attr);
  Status MkDir(uint64_t parent_ino, const std::string& name, uint32_t uid,
               uint32_t gid, mode_t mode, dev_t rdev, Attr& out_attr);
  Status RmDir(uint64_t parent_ino, const std::string& name);

  Status ReadDir(uint64_t ino, const std::string& last_name, uint32_t limit,
                 bool with_attr, std::vector<DirEntry>& entries);

  Status Open(uint64_t ino);
  Status Release(uint64_t ino);

  Status Link(uint64_t ino, uint64_t new_parent_ino,
              const std::string& new_name, Attr& out_attr);
  Status UnLink(uint64_t parent_ino, const std::string& name);
  Status Symlink(uint64_t parent_ino, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& symlink, Attr& out_attr);
  Status ReadLink(uint64_t ino, std::string& symlink);

  Status GetAttr(uint64_t ino, Attr& out_attr);
  Status SetAttr(uint64_t ino, const Attr& attr, int to_set, Attr& out_attr);
  Status GetXAttr(uint64_t ino, const std::string& name, std::string& value);
  Status SetXAttr(uint64_t ino, const std::string& name,
                  const std::string& value);
  Status ListXAttr(uint64_t ino, std::map<std::string, std::string>& xattrs);

  Status Rename(uint64_t old_parent_ino, const std::string& old_name,
                uint64_t new_parent_ino, const std::string& new_name);

  Status ReadSlice(Ino ino, uint64_t index, std::vector<Slice>* slices);
  Status NewSliceId(uint64_t* id);
  Status WriteSlice(Ino ino, uint64_t index, const std::vector<Slice>& slices);

 private:
  EndPoint GetEndPointByIno(int64_t ino);
  EndPoint GetEndPointByParentIno(int64_t parent_ino);

  bool UpdateRouter();

  bool ProcessEpochChange();
  bool ProcessNotServe();
  bool ProcessNetError(EndPoint& endpoint);

  template <typename Request, typename Response>
  Status SendRequest(EndPoint& endpoint, const std::string& service_name,
                     const std::string& api_name, Request& request,
                     Response& response);

  uint32_t fs_id_{0};
  uint64_t epoch_{0};
  mdsv2::FsInfoPtr fs_info_;

  ParentCachePtr parent_cache_;

  MDSRouterPtr mds_router_;

  MDSDiscoveryPtr mds_discovery_;

  RPCPtr rpc_;
};

template <typename Request, typename Response>
Status MDSClient::SendRequest(EndPoint& endpoint,
                              const std::string& service_name,
                              const std::string& api_name, Request& request,
                              Response& response) {
  request.mutable_info()->set_request_id(mdsv2::Helper::TimestampNs());
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