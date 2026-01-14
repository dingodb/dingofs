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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_CLIENT_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_CLIENT_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "client/vfs/common/client_id.h"
#include "client/vfs/metasystem/mds/mds_router.h"
#include "client/vfs/metasystem/mds/rpc.h"
#include "client/vfs/metasystem/meta_system.h"
#include "client/vfs/vfs_meta.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "common/trace/trace_manager.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "mds/common/type.h"
#include "mds/filesystem/fs_info.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

class MDSClient;
using MDSClientSPtr = std::shared_ptr<MDSClient>;

using mds::AttrEntry;
using mds::ChunkDescriptor;
using mds::MDSMeta;

using GetMdsFn = std::function<MDSMeta(bool& is_primary_mds)>;

class MDSClient {
 public:
  MDSClient(const ClientId& client_id, mds::FsInfo& fs_info, RPC&& rpc,
            TraceManager& trace_manager);
  virtual ~MDSClient() = default;

  bool Init();
  void Stop();

  bool Dump(Json::Value& value);
  bool Dump(const DumpOption& options, Json::Value& value);
  bool Load(const Json::Value& value);

  bool SetEndpoint(const std::string& ip, int port);

  static Status GetFsInfo(RPC& rpc, const std::string& name,
                          mds::FsInfoEntry& fs_info);
  static Status GetFsInfo(RPC& rpc, uint32_t fs_id, mds::FsInfoEntry& fs_info);

  RPC& GetRpc();

  Status Heartbeat();

  Status MountFs(const std::string& name,
                 const pb::mds::MountPoint& mount_point);
  Status UmountFs(const std::string& name, const std::string& client_id);

  Status Lookup(ContextSPtr& ctx, Ino parent, const std::string& name,
                AttrEntry& attr_entry);

  Status Create(ContextSPtr& ctx, Ino parent, const std::string& name,
                uint32_t uid, uint32_t gid, uint32_t mode, int flag,
                AttrEntry& attr_entry, AttrEntry& parent_attr_entry,
                std::vector<std::string>& session_ids);
  Status MkNod(ContextSPtr& ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, mode_t mode, dev_t rdev,
               AttrEntry& attr_entry, AttrEntry& parent_attr_entry);

  struct MkNodParam {
    std::string name;
    uint32_t uid;
    uint32_t gid;
    mode_t mode;
    dev_t rdev;
  };
  Status BatchMkNod(ContextSPtr& ctx, Ino parent,
                    const std::vector<MkNodParam>& params,
                    std::vector<AttrEntry>& attr_entries,
                    AttrEntry& parent_attr_entry);

  Status MkDir(ContextSPtr& ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, mode_t mode, dev_t rdev,
               AttrEntry& attr_entry, AttrEntry& parent_attr_entry);
  struct MkDirParam {
    std::string name;
    uint32_t uid;
    uint32_t gid;
    mode_t mode;
    dev_t rdev;
  };
  Status BatchMkDir(ContextSPtr& ctx, Ino parent,
                    const std::vector<MkDirParam>& params,
                    std::vector<AttrEntry>& attr_entries,
                    AttrEntry& parent_attr_entry);

  Status RmDir(ContextSPtr& ctx, Ino parent, const std::string& name, Ino& ino,
               AttrEntry& parent_attr_entry);

  Status ReadDir(ContextSPtr& ctx, Ino ino, uint64_t fh,
                 const std::string& last_name, uint32_t limit, bool with_attr,
                 std::vector<DirEntry>& entries);

  Status Open(ContextSPtr& ctx, Ino ino, int flags, std::string& session_id,
              bool is_prefetch_chunk,
              const std::vector<mds::ChunkDescriptor>& chunk_descriptors,
              AttrEntry& attr_entry, std::vector<mds::ChunkEntry>& chunks);
  Status Release(ContextSPtr& ctx, Ino ino, const std::string& session_id);

  Status Link(ContextSPtr& ctx, Ino ino, Ino new_parent,
              const std::string& new_name, AttrEntry& attr_entry,
              AttrEntry& parent_attr_entry);
  Status UnLink(ContextSPtr& ctx, Ino parent, const std::string& name,
                AttrEntry& attr_entry, AttrEntry& parent_attr_entry);
  Status BatchUnLink(ContextSPtr& ctx, Ino parent,
                     const std::vector<std::string>& names,
                     std::vector<AttrEntry>& attr_entries,
                     AttrEntry& parent_attr_entry);
  Status Symlink(ContextSPtr& ctx, Ino parent, const std::string& name,
                 uint32_t uid, uint32_t gid, const std::string& symlink,
                 AttrEntry& attr_entry, AttrEntry& parent_attr_entry);
  Status ReadLink(ContextSPtr& ctx, Ino ino, std::string& symlink);

  Status GetAttr(ContextSPtr& ctx, Ino ino, AttrEntry& attr_entry);
  Status SetAttr(ContextSPtr& ctx, Ino ino, const Attr& attr, int to_set,
                 AttrEntry& attr_entry, bool& shrink_file);
  Status GetXAttr(ContextSPtr& ctx, Ino ino, const std::string& name,
                  std::string& value);
  Status SetXAttr(ContextSPtr& ctx, Ino ino, const std::string& name,
                  const std::string& value, AttrEntry& attr_entry);
  Status RemoveXAttr(ContextSPtr& ctx, Ino ino, const std::string& name,
                     AttrEntry& attr_entry);
  Status ListXAttr(ContextSPtr& ctx, Ino ino,
                   std::map<std::string, std::string>& xattrs);

  Status Rename(ContextSPtr& ctx, Ino old_parent, const std::string& old_name,
                Ino new_parent, const std::string& new_name,
                std::vector<Ino>& effected_inos);

  Status NewSliceId(ContextSPtr& ctx, uint32_t num, uint64_t* id);

  Status ReadSlice(ContextSPtr& ctx, Ino ino,
                   const std::vector<ChunkDescriptor>& chunk_descriptors,
                   std::vector<mds::ChunkEntry>& chunks);

  Status WriteSlice(ContextSPtr& ctx, Ino ino,
                    const std::vector<mds::DeltaSliceEntry>& delta_slices,
                    std::vector<ChunkDescriptor>& chunk_descriptors);

  struct CompactChunkParam {
    uint64_t version{0};

    // old slices in [start_slice_id, end_slice_id) will be replaced by
    // new_slices
    uint32_t start_pos{0};
    uint64_t start_slice_id{0};

    uint32_t end_pos{0};
    uint64_t end_slice_id{0};

    std::vector<mds::SliceEntry> new_slices;
  };
  Status CompactChunk(ContextSPtr& ctx, Ino ino, uint32_t chunk_index,
                      const CompactChunkParam& param,
                      mds::ChunkEntry& chunk_entry);

  Status Fallocate(ContextSPtr& ctx, Ino ino, int32_t mode, uint64_t offset,
                   uint64_t length);

  Status GetFsQuota(ContextSPtr& ctx, FsStat& fs_stat);
  Status GetDirQuota(ContextSPtr& ctx, Ino ino, FsStat& fs_stat);

 private:
  static Status DoGetFsInfo(RPC& rpc, pb::mds::GetFsInfoRequest& request,
                            mds::FsInfoEntry& fs_info);

  MDSMeta GetMds(Ino ino, bool& is_primary_mds);
  MDSMeta GetMdsByParent(int64_t parent, bool& is_primary_mds);

  uint64_t GetInodeVersion(Ino ino);
  int32_t GetInodeRenameRefCount(Ino ino);

  bool UpdateRouter();

  void ProcessEpochChange();
  void ProcessNotServe();
  void ProcessNetError(MDSMeta& mds_meta);

  template <typename Request>
  void SetAncestorInContext(Request& request, Ino ino);

  template <typename Request, typename Response>
  Status SendRequest(ContextSPtr ctx, SpanScopeSPtr& span, GetMdsFn get_mds_fn,
                     const std::string& service_name,
                     const std::string& api_name, Request& request,
                     Response& response);

  uint32_t fs_id_{0};
  uint64_t epoch_{0};

  const ClientId client_id_;
  mds::FsInfo& fs_info_;

  ParentMemo parent_memo_;

  RPC rpc_;

  MDSDiscovery mds_discovery_;

  MDSRouterUPtr mds_router_;

  TraceManager& trace_manager_;
};

template <typename Request>
void MDSClient::SetAncestorInContext(Request& request, Ino ino) {
  if (fs_info_.IsMonoPartition()) {
    return;
  }

  auto ancestors = parent_memo_.GetAncestors(ino);
  for (auto& ancestor : ancestors) {
    request.mutable_context()->add_ancestors(ancestor);
  }
}

template <typename Request, typename Response>
Status MDSClient::SendRequest(ContextSPtr ctx, SpanScopeSPtr& span,
                              GetMdsFn get_mds_fn,
                              const std::string& service_name,
                              const std::string& api_name, Request& request,
                              Response& response) {
  MDSMeta mds_meta;
  bool is_primary_mds = true;
  uint64_t primary_mds_id{0};
  bool is_refresh_mds = true;

  request.mutable_info()->set_request_id(
      ctx ? ctx->SessionID() : std::to_string(utils::TimestampNs()));

  request.mutable_context()->set_client_id(client_id_.ID());
  if (span != nullptr) {
    request.mutable_info()->set_span_id(SpanScope::GetSpanID(span));
  }

  Status status;
  int retry = 0;
  do {
    request.mutable_context()->set_epoch(epoch_);

    if (is_refresh_mds) {
      mds_meta = get_mds_fn(is_primary_mds);
      primary_mds_id = is_primary_mds ? mds_meta.ID() : 0;
      request.mutable_context()->set_is_bypass_cache(!is_primary_mds);
    }
    auto endpoint = StrToEndpoint(mds_meta.Host(), mds_meta.Port());

    status =
        rpc_.SendRequest(endpoint, service_name, api_name, request, response);
    if (!status.ok()) {
      LOG(INFO) << fmt::format(
          "[meta.client] send request fail, {} reqid({}) mds({}) retry({}) "
          "is_primary({}) status({}).",
          api_name, request.info().request_id(), mds_meta.ID(), retry,
          is_primary_mds, status.ToString());

      if (status.Errno() == pb::error::EROUTER_EPOCH_CHANGE) {
        ProcessEpochChange();
        is_refresh_mds = true;
        continue;

      } else if (status.Errno() == pb::error::ENOT_SERVE) {
        ProcessNotServe();
        is_refresh_mds = true;
        continue;

      } else if (status.IsNetError()) {
        ProcessNetError(mds_meta);

        is_primary_mds =
            (primary_mds_id != 0 && mds_meta.ID() == primary_mds_id);
        request.mutable_context()->set_is_bypass_cache(!is_primary_mds);

        is_refresh_mds = false;
        continue;
      }
    }

    return status;
  } while (IsRetry(retry, FLAGS_vfs_meta_rpc_retry_times));

  return status;
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_CLIENT_H_
