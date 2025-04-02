/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: Mon Sept 1 2021
 * Author: lixiaocui
 */

#ifndef DINGOFS_SRC_CLIENT_RPCCLIENT_METASERVER_CLIENT_H_
#define DINGOFS_SRC_CLIENT_RPCCLIENT_METASERVER_CLIENT_H_

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/types/optional.h"
#include "common/rpc_stream.h"
#include "dingofs/common.pb.h"
#include "dingofs/metaserver.pb.h"
#include "stub/metric/metric.h"
#include "stub/rpcclient/base_client.h"
#include "stub/rpcclient/task_excutor.h"

namespace dingofs {
namespace stub {
namespace rpcclient {

using S3ChunkInfoMap =
    google::protobuf::Map<uint64_t, pb::metaserver::S3ChunkInfoList>;

struct DataIndices {
  absl::optional<S3ChunkInfoMap> s3ChunkInfoMap;
  absl::optional<pb::metaserver::VolumeExtentList> volumeExtents;
};

class MetaServerClient {
 public:
  virtual ~MetaServerClient() = default;

  virtual pb::metaserver::MetaStatusCode Init(
      const common::ExcutorOpt& excutor_opt,
      const common::ExcutorOpt& excutor_internal_opt,
      std::shared_ptr<MetaCache> meta_cache,
      std::shared_ptr<ChannelManager<common::MetaserverID>>
          channel_manager) = 0;

  virtual pb::metaserver::MetaStatusCode GetTxId(uint32_t fs_id,
                                                 uint64_t inode_id,
                                                 uint32_t* partition_id,
                                                 uint64_t* tx_id) = 0;

  virtual void SetTxId(uint32_t partition_id, uint64_t tx_id) = 0;

  virtual pb::metaserver::MetaStatusCode GetDentry(
      uint32_t fs_id, uint64_t inodeid, const std::string& name,
      pb::metaserver::Dentry* out) = 0;

  virtual pb::metaserver::MetaStatusCode ListDentry(
      uint32_t fs_id, uint64_t inodeid, const std::string& last, uint32_t count,
      bool only_dir, std::list<pb::metaserver::Dentry>* dentry_list) = 0;

  virtual pb::metaserver::MetaStatusCode CreateDentry(
      const pb::metaserver::Dentry& dentry) = 0;

  virtual pb::metaserver::MetaStatusCode DeleteDentry(
      uint32_t fs_id, uint64_t inodeid, const std::string& name,
      pb::metaserver::FsFileType type) = 0;

  virtual pb::metaserver::MetaStatusCode PrepareRenameTx(
      const std::vector<pb::metaserver::Dentry>& dentrys) = 0;

  virtual pb::metaserver::MetaStatusCode GetInode(uint32_t fs_id,
                                                  uint64_t inodeid,
                                                  pb::metaserver::Inode* out,
                                                  bool* streaming) = 0;

  virtual pb::metaserver::MetaStatusCode GetInodeAttr(
      uint32_t fs_id, uint64_t inodeid, pb::metaserver::InodeAttr* attr) = 0;

  virtual pb::metaserver::MetaStatusCode BatchGetInodeAttr(
      uint32_t fs_id, const std::set<uint64_t>& inode_ids,
      std::list<pb::metaserver::InodeAttr>* attr) = 0;

  virtual pb::metaserver::MetaStatusCode BatchGetInodeAttrAsync(
      uint32_t fs_id, const std::vector<uint64_t>& inode_ids,
      MetaServerClientDone* done) = 0;

  virtual pb::metaserver::MetaStatusCode BatchGetXAttr(
      uint32_t fs_id, const std::set<uint64_t>& inode_ids,
      std::list<pb::metaserver::XAttr>* xattr) = 0;

  virtual pb::metaserver::MetaStatusCode UpdateInodeAttr(
      uint32_t fs_id, uint64_t inode_id,
      const pb::metaserver::InodeAttr& attr) = 0;

  virtual pb::metaserver::MetaStatusCode UpdateInodeAttrWithOutNlink(
      uint32_t fs_id, uint64_t inode_id, const pb::metaserver::InodeAttr& attr,
      S3ChunkInfoMap* s3_chunk_info_add = nullptr, bool internal = false) = 0;

  virtual void UpdateInodeWithOutNlinkAsync(
      uint32_t fs_id, uint64_t inode_id, const pb::metaserver::InodeAttr& attr,
      MetaServerClientDone* done, DataIndices&& indices = {}) = 0;

  virtual pb::metaserver::MetaStatusCode GetOrModifyS3ChunkInfo(
      uint32_t fs_id, uint64_t inode_id,
      const google::protobuf::Map<uint64_t, pb::metaserver::S3ChunkInfoList>&
          s3_chunk_infos,
      bool return_s3_chunk_info_map = false,
      google::protobuf::Map<uint64_t, pb::metaserver::S3ChunkInfoList>* out =
          nullptr,
      bool internal = false) = 0;

  virtual void GetOrModifyS3ChunkInfoAsync(
      uint32_t fs_id, uint64_t inode_id,
      const google::protobuf::Map<uint64_t, pb::metaserver::S3ChunkInfoList>&
          s3_chunk_infos,
      MetaServerClientDone* done) = 0;

  virtual pb::metaserver::MetaStatusCode CreateInode(
      const InodeParam& param, pb::metaserver::Inode* out) = 0;

  virtual pb::metaserver::MetaStatusCode CreateManageInode(
      const InodeParam& param, pb::metaserver::Inode* out) = 0;

  virtual pb::metaserver::MetaStatusCode DeleteInode(uint32_t fs_id,
                                                     uint64_t inodeid) = 0;

  virtual bool SplitRequestInodes(
      uint32_t fs_id, const std::set<uint64_t>& inode_ids,
      std::vector<std::vector<uint64_t>>* inode_groups) = 0;

  virtual pb::metaserver::MetaStatusCode GetFsQuota(
      uint32_t fs_id, pb::metaserver::Quota& quota) = 0;
  virtual pb::metaserver::MetaStatusCode FlushFsUsage(
      uint32_t fs_id, const pb::metaserver::Usage& usage,
      pb::metaserver::Quota& new_quota) = 0;

  virtual pb::metaserver::MetaStatusCode LoadDirQuotas(
      uint32_t fs_id,
      std::unordered_map<uint64_t, pb::metaserver::Quota>& dir_quotas) = 0;
  virtual pb::metaserver::MetaStatusCode FlushDirUsages(
      uint32_t fs_id,
      std::unordered_map<uint64_t, pb::metaserver::Usage>& dir_usages) = 0;
};

class MetaServerClientImpl : public MetaServerClient {
 public:
  MetaServerClientImpl() = default;

  pb::metaserver::MetaStatusCode Init(
      const common::ExcutorOpt& excutor_opt,
      const common::ExcutorOpt& excutor_internal_opt,
      std::shared_ptr<MetaCache> meta_cache,
      std::shared_ptr<ChannelManager<common::MetaserverID>> channel_manager)
      override;

  pb::metaserver::MetaStatusCode GetTxId(uint32_t fs_id, uint64_t inode_id,
                                         uint32_t* partition_id,
                                         uint64_t* tx_id) override;

  void SetTxId(uint32_t partition_id, uint64_t tx_id) override;

  pb::metaserver::MetaStatusCode GetDentry(
      uint32_t fs_id, uint64_t inodeid, const std::string& name,
      pb::metaserver::Dentry* out) override;

  pb::metaserver::MetaStatusCode ListDentry(
      uint32_t fs_id, uint64_t inodeid, const std::string& last, uint32_t count,
      bool only_dir, std::list<pb::metaserver::Dentry>* dentry_list) override;

  pb::metaserver::MetaStatusCode CreateDentry(
      const pb::metaserver::Dentry& dentry) override;

  pb::metaserver::MetaStatusCode DeleteDentry(
      uint32_t fs_id, uint64_t inodeid, const std::string& name,
      pb::metaserver::FsFileType type) override;

  pb::metaserver::MetaStatusCode PrepareRenameTx(
      const std::vector<pb::metaserver::Dentry>& dentrys) override;

  pb::metaserver::MetaStatusCode GetInode(uint32_t fs_id, uint64_t inodeid,
                                          pb::metaserver::Inode* out,
                                          bool* streaming) override;

  pb::metaserver::MetaStatusCode GetInodeAttr(
      uint32_t fs_id, uint64_t inodeid,
      pb::metaserver::InodeAttr* attr) override;

  pb::metaserver::MetaStatusCode BatchGetInodeAttr(
      uint32_t fs_id, const std::set<uint64_t>& inode_ids,
      std::list<pb::metaserver::InodeAttr>* attr) override;

  pb::metaserver::MetaStatusCode BatchGetInodeAttrAsync(
      uint32_t fs_id, const std::vector<uint64_t>& inode_ids,
      MetaServerClientDone* done) override;

  pb::metaserver::MetaStatusCode BatchGetXAttr(
      uint32_t fs_id, const std::set<uint64_t>& inode_ids,
      std::list<pb::metaserver::XAttr>* xattr) override;

  pb::metaserver::MetaStatusCode UpdateInodeAttr(
      uint32_t fs_id, uint64_t inode_id,
      const pb::metaserver::InodeAttr& attr) override;

  pb::metaserver::MetaStatusCode UpdateInodeAttrWithOutNlink(
      uint32_t fs_id, uint64_t inode_id, const pb::metaserver::InodeAttr& attr,
      S3ChunkInfoMap* s3_chunk_info_add = nullptr,
      bool internal = false) override;

  void UpdateInodeWithOutNlinkAsync(uint32_t fs_id, uint64_t inode_id,
                                    const pb::metaserver::InodeAttr& attr,
                                    MetaServerClientDone* done,
                                    DataIndices&& indices = {}) override;

  pb::metaserver::MetaStatusCode GetOrModifyS3ChunkInfo(
      uint32_t fs_id, uint64_t inode_id,
      const google::protobuf::Map<uint64_t, pb::metaserver::S3ChunkInfoList>&
          s3_chunk_infos,
      bool return_s3_chunk_info_map = false,
      google::protobuf::Map<uint64_t, pb::metaserver::S3ChunkInfoList>* out =
          nullptr,
      bool internal = false) override;

  void GetOrModifyS3ChunkInfoAsync(
      uint32_t fs_id, uint64_t inode_id,
      const google::protobuf::Map<uint64_t, pb::metaserver::S3ChunkInfoList>&
          s3_chunk_infos,
      MetaServerClientDone* done) override;

  pb::metaserver::MetaStatusCode CreateInode(
      const InodeParam& param, pb::metaserver::Inode* out) override;

  pb::metaserver::MetaStatusCode CreateManageInode(
      const InodeParam& param, pb::metaserver::Inode* out) override;

  pb::metaserver::MetaStatusCode DeleteInode(uint32_t fs_id,
                                             uint64_t inodeid) override;

  bool SplitRequestInodes(
      uint32_t fs_id, const std::set<uint64_t>& inode_ids,
      std::vector<std::vector<uint64_t>>* inode_groups) override;

  pb::metaserver::MetaStatusCode GetFsQuota(
      uint32_t fs_id, pb::metaserver::Quota& quota) override;
  pb::metaserver::MetaStatusCode FlushFsUsage(
      uint32_t fs_id, const pb::metaserver::Usage& usage,
      pb::metaserver::Quota& new_quota) override;

  pb::metaserver::MetaStatusCode LoadDirQuotas(
      uint32_t fs_id,
      std::unordered_map<uint64_t, pb::metaserver::Quota>& dir_quotas) override;
  pb::metaserver::MetaStatusCode FlushDirUsages(
      uint32_t fs_id,
      std::unordered_map<uint64_t, pb::metaserver::Usage>& dir_usages) override;

 private:
  pb::metaserver::MetaStatusCode UpdateInode(
      const pb::metaserver::UpdateInodeRequest& request, bool internal = false);

  void UpdateInodeAsync(const pb::metaserver::UpdateInodeRequest& request,
                        MetaServerClientDone* done);

  bool ParseS3MetaStreamBuffer(butil::IOBuf* buffer, uint64_t* chunk_index,
                               pb::metaserver::S3ChunkInfoList* list);

  bool HandleS3MetaStreamBuffer(butil::IOBuf* buffer, S3ChunkInfoMap* out);

  common::ExcutorOpt opt_;
  common::ExcutorOpt optInternal_;

  std::shared_ptr<MetaCache> metaCache_;
  std::shared_ptr<ChannelManager<common::MetaserverID>> channelManager_;

  dingofs::common::StreamClient streamClient_;
  metric::MetaServerClientMetric metric_;
};
}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_RPCCLIENT_METASERVER_CLIENT_H_
