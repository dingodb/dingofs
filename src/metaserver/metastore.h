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
 * @Project: dingo
 * @Date: 2021-08-30 19:42:18
 * @Author: chenwei
 */

#ifndef DINGOFS_SRC_METASERVER_METASTORE_H_
#define DINGOFS_SRC_METASERVER_METASTORE_H_

#include <gtest/gtest_prod.h>

#include <list>
#include <map>
#include <memory>
#include <string>

#include "proto/metaserver.pb.h"
#include "common/rpc_stream.h"
#include "metaserver/copyset/snapshot_closure.h"
#include "metaserver/metastore_fstream.h"
#include "metaserver/partition.h"
#include "metaserver/storage/iterator.h"
#include "metaserver/superpartition/super_partition.h"

namespace dingofs {
namespace metaserver {

namespace copyset {
class CopysetNode;
}  // namespace copyset

using S3ChunkInfoMap = google::protobuf::Map<uint64_t, S3ChunkInfoList>;

class MetaStore {
 public:
  MetaStore() = default;
  virtual ~MetaStore() = default;

  virtual bool Load(const std::string& pathname) = 0;
  virtual bool Save(const std::string& dir,
                    copyset::OnSnapshotSaveDoneClosure* done) = 0;
  virtual bool Clear() = 0;
  virtual bool Destroy() = 0;

  // super partition
  virtual pb::metaserver::MetaStatusCode SetFsQuota(
      const pb::metaserver::SetFsQuotaRequest* request,
      pb::metaserver::SetFsQuotaResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode GetFsQuota(
      const pb::metaserver::GetFsQuotaRequest* request,
      pb::metaserver::GetFsQuotaResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode FlushFsUsage(
      const pb::metaserver::FlushFsUsageRequest* request,
      pb::metaserver::FlushFsUsageResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode SetDirQuota(
      const pb::metaserver::SetDirQuotaRequest* request,
      pb::metaserver::SetDirQuotaResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode GetDirQuota(
      const pb::metaserver::GetDirQuotaRequest* request,
      pb::metaserver::GetDirQuotaResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode DeleteDirQuota(
      const pb::metaserver::DeleteDirQuotaRequest* request,
      pb::metaserver::DeleteDirQuotaResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode LoadDirQuotas(
      const pb::metaserver::LoadDirQuotasRequest* request,
      pb::metaserver::LoadDirQuotasResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode FlushDirUsages(
      const pb::metaserver::FlushDirUsagesRequest* request,
      pb::metaserver::FlushDirUsagesResponse* response) = 0;

  // partition
  virtual pb::metaserver::MetaStatusCode CreatePartition(
      const pb::metaserver::CreatePartitionRequest* request,
      pb::metaserver::CreatePartitionResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode DeletePartition(
      const pb::metaserver::DeletePartitionRequest* request,
      pb::metaserver::DeletePartitionResponse* response) = 0;

  virtual bool GetPartitionInfoList(
      std::list<pb::common::PartitionInfo>* partitionInfoList) = 0;

  virtual std::shared_ptr<common::StreamServer> GetStreamServer() = 0;

  // dentry
  virtual pb::metaserver::MetaStatusCode CreateDentry(
      const pb::metaserver::CreateDentryRequest* request,
      pb::metaserver::CreateDentryResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode GetDentry(
      const pb::metaserver::GetDentryRequest* request,
      pb::metaserver::GetDentryResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode DeleteDentry(
      const pb::metaserver::DeleteDentryRequest* request,
      pb::metaserver::DeleteDentryResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode ListDentry(
      const pb::metaserver::ListDentryRequest* request,
      pb::metaserver::ListDentryResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode PrepareRenameTx(
      const pb::metaserver::PrepareRenameTxRequest* request,
      pb::metaserver::PrepareRenameTxResponse* response) = 0;

  // inode
  virtual pb::metaserver::MetaStatusCode CreateInode(
      const pb::metaserver::CreateInodeRequest* request,
      pb::metaserver::CreateInodeResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode CreateRootInode(
      const pb::metaserver::CreateRootInodeRequest* request,
      pb::metaserver::CreateRootInodeResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode CreateManageInode(
      const pb::metaserver::CreateManageInodeRequest* request,
      pb::metaserver::CreateManageInodeResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode GetInode(
      const pb::metaserver::GetInodeRequest* request,
      pb::metaserver::GetInodeResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode BatchGetInodeAttr(
      const pb::metaserver::BatchGetInodeAttrRequest* request,
      pb::metaserver::BatchGetInodeAttrResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode BatchGetXAttr(
      const pb::metaserver::BatchGetXAttrRequest* request,
      pb::metaserver::BatchGetXAttrResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode DeleteInode(
      const pb::metaserver::DeleteInodeRequest* request,
      pb::metaserver::DeleteInodeResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode UpdateInode(
      const pb::metaserver::UpdateInodeRequest* request,
      pb::metaserver::UpdateInodeResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode GetOrModifyS3ChunkInfo(
      const pb::metaserver::GetOrModifyS3ChunkInfoRequest* request,
      pb::metaserver::GetOrModifyS3ChunkInfoResponse* response,
      std::shared_ptr<storage::Iterator>* iterator) = 0;

  virtual pb::metaserver::MetaStatusCode SendS3ChunkInfoByStream(
      std::shared_ptr<common::StreamConnection> connection,
      std::shared_ptr<storage::Iterator> iterator) = 0;

  virtual pb::metaserver::MetaStatusCode GetVolumeExtent(
      const pb::metaserver::GetVolumeExtentRequest* request,
      pb::metaserver::GetVolumeExtentResponse* response) = 0;

  virtual pb::metaserver::MetaStatusCode UpdateVolumeExtent(
      const pb::metaserver::UpdateVolumeExtentRequest* request,
      pb::metaserver::UpdateVolumeExtentResponse* response) = 0;
};

class MetaStoreImpl : public MetaStore {
 public:
  static std::unique_ptr<MetaStoreImpl> Create(
      copyset::CopysetNode* node,
      const storage::StorageOptions& storageOptions);

  bool Load(const std::string& checkpoint) override;
  bool Save(const std::string& dir,
            copyset::OnSnapshotSaveDoneClosure* done) override;
  bool Clear() override;
  bool Destroy() override;

  // super partition
  pb::metaserver::MetaStatusCode SetFsQuota(
      const pb::metaserver::SetFsQuotaRequest* request,
      pb::metaserver::SetFsQuotaResponse* response) override;

  pb::metaserver::MetaStatusCode GetFsQuota(
      const pb::metaserver::GetFsQuotaRequest* request,
      pb::metaserver::GetFsQuotaResponse* response) override;

  pb::metaserver::MetaStatusCode FlushFsUsage(
      const pb::metaserver::FlushFsUsageRequest* request,
      pb::metaserver::FlushFsUsageResponse* response) override;

  pb::metaserver::MetaStatusCode SetDirQuota(
      const pb::metaserver::SetDirQuotaRequest* request,
      pb::metaserver::SetDirQuotaResponse* response) override;

  pb::metaserver::MetaStatusCode GetDirQuota(
      const pb::metaserver::GetDirQuotaRequest* request,
      pb::metaserver::GetDirQuotaResponse* response) override;

  pb::metaserver::MetaStatusCode DeleteDirQuota(
      const pb::metaserver::DeleteDirQuotaRequest* request,
      pb::metaserver::DeleteDirQuotaResponse* response) override;

  pb::metaserver::MetaStatusCode LoadDirQuotas(
      const pb::metaserver::LoadDirQuotasRequest* request,
      pb::metaserver::LoadDirQuotasResponse* response) override;

  pb::metaserver::MetaStatusCode FlushDirUsages(
      const pb::metaserver::FlushDirUsagesRequest* request,
      pb::metaserver::FlushDirUsagesResponse* response) override;

  // partition
  pb::metaserver::MetaStatusCode CreatePartition(
      const pb::metaserver::CreatePartitionRequest* request,
      pb::metaserver::CreatePartitionResponse* response) override;

  pb::metaserver::MetaStatusCode DeletePartition(
      const pb::metaserver::DeletePartitionRequest* request,
      pb::metaserver::DeletePartitionResponse* response) override;

  bool GetPartitionInfoList(
      std::list<pb::common::PartitionInfo>* partitionInfoList) override;

  std::shared_ptr<common::StreamServer> GetStreamServer() override;

  // dentry
  pb::metaserver::MetaStatusCode CreateDentry(
      const pb::metaserver::CreateDentryRequest* request,
      pb::metaserver::CreateDentryResponse* response) override;

  pb::metaserver::MetaStatusCode GetDentry(
      const pb::metaserver::GetDentryRequest* request,
      pb::metaserver::GetDentryResponse* response) override;

  pb::metaserver::MetaStatusCode DeleteDentry(
      const pb::metaserver::DeleteDentryRequest* request,
      pb::metaserver::DeleteDentryResponse* response) override;

  pb::metaserver::MetaStatusCode ListDentry(
      const pb::metaserver::ListDentryRequest* request,
      pb::metaserver::ListDentryResponse* response) override;

  pb::metaserver::MetaStatusCode PrepareRenameTx(
      const pb::metaserver::PrepareRenameTxRequest* request,
      pb::metaserver::PrepareRenameTxResponse* response) override;

  // inode
  pb::metaserver::MetaStatusCode CreateInode(
      const pb::metaserver::CreateInodeRequest* request,
      pb::metaserver::CreateInodeResponse* response) override;

  pb::metaserver::MetaStatusCode CreateRootInode(
      const pb::metaserver::CreateRootInodeRequest* request,
      pb::metaserver::CreateRootInodeResponse* response) override;

  pb::metaserver::MetaStatusCode CreateManageInode(
      const pb::metaserver::CreateManageInodeRequest* request,
      pb::metaserver::CreateManageInodeResponse* response) override;

  pb::metaserver::MetaStatusCode GetInode(
      const pb::metaserver::GetInodeRequest* request,
      pb::metaserver::GetInodeResponse* response) override;

  pb::metaserver::MetaStatusCode BatchGetInodeAttr(
      const pb::metaserver::BatchGetInodeAttrRequest* request,
      pb::metaserver::BatchGetInodeAttrResponse* response) override;

  pb::metaserver::MetaStatusCode BatchGetXAttr(
      const pb::metaserver::BatchGetXAttrRequest* request,
      pb::metaserver::BatchGetXAttrResponse* response) override;

  pb::metaserver::MetaStatusCode DeleteInode(
      const pb::metaserver::DeleteInodeRequest* request,
      pb::metaserver::DeleteInodeResponse* response) override;

  pb::metaserver::MetaStatusCode UpdateInode(
      const pb::metaserver::UpdateInodeRequest* request,
      pb::metaserver::UpdateInodeResponse* response) override;

  std::shared_ptr<Partition> GetPartition(uint32_t partitionId);

  pb::metaserver::MetaStatusCode GetOrModifyS3ChunkInfo(
      const pb::metaserver::GetOrModifyS3ChunkInfoRequest* request,
      pb::metaserver::GetOrModifyS3ChunkInfoResponse* response,
      std::shared_ptr<storage::Iterator>* iterator) override;

  pb::metaserver::MetaStatusCode SendS3ChunkInfoByStream(
      std::shared_ptr<common::StreamConnection> connection,
      std::shared_ptr<storage::Iterator> iterator) override;

  pb::metaserver::MetaStatusCode GetVolumeExtent(
      const pb::metaserver::GetVolumeExtentRequest* request,
      pb::metaserver::GetVolumeExtentResponse* response) override;

  pb::metaserver::MetaStatusCode UpdateVolumeExtent(
      const pb::metaserver::UpdateVolumeExtentRequest* request,
      pb::metaserver::UpdateVolumeExtentResponse* response) override;

 private:
  FRIEND_TEST(MetastoreTest, partition);
  FRIEND_TEST(MetastoreTest, test_inode);
  FRIEND_TEST(MetastoreTest, test_dentry);
  FRIEND_TEST(MetastoreTest, persist_success);
  FRIEND_TEST(MetastoreTest, DISABLED_persist_deleting_partition_success);
  FRIEND_TEST(MetastoreTest, persist_partition_fail);
  FRIEND_TEST(MetastoreTest, persist_dentry_fail);
  FRIEND_TEST(MetastoreTest, testBatchGetInodeAttr);
  FRIEND_TEST(MetastoreTest, testBatchGetXAttr);
  FRIEND_TEST(MetastoreTest, GetOrModifyS3ChunkInfo);
  FRIEND_TEST(MetastoreTest, GetInodeWithPaddingS3Meta);
  FRIEND_TEST(MetastoreTest, TestUpdateVolumeExtent_PartitionNotFound);
  FRIEND_TEST(MetastoreTest, persist_deleting_partition_success);

  MetaStoreImpl(copyset::CopysetNode* node,
                const storage::StorageOptions& storageOptions);

  void PrepareStreamBuffer(butil::IOBuf* buffer, uint64_t chunkIndex,
                           const std::string& value);

  void SaveBackground(const std::string& path, storage::DumpFileClosure* child,
                      copyset::OnSnapshotSaveDoneClosure* done);

  bool InitStorage();

  // Clear data and stop background tasks
  // REQUIRES: rwLock_ is held with write permission
  bool ClearInternal();

  utils::RWLock rwLock_;  // protect partitionMap_
  std::shared_ptr<storage::KVStorage> kvStorage_;
  std::unique_ptr<superpartition::SuperPartition> super_partition_;
  std::map<uint32_t, std::shared_ptr<Partition>> partitionMap_;
  std::list<uint32_t> partitionIds_;

  copyset::CopysetNode* copysetNode_;

  std::shared_ptr<common::StreamServer> streamServer_;

  storage::StorageOptions storageOptions_;
};

}  // namespace metaserver
}  // namespace dingofs
#endif  // DINGOFS_SRC_METASERVER_METASTORE_H_
