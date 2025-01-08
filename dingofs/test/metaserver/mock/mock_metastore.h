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
 * Date: Wed Aug 18 15:10:17 CST 2021
 * Author: wuhanqing
 */

#ifndef DINGOFS_TEST_METASERVER_MOCK_MOCK_METASTORE_H_
#define DINGOFS_TEST_METASERVER_MOCK_MOCK_METASTORE_H_

#include <gmock/gmock.h>

#include <list>
#include <memory>
#include <string>

#include "metaserver/metastore.h"

namespace dingofs {
namespace metaserver {
namespace mock {

using pb::metaserver::MetaStatusCode;

class MockMetaStore : public dingofs::metaserver::MetaStore {
 public:
  MOCK_METHOD1(Load, bool(const std::string&));
  MOCK_METHOD2(Save,
               bool(const std::string&, copyset::OnSnapshotSaveDoneClosure*));
  MOCK_METHOD0(Clear, bool());
  MOCK_METHOD0(Destroy, bool());

  MOCK_METHOD2(SetFsQuota,
               MetaStatusCode(const pb::metaserver::SetFsQuotaRequest*,
                              pb::metaserver::SetFsQuotaResponse*));
  MOCK_METHOD2(GetFsQuota,
               MetaStatusCode(const pb::metaserver::GetFsQuotaRequest*,
                              pb::metaserver::GetFsQuotaResponse*));
  MOCK_METHOD2(FlushFsUsage,
               MetaStatusCode(const pb::metaserver::FlushFsUsageRequest*,
                              pb::metaserver::FlushFsUsageResponse*));
  MOCK_METHOD2(SetDirQuota,
               MetaStatusCode(const pb::metaserver::SetDirQuotaRequest*,
                              pb::metaserver::SetDirQuotaResponse*));
  MOCK_METHOD2(GetDirQuota,
               MetaStatusCode(const pb::metaserver::GetDirQuotaRequest*,
                              pb::metaserver::GetDirQuotaResponse*));
  MOCK_METHOD2(DeleteDirQuota,
               MetaStatusCode(const pb::metaserver::DeleteDirQuotaRequest*,
                              pb::metaserver::DeleteDirQuotaResponse*));
  MOCK_METHOD2(LoadDirQuotas,
               MetaStatusCode(const pb::metaserver::LoadDirQuotasRequest*,
                              pb::metaserver::LoadDirQuotasResponse*));
  MOCK_METHOD2(FlushDirUsages,
               MetaStatusCode(const pb::metaserver::FlushDirUsagesRequest*,
                              pb::metaserver::FlushDirUsagesResponse*));

  MOCK_METHOD2(CreatePartition,
               MetaStatusCode(const pb::metaserver::CreatePartitionRequest*,
                              pb::metaserver::CreatePartitionResponse*));
  MOCK_METHOD2(DeletePartition,
               MetaStatusCode(const pb::metaserver::DeletePartitionRequest*,
                              pb::metaserver::DeletePartitionResponse*));
  MOCK_METHOD1(GetPartitionInfoList,
               bool(std::list<pb::common::PartitionInfo>*));

  MOCK_METHOD2(CreateDentry,
               MetaStatusCode(const pb::metaserver::CreateDentryRequest*,
                              pb::metaserver::CreateDentryResponse*));
  MOCK_METHOD2(DeleteDentry,
               MetaStatusCode(const pb::metaserver::DeleteDentryRequest*,
                              pb::metaserver::DeleteDentryResponse*));
  MOCK_METHOD2(GetDentry,
               MetaStatusCode(const pb::metaserver::GetDentryRequest*,
                              pb::metaserver::GetDentryResponse*));
  MOCK_METHOD2(ListDentry,
               MetaStatusCode(const pb::metaserver::ListDentryRequest*,
                              pb::metaserver::ListDentryResponse*));

  MOCK_METHOD2(CreateInode,
               MetaStatusCode(const pb::metaserver::CreateInodeRequest*,
                              pb::metaserver::CreateInodeResponse*));
  MOCK_METHOD2(CreateRootInode,
               MetaStatusCode(const pb::metaserver::CreateRootInodeRequest*,
                              pb::metaserver::CreateRootInodeResponse*));
  MOCK_METHOD2(CreateManageInode,
               MetaStatusCode(const pb::metaserver::CreateManageInodeRequest*,
                              pb::metaserver::CreateManageInodeResponse*));
  MOCK_METHOD2(GetInode, MetaStatusCode(const pb::metaserver::GetInodeRequest*,
                                        pb::metaserver::GetInodeResponse*));
  MOCK_METHOD2(BatchGetInodeAttr,
               MetaStatusCode(const pb::metaserver::BatchGetInodeAttrRequest*,
                              pb::metaserver::BatchGetInodeAttrResponse*));
  MOCK_METHOD2(BatchGetXAttr,
               MetaStatusCode(const pb::metaserver::BatchGetXAttrRequest*,
                              pb::metaserver::BatchGetXAttrResponse*));
  MOCK_METHOD2(DeleteInode,
               MetaStatusCode(const pb::metaserver::DeleteInodeRequest*,
                              pb::metaserver::DeleteInodeResponse*));
  MOCK_METHOD2(UpdateInode,
               MetaStatusCode(const pb::metaserver::UpdateInodeRequest*,
                              pb::metaserver::UpdateInodeResponse*));

  MOCK_METHOD2(PrepareRenameTx,
               MetaStatusCode(const pb::metaserver::PrepareRenameTxRequest*,
                              pb::metaserver::PrepareRenameTxResponse*));

  MOCK_METHOD0(GetStreamServer, std::shared_ptr<common::StreamServer>());

  MOCK_METHOD3(GetOrModifyS3ChunkInfo,
               MetaStatusCode(
                   const pb::metaserver::GetOrModifyS3ChunkInfoRequest* request,
                   pb::metaserver::GetOrModifyS3ChunkInfoResponse* response,
                   std::shared_ptr<storage::Iterator>* iterator));

  MOCK_METHOD2(
      SendS3ChunkInfoByStream,
      MetaStatusCode(std::shared_ptr<common::StreamConnection> connection,
                     std::shared_ptr<storage::Iterator> iterator));

  MOCK_METHOD2(GetVolumeExtent,
               MetaStatusCode(const pb::metaserver::GetVolumeExtentRequest*,
                              pb::metaserver::GetVolumeExtentResponse*));

  MOCK_METHOD2(UpdateVolumeExtent,
               MetaStatusCode(const pb::metaserver::UpdateVolumeExtentRequest*,
                              pb::metaserver::UpdateVolumeExtentResponse*));
};

}  // namespace mock
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_TEST_METASERVER_MOCK_MOCK_METASTORE_H_
