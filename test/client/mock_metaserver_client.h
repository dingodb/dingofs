/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef DINGOFS_TEST_CLIENT_MOCK_METASERVER_CLIENT_H_
#define DINGOFS_TEST_CLIENT_MOCK_METASERVER_CLIENT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <list>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dingofs/metaserver.pb.h"
#include "stub/common/common.h"
#include "stub/common/config.h"
#include "stub/rpcclient/metaserver_client.h"

namespace dingofs {
namespace stub {
namespace rpcclient {

using common::ExcutorOpt;
using common::MetaserverID;

using pb::metaserver::Dentry;
using pb::metaserver::FsFileType;
using pb::metaserver::Inode;
using pb::metaserver::InodeAttr;
using pb::metaserver::MetaStatusCode;
using pb::metaserver::Quota;
using pb::metaserver::S3ChunkInfoList;
using pb::metaserver::Usage;
using pb::metaserver::VolumeExtentList;
using pb::metaserver::XAttr;

class MockMetaServerClient : public MetaServerClient {
 public:
  MOCK_METHOD(MetaStatusCode, Init,
              (const ExcutorOpt& excutorOpt,
               const ExcutorOpt& excutorInternalOpt,
               std::shared_ptr<MetaCache> metaCache,
               std::shared_ptr<ChannelManager<MetaserverID>> channelManager),
              (override));

  MOCK_METHOD(MetaStatusCode, GetTxId,
              (uint32_t fsId, uint64_t inodeId, uint32_t* partitionId,
               uint64_t* txId),
              (override));

  MOCK_METHOD(void, SetTxId, (uint32_t partitionId, uint64_t txId), (override));

  MOCK_METHOD(MetaStatusCode, GetDentry,
              (uint32_t fsId, uint64_t inodeid, const std::string& name,
               Dentry* out),
              (override));

  MOCK_METHOD(MetaStatusCode, ListDentry,
              (uint32_t fsId, uint64_t inodeid, const std::string& last,
               uint32_t count, bool onlyDir, std::list<Dentry>* dentryList),
              (override));

  MOCK_METHOD(MetaStatusCode, CreateDentry, (const Dentry& dentry), (override));

  MOCK_METHOD(MetaStatusCode, DeleteDentry,
              (uint32_t fsId, uint64_t inodeid, const std::string& name,
               FsFileType type),
              (override));

  MOCK_METHOD(MetaStatusCode, PrepareRenameTx,
              (const std::vector<Dentry>& dentrys), (override));

  MOCK_METHOD(MetaStatusCode, GetInode,
              (uint32_t fsId, uint64_t inodeid, Inode* out, bool* streaming),
              (override));

  MOCK_METHOD(MetaStatusCode, GetInodeAttr,
              (uint32_t fsId, uint64_t inodeid, InodeAttr* attr), (override));

  MOCK_METHOD(MetaStatusCode, BatchGetInodeAttr,
              (uint32_t fsId, const std::set<uint64_t>& inodeIds,
               std::list<InodeAttr>* attr),
              (override));

  MOCK_METHOD(MetaStatusCode, BatchGetInodeAttrAsync,
              (uint32_t fsId, const std::vector<uint64_t>& inodeIds,
               MetaServerClientDone* done),
              (override));

  MOCK_METHOD(MetaStatusCode, BatchGetXAttr,
              (uint32_t fsId, const std::set<uint64_t>& inodeIds,
               std::list<XAttr>* xattr),
              (override));

  MOCK_METHOD(MetaStatusCode, UpdateInodeAttr,
              (uint32_t, uint64_t, const InodeAttr&), (override));

  MOCK_METHOD(MetaStatusCode, UpdateInodeAttrWithOutNlink,
              (uint32_t, uint64_t, const InodeAttr&,
               S3ChunkInfoMap* s3ChunkInfoAdd, bool internal),
              (override));

  // Workaround for rvalue parameters
  // https://stackoverflow.com/questions/12088537/workaround-for-gmock-to-support-rvalue-reference
  void UpdateInodeWithOutNlinkAsync(uint32_t fsId, uint64_t inodeId,
                                    const InodeAttr& attr,
                                    MetaServerClientDone* done,
                                    DataIndices&& indices) override {
    return UpdateInodeWithOutNlinkAsync_rvr(fsId, inodeId, attr, done,
                                            std::move(indices));
  }

  MOCK_METHOD(void, UpdateInodeWithOutNlinkAsync_rvr,
              (uint32_t, uint64_t, const InodeAttr&, MetaServerClientDone* done,
               DataIndices));

  MOCK_METHOD(
      MetaStatusCode, GetOrModifyS3ChunkInfo,
      (uint32_t fsId, uint64_t inodeId,
       (const google::protobuf::Map<uint64_t, S3ChunkInfoList>& s3ChunkInfos),
       bool returnS3ChunkInfoMap,
       (google::protobuf::Map<uint64_t, S3ChunkInfoList> * out), bool internal),
      (override));

  MOCK_METHOD(
      void, GetOrModifyS3ChunkInfoAsync,
      (uint32_t fsId, uint64_t inodeId,
       (const google::protobuf::Map<uint64_t, S3ChunkInfoList>& s3ChunkInfos),
       MetaServerClientDone* done),
      (override));

  MOCK_METHOD(MetaStatusCode, CreateInode,
              (const InodeParam& param, Inode* out), (override));

  MOCK_METHOD(MetaStatusCode, CreateManageInode,
              (const InodeParam& param, Inode* out), (override));

  MOCK_METHOD(MetaStatusCode, DeleteInode, (uint32_t fsId, uint64_t inodeid),
              (override));

  MOCK_METHOD(bool, SplitRequestInodes,
              (uint32_t fsId, const std::set<uint64_t>& inodeIds,
               std::vector<std::vector<uint64_t>>* inodeGroups),
              (override));

  MOCK_METHOD(void, AsyncUpdateVolumeExtent,
              (uint32_t, uint64_t, const VolumeExtentList&,
               MetaServerClientDone*),
              (override));

  MOCK_METHOD(MetaStatusCode, GetVolumeExtent,
              (uint32_t, uint64_t, bool, VolumeExtentList*), (override));
  MOCK_METHOD(MetaStatusCode, GetFsQuota, (uint32_t fs_id, Quota& quota),
              (override));

  MOCK_METHOD(MetaStatusCode, FlushFsUsage,
              (uint32_t fs_id, const Usage& usage, Quota& new_quota),
              (override));

  MOCK_METHOD(MetaStatusCode, LoadDirQuotas,
              (uint32_t fs_id,
               (std::unordered_map<uint64_t, Quota> & dir_quotas)),
              (override));

  MOCK_METHOD(MetaStatusCode, FlushDirUsages,
              (uint32_t fs_id,
               (std::unordered_map<uint64_t, Usage> & dir_usages)),
              (override));
};

}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_MOCK_METASERVER_CLIENT_H_
