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

#ifndef DINGOFS_TEST_CLIENT_MOCK_INODE_CACHE_MANAGER_H_
#define DINGOFS_TEST_CLIENT_MOCK_INODE_CACHE_MANAGER_H_

#include <gmock/gmock.h>

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <set>

#include "dingofs/metaserver.pb.h"
#include "client/vfs_old/inode_cache_manager.h"

namespace dingofs {
namespace client {

using dingofs::pb::metaserver::Inode;
using dingofs::pb::metaserver::InodeAttr;
using dingofs::pb::metaserver::XAttr;

class MockInodeCacheManager : public InodeCacheManager {
 public:
  MockInodeCacheManager() = default;
  ~MockInodeCacheManager() override = default;

  MOCK_METHOD3(Init,
               DINGOFS_ERROR(common::RefreshDataOption option,
                             std::shared_ptr<filesystem::OpenFiles> openFiles,
                             std::shared_ptr<filesystem::DeferSync> deferSync));

  MOCK_METHOD2(GetInode,
               DINGOFS_ERROR(uint64_t inodeId,
                             std::shared_ptr<InodeWrapper>& out));  // NOLINT

  MOCK_METHOD2(GetInodeAttr, DINGOFS_ERROR(uint64_t inodeId, InodeAttr* out));

  MOCK_METHOD2(BatchGetInodeAttr, DINGOFS_ERROR(std::set<uint64_t>* inodeIds,
                                                std::list<InodeAttr>* attrs));

  MOCK_METHOD3(BatchGetInodeAttrAsync,
               DINGOFS_ERROR(uint64_t parentId, std::set<uint64_t>* inodeIds,
                             std::map<uint64_t, InodeAttr>* attrs));

  MOCK_METHOD2(BatchGetXAttr, DINGOFS_ERROR(std::set<uint64_t>* inodeIds,
                                            std::list<XAttr>* xattrs));

  MOCK_METHOD2(CreateInode,
               DINGOFS_ERROR(const stub::rpcclient::InodeParam& param,
                             std::shared_ptr<InodeWrapper>& out));  // NOLINT

  MOCK_METHOD2(CreateManageInode,
               DINGOFS_ERROR(const stub::rpcclient::InodeParam& param,
                             std::shared_ptr<InodeWrapper>& out));  // NOLINT

  MOCK_METHOD1(DeleteInode, DINGOFS_ERROR(uint64_t inodeid));

  MOCK_METHOD1(ShipToFlush,
               void(const std::shared_ptr<InodeWrapper>& inodeWrapper));
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_MOCK_INODE_CACHE_MANAGER_H_
