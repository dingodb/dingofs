/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: Thur May 12 2022
 * Author: wanghai01
 */

#ifndef DINGOFS_SRC_CLIENT_XATTR_MANAGER_H_
#define DINGOFS_SRC_CLIENT_XATTR_MANAGER_H_

#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <stack>
#include <string>
#include <unordered_map>

#include "proto/metaserver.pb.h"
#include "client/client_operator.h"
#include "client/filesystem/error.h"
#include "utils/interruptible_sleeper.h"

#define DirectIOAlignment 512

namespace dingofs {
namespace client {

struct SummaryInfo {
  uint64_t files = 0;
  uint64_t subdirs = 0;
  uint64_t entries = 0;
  uint64_t fbytes = 0;
};

class XattrManager {
 public:
  XattrManager(const std::shared_ptr<InodeCacheManager>& inodeManager,
               const std::shared_ptr<DentryCacheManager>& dentryManager,
               uint32_t listDentryLimit, uint32_t listDentryThreads)
      : inodeManager_(inodeManager),
        dentryManager_(dentryManager),
        listDentryLimit_(listDentryLimit),
        listDentryThreads_(listDentryThreads),
        isStop_(false) {}

  ~XattrManager() {}

  void Stop() { isStop_.store(true); }

  DINGOFS_ERROR GetXattr(const char* name, std::string* value,
                         pb::metaserver::InodeAttr* attr, bool enableSumInDir);

  DINGOFS_ERROR UpdateParentInodeXattr(uint64_t parentId,
                                       const pb::metaserver::XAttr& xattr,
                                       bool direction);

  DINGOFS_ERROR UpdateParentXattrAfterRename(uint64_t parent,
                                             uint64_t newparent,
                                             const char* newname,
                                             RenameOperator* renameOp);

 private:
  bool ConcurrentListDentry(std::list<pb::metaserver::Dentry>* dentrys,
                            std::stack<uint64_t>* iStack,
                            std::mutex* stackMutex, bool dirOnly,
                            utils::Atomic<uint32_t>* inflightNum,
                            utils::Atomic<bool>* ret);

  void ConcurrentGetInodeAttr(
      std::stack<uint64_t>* iStack, std::mutex* stackMutex,
      std::unordered_map<uint64_t, uint64_t>* hardLinkMap, std::mutex* mapMutex,
      SummaryInfo* summaryInfo, std::mutex* valueMutex,
      utils::Atomic<uint32_t>* inflightNum, utils::Atomic<bool>* ret);

  void ConcurrentGetInodeXattr(std::stack<uint64_t>* iStack,
                               std::mutex* stackMutex,
                               pb::metaserver::InodeAttr* attr,
                               std::mutex* inodeMutex,
                               utils::Atomic<uint32_t>* inflightNum,
                               utils::Atomic<bool>* ret);

  DINGOFS_ERROR CalOneLayerSumInfo(pb::metaserver::InodeAttr* attr);

  DINGOFS_ERROR CalAllLayerSumInfo(pb::metaserver::InodeAttr* attr);

  DINGOFS_ERROR FastCalOneLayerSumInfo(pb::metaserver::InodeAttr* attr);

  DINGOFS_ERROR FastCalAllLayerSumInfo(pb::metaserver::InodeAttr* attr);

  // inode cache manager
  std::shared_ptr<InodeCacheManager> inodeManager_;

  // dentry cache manager
  std::shared_ptr<DentryCacheManager> dentryManager_;

  utils::InterruptibleSleeper sleeper_;

  uint32_t listDentryLimit_;

  uint32_t listDentryThreads_;

  utils::Atomic<bool> isStop_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_XATTR_MANAGER_H_
