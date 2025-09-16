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

#ifndef DINGOFS_MDSV2_BACKGROUND_CACHE_MEMBER_SYNC_H_
#define DINGOFS_MDSV2_BACKGROUND_CACHE_MEMBER_SYNC_H_

#include "mdsv2/cachegroup/member_manager.h"

namespace dingofs {
namespace mdsv2 {

class CacheMemberSynchronizer;
using CacheMemberSynchronizerSPtr = std::shared_ptr<CacheMemberSynchronizer>;

class CacheMemberSynchronizer {
 public:
  CacheMemberSynchronizer(CacheGroupMemberManagerSPtr cache_group_member_manager)
      : cache_group_member_manager_(cache_group_member_manager) {};
  ~CacheMemberSynchronizer() = default;

  static CacheMemberSynchronizerSPtr New(CacheGroupMemberManagerSPtr cache_group_member_manager) {
    return std::make_shared<CacheMemberSynchronizer>(cache_group_member_manager);
  }

  void Run();

 private:
  void SyncCacheMember();

  std::atomic<bool> is_running_{false};

  CacheGroupMemberManagerSPtr cache_group_member_manager_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_BACKGROUND_CACHE_MEMBER_SYNC_H_