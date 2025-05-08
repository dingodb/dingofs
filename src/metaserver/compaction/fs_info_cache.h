/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef METASERVER_COMPACT_FS_INFO_CACHE_H_
#define METASERVER_COMPACT_FS_INFO_CACHE_H_

#include <braft/closure_helper.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "dingofs/mds.pb.h"

namespace dingofs {
namespace metaserver {

class FsInfoCache {
 public:
  explicit FsInfoCache(uint64_t capacity,
                       const std::vector<std::string>& mds_addrs,
                       butil::EndPoint metaserver_addr)
      : capacity_(capacity),
        mds_addrs_(mds_addrs),
        metaserver_addr_(metaserver_addr) {}

  virtual ~FsInfoCache() = default;

  virtual Status RequestFsInfo(uint64_t fsid, pb::mds::FsInfo* fs_info);

  virtual Status GetFsInfo(uint64_t fsid, pb::mds::FsInfo* fs_info);

  virtual void InvalidateFsInfo(uint64_t fsid);

 private:
  std::mutex mtx_;
  uint64_t capacity_;
  std::vector<std::string> mds_addrs_;
  uint64_t mds_index_{0};
  butil::EndPoint metaserver_addr_;

  // lru cache
  std::unordered_map<uint64_t, pb::mds::FsInfo> cache_;
  std::list<uint64_t> recent_;
  std::unordered_map<uint64_t, std::list<uint64_t>::iterator> pos_;

  void UpdateRecentUnlocked(uint64_t fsid);

  pb::mds::FsInfo GetUnlocked(uint64_t fsid);

  void PutUnlocked(uint64_t fsid, const pb::mds::FsInfo& fs_info);
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // METASERVER_COMPACT_FS_INFO_CACHE_H_
