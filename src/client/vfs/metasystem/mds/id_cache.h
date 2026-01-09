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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_ID_CACHE_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_ID_CACHE_H_

#include <gflags/gflags_declare.h>

#include "client/vfs/metasystem/mds/mds_client.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

DECLARE_uint32(client_vfs_id_cache_batch_size);

class IdCache {
 public:
  IdCache(const std::string& name, MDSClient& mds_client,
          uint32_t batch_size = FLAGS_client_vfs_id_cache_batch_size)
      : name_(name), mds_client_(mds_client), batch_size_(batch_size) {}

  bool GenID(uint64_t& id);
  bool GenID(uint32_t num, uint64_t& id);

 private:
  Status AllocateIds(uint32_t size);

  const std::string name_;

  MDSClient& mds_client_;

  utils::RWLock lock_;

  uint64_t next_id_{0};
  uint64_t last_alloc_id_{0};
  uint32_t batch_size_{0};
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_ID_CACHE_H_