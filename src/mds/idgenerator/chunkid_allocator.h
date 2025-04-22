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
 * Created Date: 2021-08-27
 * Author: chengyi01
 */

#ifndef DINGOFS_SRC_MDS_CHUNKID_ALLOCATOR_H_
#define DINGOFS_SRC_MDS_CHUNKID_ALLOCATOR_H_

#include <cstdint>
#include <memory>
#include <string>

#include "mds/common/storage_key.h"
#include "mds/idgenerator/etcd_id_generator.h"
#include "mds/kvstorageclient/etcd_client.h"

namespace dingofs {
namespace mds {

const uint64_t CHUNKIDINITIALIZE = 0;
const uint64_t CHUNKBUNDLEALLOCATED = 1000;

class ChunkIdAllocator {
 public:
  ChunkIdAllocator(std::shared_ptr<kvstorage::KVStorageClient> client = nullptr,
                   std::string key = CHUNKID_NAME_KEY_PREFIX,
                   uint64_t init_id = CHUNKIDINITIALIZE,
                   uint64_t bundle_size = CHUNKBUNDLEALLOCATED)
      : id_allocator_(std::make_unique<idgenerator::EtcdIdGenerator>(
            client, key, init_id, bundle_size)) {}

  ~ChunkIdAllocator() = default;

  bool Init() { return id_allocator_->Init(); }

  int GenChunkId(uint64_t num, uint64_t* chunk_id) {
    return id_allocator_->GenId(num, chunk_id);
  }

 private:
  idgenerator::IdAllocatorUPtr id_allocator_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_CHUNKID_ALLOCATOR_H_
