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
 * Created Date: Monday September 10th 2018
 * Author: hzsunjianliang
 */

#ifndef SRC_IDGENERATOR_ETCD_ID_GENERATOR_H_
#define SRC_IDGENERATOR_ETCD_ID_GENERATOR_H_

#include <memory>
#include <string>

#include "mds/kvstorageclient/etcd_client.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace idgenerator {

using dingofs::kvstorage::KVStorageClient;

class IdAllocator {
 public:
  IdAllocator() = default;
  virtual ~IdAllocator() = default;

  virtual bool Init() = 0;

  virtual int GenId(uint64_t num, uint64_t* id) = 0;
};

using IdAllocatorSPtr = std::shared_ptr<IdAllocator>;
using IdAllocatorUPtr = std::unique_ptr<IdAllocator>;

class EtcdIdGenerator : public IdAllocator {
 public:
  EtcdIdGenerator(std::shared_ptr<kvstorage::KVStorageClient> client,
                  std::string key, uint64_t init_id, uint64_t bundle_size)
      : client_(client),
        key_(key),
        next_id_(init_id),
        last_alloc_id_(init_id),
        bundle_size_(bundle_size) {}

  ~EtcdIdGenerator() override = default;

  int GenId(uint64_t num, uint64_t* id) override;

  bool Init() override;

 private:
  static bool DecodeID(const std::string& value, uint64_t* out);
  static std::string EncodeID(uint64_t value);

  bool AllocateIds(uint64_t bundle_size);
  bool GetOrPutAllocId(uint64_t* alloc_id);

  // the etcd client
  std::shared_ptr<kvstorage::KVStorageClient> client_;

  // guarantee the uniqueness of the id
  utils::RWLock lock_;

  // the key of id
  std::string key_;

  // the next id can be allocated in this bunlde
  uint64_t next_id_;
  // the last id can be allocated in this bunlde
  uint64_t last_alloc_id_;
  // get the numnber of id at a time
  uint64_t bundle_size_;
};

}  // namespace idgenerator
}  // namespace dingofs

#endif  // SRC_IDGENERATOR_ETCD_ID_GENERATOR_H_
