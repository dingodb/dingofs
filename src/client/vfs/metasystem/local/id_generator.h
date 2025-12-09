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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_LOCAL_ID_GENERATOR_H_
#define DINGOFS_SRC_CLIENT_VFS_META_LOCAL_ID_GENERATOR_H_

#include <cstdint>
#include <string>

#include "bthread/types.h"
#include "common/status.h"
#include "leveldb/db.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace local {

class IdGenerator {
 public:
  IdGenerator() = default;
  virtual ~IdGenerator() = default;

  virtual bool Init() = 0;
  virtual bool Destroy() = 0;

  virtual bool GenID(uint32_t num, uint64_t& id) = 0;
  virtual bool GenID(uint32_t num, uint64_t min_slice_id, uint64_t& id) = 0;

  virtual std::string Describe() const = 0;
};

using IdGeneratorUPtr = std::unique_ptr<IdGenerator>;

class LevelIdGenerator : public IdGenerator {
 public:
  LevelIdGenerator(leveldb::DB* db, const std::string& name, int64_t start_id,
                   int batch_size);
  ~LevelIdGenerator() override;

  static IdGeneratorUPtr New(leveldb::DB* db, const std::string& name,
                             int64_t start_id, int batch_size) {
    return std::make_unique<LevelIdGenerator>(db, name, start_id, batch_size);
  }

  bool Init() override;
  bool Destroy() override;

  bool GenID(uint32_t num, uint64_t& id) override;
  bool GenID(uint32_t num, uint64_t min_slice_id, uint64_t& id) override;

  std::string Describe() const override;

 private:
  Status GetOrPutAllocId(uint64_t& alloc_id);
  Status AllocateIds(uint32_t bundle_size);
  Status DestroyId();

  Status Get(const std::string& key, std::string& value);
  Status Put(const std::string& key, const std::string& value);

  bthread_mutex_t mutex_;

  const std::string name_;
  // the key of id
  const std::string key_;

  // the next id can be allocated in this bunlde
  uint64_t next_id_;
  // the last id can be allocated in this bunlde
  uint64_t last_alloc_id_;
  // get the numnber of id at a time
  const uint32_t batch_size_;

  bool is_destroyed_{false};

  leveldb::DB* db_{nullptr};
};

}  // namespace local
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_LOCAL_ID_GENERATOR_H_