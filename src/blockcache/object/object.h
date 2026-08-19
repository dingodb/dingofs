/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_BLOCKCACHE_OBJECT_OBJECT_H_
#define DINGOFS_BLOCKCACHE_OBJECT_OBJECT_H_

#include <bvar/bvar.h>

#include <memory>
#include <string>

#include "blockcache/common/block_handle.h"
#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/object/client.h"

namespace dingofs {
namespace blockcache {

struct ObjectPutOption {
  uint32_t max_tries = 0;
};

struct ObjectGetOption {
  uint32_t max_tries = 0;
  bool retry_notfound = false;
};

class ObjectStorage {
 public:
  explicit ObjectStorage(StorageClient* client);
  virtual ~ObjectStorage() = default;

  ObjectStorage(const ObjectStorage&) = delete;
  ObjectStorage& operator=(const ObjectStorage&) = delete;

  virtual Future<Status> Put(BlockHandle handle, BufferViews block,
                             ObjectPutOption option = {});
  virtual Future<Status> Get(BlockHandle handle, uint64_t offset,
                             uint32_t length, char* buffer,
                             ObjectGetOption option = {});

 private:
  Future<Status> PutOnce(uint64_t fs_id, const std::string& key,
                         const blockaccess::PutPayload& payload);
  Future<Status> GetOnce(uint64_t fs_id, const std::string& key,
                         uint64_t offset, uint32_t length, char* buffer);

  Future<bool> WaitBackoff(uint64_t backoff_ms);

  StorageClient* client_;
  bvar::Adder<int64_t> num_upload_retry_;
  bvar::Adder<int64_t> num_download_retry_;
  bvar::Adder<int64_t> num_download_notfound_retry_;
};

using ObjectStorageUPtr = std::unique_ptr<ObjectStorage>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_OBJECT_OBJECT_H_
