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

#ifndef DINGOFS_CACHE_V2_OBJECT_CLIENT_H_
#define DINGOFS_CACHE_V2_OBJECT_CLIENT_H_

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "cache/v2/common/mds_client.h"
#include "common/blockaccess/block_accesser.h"

namespace dingofs {
namespace cache {
namespace v2 {

class StorageClient {
 public:
  explicit StorageClient(MDSClient* mds_client);
  virtual ~StorageClient();

  StorageClient(const StorageClient&) = delete;
  StorageClient& operator=(const StorageClient&) = delete;

  void Shutdown();

  virtual Status GetOrCreate(uint64_t fs_id,
                             blockaccess::BlockAccesser** accesser);

  bool running() const { return running_.load(std::memory_order_relaxed); }

 private:
  Status Create(uint64_t fs_id);

  std::atomic<bool> running_{true};
  MDSClient* mds_client_;
  std::shared_mutex mutex_;
  std::unordered_map<uint64_t, blockaccess::BlockAccesserUPtr> accessers_;
};

using StorageClientUPtr = std::unique_ptr<StorageClient>;

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_OBJECT_CLIENT_H_
