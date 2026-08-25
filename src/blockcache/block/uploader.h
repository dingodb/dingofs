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

#ifndef DINGOFS_BLOCKCACHE_BLOCK_UPLOADER_H_
#define DINGOFS_BLOCKCACHE_BLOCK_UPLOADER_H_

#include <deque>
#include <memory>

#include "blockcache/object/object.h"
#include "blockcache/store/cache_store.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {

class Uploader final {
 public:
  Uploader(CacheStore* store, ObjectStorage* storage);
  Uploader(const Uploader&) = delete;
  Uploader& operator=(const Uploader&) = delete;

  Future<> Start();
  Future<> Shutdown();

  void Enqueue(BlockHandle handle);

 private:
  static constexpr uint32_t kIdleSleepMs = 100;

  struct UploadingBlock {
    BlockHandle handle;
    Gate::Holder holder;
  };

  Future<> Worker();
  Future<> UploadOne(UploadingBlock block);
  Future<Status> SendToStorage(BlockHandle handle);
  Future<> RetryLater(UploadingBlock block);

  static bool IsGone(const Status& status) {
    return status.IsNotFound() || status.IsNotExist();
  }

  bool running_ = false;
  CacheStore* store_;
  ObjectStorage* storage_;
  unsigned inflight_;
  const unsigned max_inflight_;
  std::deque<BlockHandle> queue_;
  Gate gate_;
};

using UploaderUPtr = std::unique_ptr<Uploader>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_BLOCK_UPLOADER_H_
