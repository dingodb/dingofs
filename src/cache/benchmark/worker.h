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

/*
 * Project: DingoFS
 * Created Date: 2025-06-04
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BENCHMARK_WORKER_H_
#define DINGOFS_SRC_CACHE_BENCHMARK_WORKER_H_

#include "cache/benchmark/helper.h"
#include "cache/benchmark/reporter.h"
#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"

namespace dingofs {
namespace cache {

class Worker {
 public:
  Worker(int worker_id, BlockCacheSPtr block_cache, ReporterSPtr reporter,
         BthreadCountdownEventSPtr countdown_event);

  Status Init();
  void Run();
  void Shutdown();

 private:
  using TaskFunc = std::function<Status()>;

  BlockKeyFactory NewBlockKeyFactory() const;
  std::function<Worker::TaskFunc(const BlockKey& key)> NewTaskFactory();

  bool NeedPrepBlock() const { return IsRange() || IsAsyncRange(); }
  Status PrepBlock();

  Status PutBlock(const BlockKey& key);
  Status RangeBlock(const BlockKey& key);
  Status AsyncPutBlock(const BlockKey& key);
  Status AsyncRangeBlock(const BlockKey& key);

  void ExecuteTask(std::function<Status()> task);

  bool IsRange() const { return FLAGS_op == "range"; }
  bool IsAsyncRange() const { return FLAGS_op == "async_range"; }
  bool IsPut() const { return FLAGS_op == "put"; }
  bool IsAsyncPut() const { return FLAGS_op == "async_put"; }

  int worker_id_;
  Block block_;
  BlockCacheSPtr block_cache_;
  ReporterSPtr reporter_;
  BthreadCountdownEventSPtr countdown_event_;
  BthreadCountdownEvent inflighting_;
};

using WorkerSPtr = std::shared_ptr<Worker>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCHMARK_WORKER_H_
