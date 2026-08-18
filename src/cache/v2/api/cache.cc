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

#include "cache/v2/api/cache.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>
#include <thread>
#include <utility>

#include "cache/v2/api/task.h"
#include "cache/v2/common/flag_decls.h"
#include "cache/v2/common/route.h"
#include "cache/v2/core/memory/buffer.h"
#include "cache/v2/core/runtime/shard_inbox.h"
#include "cache/v2/core/runtime/smp.h"
#include "cache/v2/core/server/client_domain.h"

namespace dingofs {
namespace cache {
namespace v2 {

DEFINE_uint32(queue_depth, 4096,
              "in-flight requests per shard before AsyncX answers false");
DEFINE_validator(queue_depth, brpc::PositiveInteger);

BlockCacheImpl::BlockCacheImpl(MDSClient* mds_client, ObjectStorageUPtr storage)
    : mds_client_(mds_client), storage_(std::move(storage)) {}

BlockCacheImpl::~BlockCacheImpl() { Shutdown(); }

Status BlockCacheImpl::Start() {
  CHECK(!HasReactor()) << "Start on a shard thread";
  CHECK(runtime_ == nullptr) << "BlockCacheImpl started twice";

  LOG(INFO) << "BlockCacheImpl is starting...";

  StartRuntime();

  Status status = StartTierCache();
  if (!status.ok()) {
    StopRuntime();
    return status;
  }

  throttle_ =
      std::make_unique<InflightThrottle>(ShardCount(), FLAGS_queue_depth);

  running_.store(true, std::memory_order_release);

  LOG(INFO) << "Successfully start BlockCacheImpl{shards=" << ShardCount()
            << " queue_depth=" << FLAGS_queue_depth << "}";
  return Status::OK();
}

void BlockCacheImpl::Shutdown() {
  CHECK(!HasReactor()) << "Shutdown on a shard thread";

  if (runtime_ == nullptr) {
    return;
  }

  LOG(INFO) << "BlockCacheImpl is shutting down...";

  running_.store(false, std::memory_order_release);

  DrainInflight();
  StopTierCache();
  StopRuntime();

  LOG(INFO) << "Successfully shutdown BlockCacheImpl";
}

void BlockCacheImpl::StartRuntime() {
  RuntimeOption option;
  option.shard_count = FLAGS_shards;
  option.cpuset = FLAGS_cpuset;
  option.pin_to_cpu = FLAGS_pin_cpu;
  option.reactor.poll_mode = FLAGS_poll_mode;
  runtime_ = std::make_unique<Runtime>(option);
  runtime_->Start();

  Status status = BufferPool::InitOnAllShards(FLAGS_buffer_pool_mb << 20);
  CHECK(status.ok()) << "Fail to create the buffer pools: "
                     << status.ToString();

  worker_pool_ = std::make_unique<WorkerPool>(WorkerPool::Option{});
  worker_pool_->Start();
}

Status BlockCacheImpl::StartTierCache() {
  const bool with_remote = !FLAGS_cache_group.empty();
  LOG(INFO) << "BlockCacheImpl local=" << (FLAGS_cache_store == "disk")
            << " remote=" << with_remote
            << " rdma=" << (FLAGS_remote_rdma && with_remote)
            << " storage=" << (storage_ != nullptr);

  tier_cache_ =
      std::make_unique<ShardedTierCache>(mds_client_, std::move(storage_));
  Status status = tier_cache_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start the tier cache: " << status.ToString();
    tier_cache_.reset();
  }
  return status;
}

void BlockCacheImpl::DrainInflight() {
  while (throttle_->Inflights() != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

void BlockCacheImpl::StopTierCache() {
  tier_cache_->Shutdown();
  tier_cache_.reset();
}

void BlockCacheImpl::StopRuntime() {
  worker_pool_->Shutdown();
  worker_pool_.reset();
  BufferPool::ShutdownOnAllShards();
  runtime_->Shutdown();
  runtime_->Join();
  runtime_.reset();
}

bool BlockCacheImpl::AsyncPut(BlockHandle handle, BufferViews block,
                              AsyncCallback cb, PutOption option) {
  DCHECK_GT(block.size(), 0u);
  DCHECK_LE(block.size(), kMaxBufferViews);

  unsigned shard;
  if (!Check(handle, &shard)) {
    return false;
  }
  return SubmitTask<PutTask>(shard, handle, block, std::move(cb), option);
}

bool BlockCacheImpl::AsyncGet(BlockHandle handle, uint64_t offset,
                              uint32_t length, char* buffer, AsyncCallback cb,
                              GetOption option) {
  unsigned shard;
  if (!Check(handle, &shard)) {
    return false;
  }
  return SubmitTask<GetTask>(shard, handle, offset, length, buffer,
                             std::move(cb), option);
}

bool BlockCacheImpl::AsyncPrefetch(BlockHandle handle, AsyncCallback cb,
                                   PrefetchOption option) {
  unsigned shard;
  if (!Check(handle, &shard)) {
    return false;
  }
  return SubmitTask<PrefetchTask>(shard, handle, std::move(cb), option);
}

bool BlockCacheImpl::AsyncDelete(BlockHandle handle, AsyncCallback cb,
                                 DeleteOption option) {
  unsigned shard;
  if (!Check(handle, &shard)) {
    return false;
  }
  return SubmitTask<DeleteTask>(shard, handle, std::move(cb), option);
}

CacheStats BlockCacheImpl::GetStats() {
  CHECK(!HasReactor()) << "GetStats on a shard thread";
  ShardedTierCache* tier_cache = tier_cache_.get();
  return RunOnAndWait(0, [tier_cache] { return tier_cache->GetStats(); });
}

Status BlockCacheImpl::RegisterBuffers(void* base, size_t bytes) {
  CHECK(tier_cache_ != nullptr) << "RegisterBuffers before Start";
  if (!FLAGS_remote_rdma || mds_client_ == nullptr) {
    return Status::OK();
  }
  return ClientDomain::RegisterOnAllShards(base, bytes);
}

bool BlockCacheImpl::Check(BlockHandle handle, unsigned* shard) {
  DCHECK(!HasReactor());
  if (!running_.load(std::memory_order_acquire)) {
    return false;
  }
  *shard = OwnerIndex(handle, ShardCount());
  return throttle_->Acquire(*shard);
}

struct alignas(kCacheLineSize) BlockCacheImpl::InboxTask : InboxWork {
  virtual ~InboxTask() = default;

  BlockCacheImpl* self = nullptr;
  AsyncTask* task = nullptr;
  unsigned shard = 0;
  alignas(kCacheLineSize) Status status;
};

template <typename Task>
struct BlockCacheImpl::Context final : InboxTask {
  template <typename... Args>
  explicit Context(Args&&... args) : payload(std::forward<Args>(args)...) {
    task = &payload;
  }

  Task payload;
};

template <typename Task, typename... Args>
bool BlockCacheImpl::SubmitTask(unsigned shard, Args&&... args) {
  auto* context = new Context<Task>(std::forward<Args>(args)...);
  context->self = this;
  context->shard = shard;
  context->run = &BlockCacheImpl::RunTask;
  if (!PostTo(shard, context)) {
    delete context;
    throttle_->Release(shard);
    return false;
  }
  return true;
}

void BlockCacheImpl::RunTask(InboxWork* base) {
  InboxTask* context = static_cast<InboxTask*>(base);
  BlockCacheImpl* self = context->self;
  try {
    Future<Status> future = context->task->RunOnShard(*self->tier_cache_);
    if (future.Available()) {
      self->FinishTask(context, future.Get());
      return;
    }
    (void)self->AwaitTask(context, std::move(future));
  } catch (const std::exception& e) {
    self->FinishTask(context, Status::Internal(e.what()));
  } catch (...) {
    self->FinishTask(context, Status::Internal("the cache threw"));
  }
}

Future<> BlockCacheImpl::AwaitTask(InboxTask* context, Future<Status> future) {
  Status status;
  try {
    status = co_await std::move(future);
  } catch (const std::exception& e) {
    status = Status::Internal(e.what());
  } catch (...) {
    status = Status::Internal("the cache threw");
  }
  FinishTask(context, std::move(status));
}

void BlockCacheImpl::FinishTask(InboxTask* context, Status status) {
  context->status = std::move(status);
  context->run = &BlockCacheImpl::CompleteTask;
  worker_pool_->Post(context);
}

void BlockCacheImpl::CompleteTask(InboxWork* base) {
  InboxTask* context = static_cast<InboxTask*>(base);
  BlockCacheImpl* self = context->self;
  const unsigned shard = context->shard;
  context->task->RunOnWorker(std::move(context->status));
  delete context;
  self->throttle_->Release(shard);
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
