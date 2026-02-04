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
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/node.h"

#include <brpc/reloadable_flags.h>
#include <butil/binary_printer.h>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/block_cache_impl.h"
#include "cache/cachegroup/node.h"
#include "cache/cachegroup/task_tracker.h"
#include "cache/common/context.h"
#include "cache/common/macro.h"
#include "cache/common/mds_client.h"
#include "cache/common/storage_client.h"
#include "cache/common/storage_client_pool.h"
#include "cache/iutil/string_util.h"
#include "common/const.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_string(id, "", "specified the cache node id");
DEFINE_validator(id, iutil::StringValidator);

DEFINE_string(group_name, "default", "which group this cache node belongs to");
DEFINE_validator(group_name, iutil::StringValidator);

DEFINE_uint32(group_weight, 100,
              "weight of this cache node, used for consistent hashing");

DEFINE_uint32(max_range_size_kb, 128,
              "retrieve the whole block if length reach specified size in KB");

DEFINE_bool(retrieve_storage_lock, true,
            "lock when retrieve block from storage");

DEFINE_uint32(retrieve_storage_lock_timeout_ms, 10000,
              "timeout of retrieve storage lock");
DEFINE_validator(retrieve_storage_lock_timeout_ms, brpc::PassValidate);

CacheNode::CacheNode()
    : running_(false),
      mds_client_(std::make_shared<MDSClientImpl>()),
      storage_client_pool_(
          std::make_shared<StorageClientPoolImpl>(mds_client_)),
      heartbeat_(std::make_unique<Heartbeat>(mds_client_)),
      task_tracker_(std::make_unique<TaskTracker>()),
      num_hit_cache_("dingofs_cache_hit_count"),
      num_miss_cache_("dingofs_cache_miss_count") {
  FLAGS_cache_dir_uuid = FLAGS_id;
  block_cache_ = std::make_unique<BlockCacheImpl>(storage_client_pool_);
}

Status CacheNode::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "CacheNode already started";
    return Status::OK();
  }

  LOG(INFO) << "CacheNode is starting...";

  auto status = mds_client_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start MDSClient";
    return status;
  }

  status = block_cache_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start BlockCache";
    return status;
  }

  status = JoinGroup();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to join group=" << FLAGS_group_name;
    return status;
  }

  BRPC_SCOPE_EXIT {
    if (!status.ok()) {
      LeaveGroup();
    }
  };

  heartbeat_->Start();

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "Successfully start CacheNode";
  return Status::OK();
}

Status CacheNode::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "CacheNode already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "CacheNode is shutting down...";

  heartbeat_->Shutdown();

  Status status = LeaveGroup();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to leave group=" << FLAGS_group_name;
    return status;
  }

  status = block_cache_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to shutdown BlockCache";
    return status;
  }

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "Successfully shutdown CacheNode";
  return status;
}

Status CacheNode::JoinGroup() {
  auto status =
      mds_client_->JoinCacheGroup(FLAGS_id, FLAGS_listen_ip, FLAGS_listen_port,
                                  FLAGS_group_name, FLAGS_group_weight);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to send JoinCacheGroup rpc request";
    return Status::Internal("join cache group failed");
  }

  LOG(INFO) << "Successfully join " << *this
            << " into cache group=" << FLAGS_group_name;
  return Status::OK();
}

Status CacheNode::LeaveGroup() {
  CHECK_NOTNULL(mds_client_);

  auto status = mds_client_->LeaveCacheGroup(
      FLAGS_id, FLAGS_listen_ip, FLAGS_listen_port, FLAGS_group_name);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to send LeaveCacheGroup rpc request";
    return Status::Internal("leave cache group failed");
  }

  LOG(INFO) << "Successfully leave " << *this
            << " from cache group=" << FLAGS_group_name;
  return Status::OK();
}

Status CacheNode::Put(ContextSPtr ctx, const BlockKey& key,
                      const Block& block) {
  if (!IsRunning()) {
    return Status::CacheDown("cache node is down");
  }
  return block_cache_->Put(ctx, key, block, {.writeback = true});
}

Status CacheNode::Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                        size_t length, IOBuffer* buffer, size_t block_length) {
  if (!IsRunning()) {
    return Status::CacheDown("cache node is down");
  }

  auto status = RetrieveCache(ctx, key, offset, length, buffer);
  if (status.IsNotFound()) {
    status = RetrieveStorage(ctx, key, offset, length, buffer, block_length);
  }
  return status;
}

Status CacheNode::AsyncCache(ContextSPtr ctx, const BlockKey& key,
                             const Block& block) {
  if (!IsRunning()) {
    LOG(ERROR) << "Cache node is down, skip async cache block, key="
               << key.Filename();
    return Status::CacheDown("cache node is down");
  }

  block_cache_->AsyncCache(ctx, key, block, [](Status status) {
    if (!status.ok()) {
      LOG(ERROR) << "Fail to async cache block, status=" << status.ToString();
    }
  });
  return Status::OK();
}

Status CacheNode::AsyncPrefetch(ContextSPtr ctx, const BlockKey& key,
                                size_t length) {
  if (!IsRunning()) {
    return Status::CacheDown("cache node is down");
  }

  block_cache_->AsyncPrefetch(ctx, key, length, [key](Status status) {
    if (!status.ok()) {
      LOG(ERROR) << "Fail to async prefetch block, key=" << key.Filename()
                 << ", status=" << status.ToString();
    }
  });
  return Status::OK();
}

Status CacheNode::RetrieveCache(ContextSPtr ctx, const BlockKey& key,
                                off_t offset, size_t length, IOBuffer* buffer) {
  auto status = block_cache_->Range(ctx, key, offset, length, buffer,
                                    {.retrieve_storage = false});
  if (status.ok()) {
    num_hit_cache_ << 1;
    ctx->SetCacheHit(true);
  } else {
    num_miss_cache_ << 1;
  }
  return status;
}

Status CacheNode::RetrieveStorage(ContextSPtr ctx, const BlockKey& key,
                                  off_t offset, size_t length, IOBuffer* buffer,
                                  size_t block_length) {
  StorageClient* storage_client;
  auto status =
      storage_client_pool_->GetStorageClient(key.fs_id, &storage_client);
  if (!status.ok()) {
    return status;
  }

  // Retrieve range of block: unknown block size or unreach max_range_size
  if (block_length == 0 || length < FLAGS_max_range_size_kb * kKiB) {
    return RetrievePartBlock(ctx, key, offset, length, buffer, block_length);
  }

  // Retrieve the whole block
  IOBuffer block;
  status = RetrieveWholeBlock(ctx, key, block_length, &block);
  if (status.ok()) {
    block.AppendTo(buffer, length, offset);
  }
  return status;
}

// TODO: Should we check download block task?
Status CacheNode::RetrievePartBlock(ContextSPtr ctx, const BlockKey& key,
                                    off_t offset, size_t length,
                                    IOBuffer* buffer, size_t block_length) {
  StorageClient* storage_client;
  auto status =
      storage_client_pool_->GetStorageClient(key.fs_id, &storage_client);
  if (!status.ok()) {
    return status;
  }

  status = storage_client->Range(ctx, key, offset, length, buffer);
  if (status.ok() && block_length > 0) {
    block_cache_->AsyncPrefetch(ctx, key, block_length, nullptr);
  }
  return status;
}

Status CacheNode::RetrieveWholeBlock(ContextSPtr ctx, const BlockKey& key,
                                     size_t block_length, IOBuffer* buffer) {
  Status status;
  bool created = false;
  StorageClient* storage_client;

  status = storage_client_pool_->GetStorageClient(key.fs_id, &storage_client);
  if (!status.ok()) {
    return status;
  }

  BRPC_SCOPE_EXIT {
    if (status.ok()) {
      block_cache_->AsyncCache(ctx, key, Block(*buffer),
                               [this, created, key](Status /*status*/) {
                                 if (created) {
                                   task_tracker_->RemoveTask(key);
                                 }
                               });
    }
  };

  if (FLAGS_retrieve_storage_lock) {
    DownloadTaskSPtr task;
    created = task_tracker_->GetOrCreateTask(ctx, key, block_length, task);
    if (created) {
      status = RunTask(storage_client, task);
    } else {
      status = WaitTask(task);
    }

    if (status.ok()) {
      buffer->Append(&task->Result().buffer);
      return status;
    } else if (created) {
      return status;
    }
  }

  status = storage_client->Range(ctx, key, 0, block_length, buffer);
  return status;
}

Status CacheNode::RunTask(StorageClient* storage_client,
                          DownloadTaskSPtr task) {
  const auto& attr = task->Attr();
  auto& result = task->Result();
  auto status =
      storage_client->Range(attr.ctx, attr.key, 0, attr.length, &result.buffer);
  if (!status.ok()) {
    task->Run();
    task_tracker_->RemoveTask(attr.key);
    return status;
  }

  task->Run();
  return Status::OK();
}

Status CacheNode::WaitTask(DownloadTaskSPtr task) {
  bool finished = task->Wait(FLAGS_retrieve_storage_lock_timeout_ms);
  if (finished) {
    return task->Result().status;
  }

  LOG(WARNING) << "Wait " << task
               << " timeout=" << FLAGS_retrieve_storage_lock_timeout_ms
               << " ms";
  return Status::Internal("wait download task timeout");
}

std::ostream& operator<<(std::ostream& os, const CacheNode& /*node*/) {
  os << "CacheNode{id=" << FLAGS_id << " ip=" << FLAGS_listen_ip
     << " port=" << FLAGS_listen_port << " weight=" << FLAGS_group_weight
     << "}";
  return os;
}

}  // namespace cache
}  // namespace dingofs
