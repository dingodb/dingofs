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
              "retrieve the whole block if length of range request is not less "
              "than this value");

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
  if (running_.exchange(true)) {
    LOG(WARNING) << "CacheNode already running";
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

  LOG(INFO) << "CacheNode is up";

  return Status::OK();
}

Status CacheNode::Shutdown() {
  if (!running_.exchange(false)) {
    LOG(WARNING) << "CacheNode already down";
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

  LOG(INFO) << "CacheNode is down";
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
                        size_t length, IOBuffer* buffer,
                        size_t block_whole_length) {
  if (!IsRunning()) {
    return Status::CacheDown("cache node is down");
  }

  auto status = RetrieveCache(ctx, key, offset, length, buffer);
  if (status.IsNotFound()) {
    status =
        RetrieveStorage(ctx, key, offset, length, buffer, block_whole_length);
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
                                  size_t block_whole_length) {
  StorageClient* storage_client;
  auto status =
      storage_client_pool_->GetStorageClient(key.fs_id, &storage_client);
  if (!status.ok()) {
    return status;
  }

  // Retrieve range of block: unknown block size or unreach max_range_size
  if (block_whole_length == 0 || length < FLAGS_max_range_size_kb * kKiB) {
    return RetrievePartBlock(ctx, storage_client, key, offset, length,
                             block_whole_length, buffer);
  }

  // Retrieve the whole block
  IOBuffer block;
  status =
      RetrieveWholeBlock(ctx, storage_client, key, block_whole_length, &block);
  if (status.ok()) {
    block.AppendTo(buffer, length, offset);
  }
  return status;
}

Status CacheNode::RetrievePartBlock(ContextSPtr ctx,
                                    StorageClient* storage_client,
                                    const BlockKey& key, off_t offset,
                                    size_t length, size_t block_whole_length,
                                    IOBuffer* buffer) {
  auto status = storage_client->Range(ctx, key, offset, length, buffer);
  if (status.ok() && block_whole_length > 0) {
    block_cache_->AsyncPrefetch(ctx, key, block_whole_length,
                                [](Status status) {});
  }
  return status;
}

Status CacheNode::RetrieveWholeBlock(ContextSPtr ctx,
                                     StorageClient* storage_client,
                                     const BlockKey& key, size_t length,
                                     IOBuffer* buffer) {
  if (!FLAGS_retrieve_storage_lock) {
    return storage_client->Range(ctx, key, 0, length, buffer);
  }

  DownloadTaskSPtr task;
  bool created = task_tracker_->GetOrCreateTask(ctx, storage_client, key,
                                                length, buffer, task);
  if (created) {
    return RunTask(task);
  }

  bool finished = task->Wait(FLAGS_retrieve_storage_lock_timeout_ms);
  if (finished) {
    return task->status;
  }

  LOG(WARNING) << "Wait download block task timeout, key=" << key.Filename()
               << ", retry it";
  auto status = storage_client->Range(ctx, key, 0, length, buffer);
  if (status.ok()) {
    AsyncCache(task, false);
  }
  return status;
}

Status CacheNode::RunTask(DownloadTaskSPtr task) {
  task->Run();
  auto status = task->status;
  if (!status.ok()) {
    task_tracker_->RemoveTask(task->key);
  } else {
    AsyncCache(task, true);
  }
  return status;
}

Status CacheNode::WaitTask(DownloadTaskSPtr task) {
  bool finished = task->Wait(FLAGS_retrieve_storage_lock_timeout_ms);
  if (finished) {
    return task->status;
  }
  return Status::Internal("download timeout");
}

void CacheNode::AsyncCache(DownloadTaskSPtr task, bool remove_task) {
  block_cache_->AsyncCache(task->ctx, task->key, Block(*task->buffer),
                           [this, task, remove_task](Status status) {
                             if (!status.ok()) {
                               LOG(ERROR) << "Fail to async cache block, key="
                                          << task->key.Filename()
                                          << ", status=" << status.ToString();
                             }

                             if (remove_task) {
                               task_tracker_->RemoveTask(task->key);
                             }
                           });
}

std::ostream& operator<<(std::ostream& os, const CacheNode& /*node*/) {
  os << "CacheNode{id=" << FLAGS_id << " ip=" << FLAGS_listen_ip
     << " port=" << FLAGS_listen_port << " weight=" << FLAGS_group_weight
     << "}";
  return os;
}

}  // namespace cache
}  // namespace dingofs
