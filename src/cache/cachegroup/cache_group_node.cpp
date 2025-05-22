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

#include "cache/cachegroup/cache_group_node.h"

#include <absl/cleanup/cleanup.h>
#include <butil/iobuf.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

#include "base/filepath/filepath.h"
#include "base/math/math.h"
#include "base/string/string.h"
#include "base/time/time.h"
#include "base/timer/timer_impl.h"
#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/cachegroup/async_cache.h"
#include "cache/cachegroup/cache_group_node_heartbeat.h"
#include "cache/cachegroup/cache_group_node_metric.h"
#include "cache/common/common.h"
#include "cache/utils/block_accesser_pool.h"
#include "cache/utils/helper.h"
#include "cache/utils/local_filesystem.h"
#include "common/status.h"
#include "dingofs/cachegroup.pb.h"
#include "dingofs/mds.pb.h"
#include "options/client/rpc.h"
#include "stub/common/config.h"
#include "stub/rpcclient/base_client.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using dingofs::base::filepath::PathJoin;
using dingofs::base::math::kKiB;
using dingofs::base::string::StrFormat;
using dingofs::cache::blockcache::BlockCacheImpl;
using dingofs::cache::blockcache::BlockKey;
using dingofs::cache::utils::BlockAccesserPoolImpl;
using dingofs::blockaccess::BlockAccesserSPtr;
using dingofs::stub::common::MdsOption;
using dingofs::stub::rpcclient::MDSBaseClient;
using dingofs::stub::rpcclient::MdsClientImpl;

static void BufferDeleter(void* ptr) { delete[] static_cast<char*>(ptr); }

CacheGroupNodeImpl::CacheGroupNodeImpl(CacheGroupNodeOption option)
    : running_(false), option_(option) {
  mds_base_ = std::make_shared<MDSBaseClient>();
  mds_client_ = std::make_shared<MdsClientImpl>();
  block_accesser_pool_ = std::make_unique<BlockAccesserPoolImpl>(mds_client_);
  member_ = std::make_shared<CacheGroupNodeMemberImpl>(option, mds_client_);
  metric_ = std::make_shared<CacheGroupNodeMetric>();
  heartbeat_ = std::make_unique<CacheGroupNodeHeartbeatImpl>(
      option, member_, metric_, mds_client_);
}

Status CacheGroupNodeImpl::Start() {
  if (!running_.exchange(true)) {
    MdsOption mds_option;  // FIXME(Wine93): use new version options
    mds_option.rpcRetryOpt.addrs = option_.mds_rpc_option().addrs();
    auto rc = mds_client_->Init(mds_option, mds_base_.get());
    if (rc != FSStatusCode::OK) {
      return Status::Internal("init mds client failed");
    }

    Status status = member_->JoinGroup();
    if (!status.ok()) {
      return status;
    }

    status = InitBlockCache();
    if (!status.ok()) {
      return status;
    }

    heartbeat_->Start();
  }
  return Status::OK();
}

Status CacheGroupNodeImpl::Stop() {
  if (running_.exchange(false)) {
    heartbeat_->Stop();
    Status status = member_->LeaveGroup();
    if (!status.ok()) {
      return status;
    }

    status = async_cache_->Stop();
    if (!status.ok()) {
      return status;
    }

    status = block_cache_->Shutdown();
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

std::string CacheGroupNodeImpl::GetListenIp() { return option_.listen_ip(); }

uint32_t CacheGroupNodeImpl::GetListenPort() { return option_.listen_port(); }

void CacheGroupNodeImpl::RewriteCacheDir() {
  auto member_id = member_->GetMemberId();
  CHECK_GT(member_id, 0);
  auto& disk_cache_options = option_.block_cache_option().disk_cache_options();
  for (auto& option : disk_cache_options) {
    option.cache_dir() =
        PathJoin({option.cache_dir(), StrFormat("cache-group-%d", member_id)});
  }
}

Status CacheGroupNodeImpl::InitBlockCache() {
  RewriteCacheDir();
  block_cache_ = std::make_shared<BlockCacheImpl>(
      option_.block_cache_option(), nullptr);  // FIXME(Wine93): maybe crash?
  async_cache_ = std::make_unique<AsyncCacheImpl>(block_cache_);

  Status status = block_cache_->Init();
  if (status.ok()) {
    status = async_cache_->Start();
  }
  return status;
}

Status CacheGroupNodeImpl::HandleRangeRequest(const BlockKey& block_key,
                                              size_t block_size, off_t offset,
                                              size_t length,
                                              butil::IOBuf* buffer) {
  auto status = HandleBlockCached(block_key, offset, length, buffer);
  if (status.ok()) {
    metric_->AddCacheHit();
  } else if (status.IsNotFound()) {
    metric_->AddCacheMiss();
    status = HandleBlockMissed(block_key, block_size, offset, length, buffer);
  }
  return status;
}

Status CacheGroupNodeImpl::HandleBlockCached(const BlockKey& block_key,
                                             off_t offset, size_t length,
                                             butil::IOBuf* buffer) {
  char* data = new char[length];
  Status status = block_cache_->Range(block_key, offset, length, data, false);
  if (!status.ok()) {
    delete[] data;
  } else {
    buffer->append_user_data(data, length, BufferDeleter);
  }
  return status;
}

Status CacheGroupNodeImpl::HandleBlockMissed(const BlockKey& block_key,
                                             size_t block_size, off_t offset,
                                             size_t length,
                                             butil::IOBuf* buffer) {
  BlockAccesserSPtr block_accesser;
  auto status = block_accesser_pool_->Get(block_key.fs_id, block_accesser);
  if (!status.ok()) {
    return status;
  }

  // retrive range of block
  if (length <= option_.max_range_size_kb() * kKiB) {
    char* data = new char[length];
    status = block_accesser->Range(block_key.StoreKey(), offset, length, data);
    if (!status.ok()) {
      delete[] data;
    } else {
      buffer->append_user_data(data, length, BufferDeleter);
    }
    return status;
  }

  // retrive the whole block
  char* data = new char[block_size];
  status = block_accesser->Range(block_key.StoreKey(), offset, length, data);
  if (!status.ok()) {
    delete[] data;
    return status;
  }

  butil::IOBuf block;
  block.append_user_data(data, block_size, BufferDeleter);
  async_cache_->Cache(block_key, block);  // FIXME(Wine93): maybe buffer freed?
  block.append_to(buffer, length, offset);
  return status;
}

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs
