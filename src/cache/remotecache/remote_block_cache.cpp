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
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/remote_block_cache.h"

#include <butil/iobuf.h>

#include <memory>

#include "cache/remotecache/remote_node_group.h"
#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {
namespace remotecache {

using dingofs::stub::common::MdsOption;
using dingofs::stub::rpcclient::MdsClientImpl;

RemoteBlockCacheImpl::RemoteBlockCacheImpl(RemoteBlockCacheOption option)
    : inited_(false),
      option_(option),
      mds_base_(std::make_shared<MDSBaseClient>()),
      mds_client_(std::make_shared<MdsClientImpl>()),
      node_group_(std::make_unique<RemoteNodeGroupImpl>(option, mds_client_)) {}

Status RemoteBlockCacheImpl::Init() {
  if (!inited_.exchange(true)) {
    MdsOption mds_option;  // FIXME(Wine93): use new version options
    mds_option.rpcRetryOpt.addrs = option_.mds_rpc_option().addrs();
    auto rc = mds_client_->Init(mds_option, mds_base_.get());
    if (rc != FSStatusCode::OK) {
      return Status::Internal("init mds client failed");
    }

    return node_group_->Start();
  }
  return Status::OK();
}

void RemoteBlockCacheImpl::Shutdown() {}

Status RemoteBlockCacheImpl::Range(const BlockKey& block_key, size_t block_size,
                                   off_t offset, size_t length,
                                   butil::IOBuf* buffer) {
  auto node = node_group_->Get(block_key.Filename());
  return node->Range(block_key, block_size, offset, length, buffer);
}

}  // namespace remotecache
}  // namespace cache
}  // namespace dingofs
