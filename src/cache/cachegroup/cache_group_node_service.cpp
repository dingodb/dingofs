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
 * Created Date: 2025-01-08
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/cache_group_node_service.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <butil/iobuf.h>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/errno.h"
#include "cache/common/io_buffer.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using cache::blockcache::BlockKey;
using cache::common::IOBufBuffer;
using cache::common::PbErr;

CacheGroupNodeServiceImpl::CacheGroupNodeServiceImpl(
    std::shared_ptr<CacheGroupNode> node)
    : node_(node) {}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Range) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(controller);

  butil::IOBuf buffer;
  BlockKey block_key(request->block_key());
  auto rc = node_->HandleRange(block_key, request->block_size(),
                               request->offset(), request->length(), &buffer);
  response->set_status(PbErr(rc));
  if (rc == Errno::OK) {
    cntl->response_attachment().append(buffer);
  }
}

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs
