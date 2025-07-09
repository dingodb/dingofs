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
 * Created Date: 2025-06-15
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_RPC_CLIENT_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_RPC_CLIENT_H_

#include <brpc/channel.h>
#include <butil/iobuf.h>
#include <fmt/format.h>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/type.h"
#include "cache/utils/context.h"

namespace dingofs {
namespace cache {

class RPCClient {
 public:
  RPCClient(const std::string& server_ip, uint32_t server_port);

  Status Init();

  Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block);
  Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
               size_t length, IOBuffer* buffer, RangeOption option);
  Status Cache(ContextSPtr ctx, const BlockKey& key, const Block& block);
  Status Prefetch(ContextSPtr ctx, const BlockKey& key, size_t length);

 private:
  Status InitChannel(const std::string& server_ip, uint32_t server_port);
  brpc::Channel* GetChannel();
  Status ResetChannel();

  bool ShouldRetry(const std::string& api_name, int retcode);
  bool ShouldReset(int retcode);
  uint32_t NextTimeoutMs(const std::string& api_name, int retry_count) const;

  template <typename Request, typename Response>
  Status SendRequest(ContextSPtr ctx, const std::string& api_name,
                     const Request& request, Response& response);

  template <typename Request, typename Response>
  Status SendRequest(ContextSPtr ctx, const std::string& api_name,
                     const Request& request,
                     const butil::IOBuf& request_attachment,
                     Response& response);

  template <typename Request, typename Response>
  Status SendRequest(ContextSPtr ctx, const std::string& api_name,
                     const Request& request, Response& response,
                     butil::IOBuf& response_attachment);

  template <typename Request, typename Response>
  Status SendRequest(ContextSPtr ctx, const std::string& api_name,
                     const Request& request,
                     const butil::IOBuf& request_attachment, Response& response,
                     butil::IOBuf& response_attachment);

  bool inited_;
  BthreadRWLock rwlock_;  // protect channel_
  const std::string server_ip_;
  const uint32_t server_port_;
  std::unique_ptr<brpc::Channel> channel_;
};

using RPCClientUPtr = std::unique_ptr<RPCClient>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_RPC_CLIENT_H_
