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

#ifndef DINGOFS_BLOCKCACHE_NET_HANDLER_H_
#define DINGOFS_BLOCKCACHE_NET_HANDLER_H_

#include "blockcache/core/reactor/coroutine.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

class Request;

// The seam between net/ and the application: transports call only this.
class RequestHandler {
 public:
  virtual ~RequestHandler() = default;

  RequestHandler(const RequestHandler&) = delete;
  RequestHandler& operator=(const RequestHandler&) = delete;

  // Runs on the owning shard; owes the caller exactly one answer.
  // `request` outlives every suspension of the returned future.
  virtual Future<Status> Serve(Request& request) = 0;

 protected:
  RequestHandler() = default;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_HANDLER_H_
