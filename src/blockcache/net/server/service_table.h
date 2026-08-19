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

// The net -> app seam: the only production RequestHandler.

#ifndef DINGOFS_BLOCKCACHE_NET_SERVER_SERVICE_TABLE_H_
#define DINGOFS_BLOCKCACHE_NET_SERVER_SERVICE_TABLE_H_

#include <memory>
#include <vector>

#include "blockcache/net/handler.h"
#include "blockcache/net/server/service.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

// Per-shard dispatch table; guarantees a caller gets exactly one answer.
class ServiceTable final : public RequestHandler {
 public:
  void Add(std::unique_ptr<Service> service);

  // Not a coroutine: a method that answers without suspending pays no frame.
  Future<Status> Serve(Request& request) override;

 private:
  static Future<Status> Answer(Request& request, ReplyCode code);
  static Future<Status> Verdict(Request& request, Status status);
  static Future<Status> Finish(Future<Status> served, Request& request);

  // Owns the objects the bound methods capture.
  std::vector<std::unique_ptr<Service>> services_;
  std::vector<Service::Method> methods_;  // opcode -> method, dense
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_SERVER_SERVICE_TABLE_H_
