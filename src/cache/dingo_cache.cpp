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
 * Created Date: 2025-02-26
 * Author: Jingli Chen (Wine93)
 */

#include "cache/dingo_cache.h"

#include <glog/logging.h>

#include <memory>

#include "cache/cachegroup/cache_group_node.h"
#include "cache/cachegroup/cache_group_node_server.h"
#include "cache/common/config.h"

using ::dingofs::cache::cachegroup::CacheGroupNodeImpl;
using ::dingofs::cache::cachegroup::CacheGroupNodeServerImpl;
using ::dingofs::cache::common::CacheGroupNodeOptions;

int main(int /*argc*/, char** /*argv*/) {
  CacheGroupNodeOptions options;
  auto node = std::make_shared<CacheGroupNodeImpl>(options);

  CacheGroupNodeServerImpl server(node);
  server.Serve();  // Start server and wait CTRL+C to quit
  server.Shutdown();
  return 0;
}