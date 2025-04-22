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
 * Created Date: 2025-05-09
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_OPTIONS_CACHE_REMOTECACHE_H_
#define DINGOFS_SRC_OPTIONS_CACHE_REMOTECACHE_H_

#include "options/options.h"

namespace dingofs {
namespace options {
namespace cache {

class RemoteNodeOption : public BaseOption {
  BIND_uint32(rpc_timeout_ms, 3000, "");
};

class RemoteBlockCacheOption : public BaseOption {
  BIND_string(group_name, "", "");
  BIND_uint32(load_members_interval_ms, 1000, "");

  BIND_suboption(remote_node_option, "remote_node", RemoteNodeOption);
};

}  // namespace cache
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CACHE_REMOTECACHE_H_
