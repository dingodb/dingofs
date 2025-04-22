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
 * Created Date: 2025-05-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_OPTIONS_CLIENT_RPC_H_
#define DINGOFS_SRC_OPTIONS_CLIENT_RPC_H_

#include "options/options.h"

namespace dingofs {
namespace options {
namespace client {

class MDSRPCOption : public BaseOption {
  BIND_string_array(addrs, std::vector<std::string>{}, "");
  BIND_uint32(max_retry_ms, 16000, "");
  BIND_uint32(max_rpc_timeout_ms, 2000, "");
  BIND_uint32(rpc_timeout_ms, 500, "");
  BIND_uint32(rpc_retry_interval_us, 50000, "");
  BIND_uint32(max_failed_times_before_change_addr, 2, "");
  BIND_uint32(normal_retry_times_before_trigger_wait, 3, "");
  BIND_uint32(wait_sleep_ms, 1000, "");
};

class MetaServerRPCOption : public BaseOption {
  BIND_int32(rpc_timeout_ms, 1000, "");
  BIND_int32(rpc_stream_idle_timeout_ms, 500, "");
  BIND_int32(max_retires, 10, "");
  BIND_int32(max_internal_retries, 3, "");
  BIND_int32(retry_interval_us, 100000, "");
  BIND_int32(list_dentry_limit, 65536, "");
  BIND_int32(max_rpc_timeout_ms, 8000, "");
  BIND_int32(max_retry_sleep_interval_us, 8000000, "");
  BIND_int32(min_retry_times_force_timeout_backoff, 5, "");
  BIND_int32(max_retry_times_before_consider_suspend, 20, "");
  BIND_int32(batch_inode_attr_limit, 10000, "");
};

class RPCOption : public BaseOption {
  BIND_suboption(mds_rpc_option, "mds", MDSRPCOption);
  BIND_suboption(metaserver_rpc_option, "metaserver", MetaServerRPCOption);
};

}  // namespace client
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CLIENT_RPC_H_
