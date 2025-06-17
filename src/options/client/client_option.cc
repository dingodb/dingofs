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

#include "options/client/client_option.h"

#include "utils/configuration.h"
#include "utils/gflags_helper.h"

namespace brpc {
DECLARE_int32(defer_close_second);
DECLARE_int32(health_check_interval);
}  // namespace brpc

namespace dingofs {
namespace client {

static void SetBrpcOpt(utils::Configuration* conf) {
  dingofs::utils::GflagsLoadValueFromConfIfCmdNotSet dummy;
  dummy.Load(conf, "defer_close_second", "rpc.defer.close.second",
             &brpc::FLAGS_defer_close_second);
  dummy.Load(conf, "health_check_interval", "rpc.healthCheckIntervalSec",
             &brpc::FLAGS_health_check_interval);
}

void InitClientOption(utils::Configuration* conf, ClientOption* client_option) {
  InitVFSLegacyOption(conf, &client_option->vfs_legacy_option);
  vfs::InitVFSOption(conf, &client_option->vfs_option);

  SetBrpcOpt(conf);
}

}  // namespace client

}  // namespace dingofs
