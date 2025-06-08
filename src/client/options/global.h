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
 * Created Date: 2025-05-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_OPTIONS_CLIENT_GLOBAL_H_
#define DINGOFS_SRC_OPTIONS_CLIENT_GLOBAL_H_

#include "options/options.h"

namespace dingofs {
namespace options {
namespace client {

class GlobalOption : public BaseOption {
  BIND_string(log_dir, "/tmp", "");
  BIND_int32(vlog_level, 0, "");
  BIND_bool(access_logging, true, "");
  BIND_uint32(dump_server_start_port, 9000, "");
};

}  // namespace client
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CLIENT_GLOBAL_H_
