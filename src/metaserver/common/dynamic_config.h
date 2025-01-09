/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-10-31
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_METASERVER_COMMON_DYNAMIC_CONFIG_H_
#define DINGOFS_SRC_METASERVER_COMMON_DYNAMIC_CONFIG_H_

#include <gflags/gflags.h>

namespace dingofs {
namespace metaserver {
namespace common {

#define USING_FLAG(name) using ::dingofs::metaserver::common::FLAGS_##name;

// You can modify the config on the fly, e.g.
//
// curl -s
// http://127.0.0.1:6701/flags/superpartition_access_logging?setvalue=true

DECLARE_bool(superpartition_access_logging);

}  // namespace common
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_COMMON_DYNAMIC_CONFIG_H_
