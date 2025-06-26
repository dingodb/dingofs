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
 * Created Date: 2025-06-02
 * Author: Jingli Chen (Wine93)
 */

#include <absl/strings/str_split.h>
#include <glog/logging.h>

#include "cache/config/cachegroup.h"
#include "cache/config/stub.h"

namespace dingofs {
namespace cache {

CacheGroupNodeOption::CacheGroupNodeOption()
    : group_name(FLAGS_group_name),
      listen_ip(FLAGS_listen_ip),
      listen_port(FLAGS_listen_port),
      group_weight(FLAGS_group_weight),
      metadata_filepath(FLAGS_metadata_filepath),
      mds_option(NewMdsOption()) {}

}  // namespace cache
}  // namespace dingofs
