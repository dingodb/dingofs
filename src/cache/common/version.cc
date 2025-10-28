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
 * Created Date: 2025-08-02
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/version.h"

#include <glog/logging.h>

#include <sstream>

#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {

std::string Version() {
  std::ostringstream os;
  os << "dingo-cache " << GIT_TAG_NAME << ", build " << GIT_LAST_COMMIT_ID
     << " +" << Helper::ToLowerCase(DINGOFS_BUILD_TYPE);
  return os.str();
}

}  // namespace cache
}  // namespace dingofs
