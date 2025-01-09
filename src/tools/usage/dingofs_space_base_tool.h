/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: 2021-10-22
 * Author: chengyi01
 */

#ifndef DINGOFS_SRC_TOOLS_USAGE_DINGOFS_SPACE_BASE_TOOL_H_
#define DINGOFS_SRC_TOOLS_USAGE_DINGOFS_SPACE_BASE_TOOL_H_

#include <cstdint>
#include <sstream>
#include <string>

namespace dingofs {
namespace tools {
namespace usage {

std::string ToReadableByte(uint64_t byte);

}  // namespace usage
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_USAGE_DINGOFS_SPACE_BASE_TOOL_H_
