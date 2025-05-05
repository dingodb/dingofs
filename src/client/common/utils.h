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

#include <sstream>

namespace dingofs {
namespace client {

static std::string Char2Addr(const char* p) {
  std::ostringstream oss;
  oss << "0x" << std::hex << std::nouppercase << reinterpret_cast<uintptr_t>(p);
  return oss.str();
}

}  // namespace client
}  // namespace dingofs