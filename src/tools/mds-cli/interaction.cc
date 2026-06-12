// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mds/client/interaction.h"

namespace dingofs {
namespace mds {
namespace client {

DEFINE_int32(timeout_ms, 8000, "Timeout for each request");
DEFINE_bool(log_each_request, false, "print log each request");

bool Interaction::Init(const std::string& addr) {
  if (channel_.Init(addr.c_str(), nullptr) != 0) {
    LOG(ERROR) << fmt::format("Init channel fail, addr({})", addr);
    return false;
  }
  return true;
}

}  // namespace client
}  // namespace mds
}  // namespace dingofs