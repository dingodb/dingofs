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
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#include <ostream>
#include <unordered_map>

#include "curvefs/src/client/bcache/error.h"

namespace curvefs {
namespace client {
namespace bcache {

static const std::unordered_map<BCACHE_ERROR, std::string> errors = {
    { BCACHE_ERROR::OK, "OK" },
    { BCACHE_ERROR::NOT_FOUND, "not found" },
    { BCACHE_ERROR::OVERLOAD, "overload" },
};

std::string StrErr(BCACHE_ERROR code) {
    auto it = errors.find(code);
    if (it != errors.end()) {
        return it->second;
    }
    return "unknown";
}

std::ostream &operator<<(std::ostream &os, BCACHE_ERROR code) {
    os << static_cast<int>(code) << "[" << [code]() {
        auto it = errors.find(code);
        if (it != errors.end()) {
            return it->second;
        }

        return std::string{"Unknown"};
    }() << "]";

    return os;
}

}  // namespace bcache
}  // namespace client
}  // namespace curvefs
