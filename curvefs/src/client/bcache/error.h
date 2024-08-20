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

#ifndef CURVEFS_SRC_CLIENT_BCACHE_ERROR_H_
#define CURVEFS_SRC_CLIENT_BCACHE_ERROR_H_

#include <string>

namespace curvefs {
namespace client {
namespace bcache {

enum class BCACHE_ERROR {
    OK,
    FAIL,
    NOT_FOUND,
    NO_SPACE,
    IO_ERROR,
    ABORT,
    OVERLOAD,

    DISABLE_STAGE,
    CACHE_UNHEALTHY,
    CACHE_FULL,
    SHUTTING_DOWN,
    CACHE_IS_DOWN,
};

std::string StrErr(BCACHE_ERROR code);

std::ostream &operator<<(std::ostream &os, BCACHE_ERROR code);

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_ERROR_H_
