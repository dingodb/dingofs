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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_ERROR_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_ERROR_H_

#include <ostream>
#include <string>

namespace curvefs {
namespace client {
namespace blockcache {

enum class BCACHE_ERROR {
  OK,
  NOT_FOUND,
  EXISTS,
  NOT_DIRECTORY,
  FILE_TOO_LARGE,
  END_OF_FILE,
  IO_ERROR,
  ABORT,
  CACHE_DOWN,
  CACHE_UNHEALTHY,
  CACHE_FULL,
  NOT_SUPPORTED,
};

std::string StrErr(BCACHE_ERROR code);

std::ostream& operator<<(std::ostream& os, BCACHE_ERROR code);

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_ERROR_H_
