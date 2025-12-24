
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
 * Created Date: 2026-01-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REQUEST_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REQUEST_H_

#include <ostream>
#include <string>

#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

template <typename T>
struct Request {
  std::string method;
  T raw;
  const IOBuffer* body;
};

template <typename U>
struct Response {
  Status status;
  U raw;
  IOBuffer body;
};

template <typename T>
inline Request<T> MakeRequest(const std::string& method, const T& raw,
                              const IOBuffer* body = nullptr) {
  return Request<T>{method, raw, body};
}

template <typename T>
inline std::ostream& operator<<(std::ostream& os, const Request<T>& request) {
  os << "Request{method=" << request.method
     << " raw=" << request.raw.ShortDebugString() << "}";
  return os;
}

template <typename U>
inline std::ostream& operator<<(std::ostream& os, const Response<U>& response) {
  os << "Response{raw=" << response.raw.ShortDebugString() << "}";
  return os;
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REQUEST_H_
