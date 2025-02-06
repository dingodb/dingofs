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

#include "cache/common/errno.h"

#include <ostream>
#include <unordered_map>

#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {
namespace common {

using pb::cache::blockcache::BlockCacheErrFailure;
using pb::cache::blockcache::BlockCacheErrNotFound;
using pb::cache::blockcache::BlockCacheOk;

struct Error {
  Errno code;
  BlockCacheErrCode pb_err;
  std::string description;
};

static const std::vector<Error> kErrors = {
    {Errno::OK, BlockCacheOk, "OK"},

};

// static const std::vector<ErrCode> kErrnos = {
//     {Errno::OK, "OK"},
//     {Errno::INVALID_ARGUMENT, "invalid argument"},
//     {Errno::NOT_FOUND, "not found"},
//     {Errno::EXISTS, "already exists"},
//     {Errno::NOT_DIRECTORY, "not a directory"},
//     {Errno::FILE_TOO_LARGE, "file is too large"},
//     {Errno::END_OF_FILE, "end of file"},
//     {Errno::IO_ERROR, "IO error"},
//     {Errno::ABORT, "abort"},
//     {Errno::CACHE_DOWN, "cache is down"},
//     {Errno::CACHE_UNHEALTHY, "cache is unhealthy"},
//     {Errno::CACHE_FULL, "cache is full"},
//     {Errno::NOT_SUPPORTED, "not supported"},
// };

static const std::unordered_map<Errno, BlockCacheErrCode> kErrnos = {
    {Errno::OK, BlockCacheOk},
};

std::string StrErr(Errno code) {
  // auto it = kErrnos.find(code);
  // if (it != kErrnos.end()) {
  //   return it->second.description;
  // }
  return "unknown";
}

BlockCacheErrCode PBErr(Errno code) { return BlockCacheOk; }

Errno ToErrno(BlockCacheErrCode pb_err) { return Errno::OK; }

std::ostream& operator<<(std::ostream& os, Errno code) {
  if (code == Errno::OK) {
    os << "success";
  } else {
    os << "failed [" << StrErr(code) << "]";
  }
  return os;
}

}  // namespace common
}  // namespace cache
}  // namespace dingofs
