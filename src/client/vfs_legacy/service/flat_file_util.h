// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGODB_CLIENT_SERVICE_FLAT_FILE_UTIL_H_
#define DINGODB_CLIENT_SERVICE_FLAT_FILE_UTIL_H_

#include <cstdint>

#include "client/vfs_legacy/inode_wrapper.h"
#include "client/vfs_legacy/service/flat_file.h"

namespace dingofs {
namespace client {

FlatFile InodeWrapperToFlatFile(std::shared_ptr<InodeWrapper> inode_wrapper,
                                uint64_t chunk_size, uint64_t block_size);

}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_SERVICE_FLAT_FILE_UTIL_H_