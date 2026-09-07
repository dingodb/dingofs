/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CLIENT_BLOCK_STORE_IMPL_H_
#define DINGOFS_CLIENT_BLOCK_STORE_IMPL_H_

#include <cstdint>
#include <memory>
#include <string>

#include "client/vfs/blockstore/block_store.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

// fs_id names the mounted filesystem the hub's block accesser serves.
std::unique_ptr<BlockStore> NewBlockStore(VFSHub* hub, std::string uuid,
                                          uint32_t fs_id, uint64_t block_size);

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
#endif  // DINGOFS_CLIENT_BLOCK_STORE_IMPL_H_
