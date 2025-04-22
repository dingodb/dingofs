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
 * Created Date: 2025-03-19
 * Author: Jingli Chen (Wine93)
 */

#include "cache/utils/sys_conf.h"

#include <unistd.h>

#include <cstdint>

namespace dingofs {
namespace cache {
namespace utils {

#define IO_ALIGNED_BLOCK_SIZE 4096

uint64_t SysConf::GetPageSize() {
  static uint64_t page_size = sysconf(_SC_PAGESIZE);
  return page_size;
}

uint64_t SysConf::GetAlignedBlockSize() { return IO_ALIGNED_BLOCK_SIZE; }

}  // namespace utils
}  // namespace cache
}  // namespace dingofs
