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
 * Created Date: 2025-08-20
 * Author: Jingli Chen (Wine93)
 */

#include "cache/benchmark/option.h"

#include <brpc/reloadable_flags.h>

namespace dingofs {
namespace cache {

DEFINE_uint32(threads, 1, "");
DEFINE_string(op, "put", "");
DEFINE_uint64(fsid, 1, "");
DEFINE_uint64(ino, 0, "");
DEFINE_uint64(blksize, 4194304, "");
DEFINE_uint64(blocks, 1, "");
DEFINE_uint64(offset, 0, "");
DEFINE_uint64(length, 4194304, "");
DEFINE_bool(writeback, false, "");
DEFINE_bool(retrive, true, "");
DEFINE_uint32(async_max_inflight, 128, "");
DEFINE_uint32(runtime, 300, "");
DEFINE_bool(time_based, false, "");

}  // namespace cache
}  // namespace dingofs
