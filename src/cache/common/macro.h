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
 * Created Date: 2025-06-03
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_MACRO_H_
#define DINGOFS_SRC_CACHE_COMMON_MACRO_H_

#include <absl/strings/str_format.h>
namespace dingofs {
namespace cache {

#define CHECK_RUNNING(service_name)               \
  CHECK(running_.load(std::memory_order_relaxed)) \
      << (service_name) << " is not running"

#define DCHECK_RUNNING(service_name)               \
  DCHECK(running_.load(std::memory_order_relaxed)) \
      << (service_name) << " is not running"

#define DISK_CACHE_BVAR(index, name) \
  absl::StrFormat("dingofs_disk_cache_%d_%s", index, name)

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_MACRO_H_
