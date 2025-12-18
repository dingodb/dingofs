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

#ifndef DINGOFS_SRC_COMMON_CONST_H_
#define DINGOFS_SRC_COMMON_CONST_H_

#include <cstdint>
#include <string>

namespace dingofs {

static constexpr uint64_t kKiB = 1024ULL;
static constexpr uint64_t kMiB = 1024ULL * kKiB;
static constexpr uint64_t kGiB = 1024ULL * kMiB;
static constexpr uint64_t kTiB = 1024ULL * kGiB;

// default dingofs runtime data dir, including cache data, log, meta, etc.
static const std::string kDefaultRuntimeBaseDir = "~/.dingofs/";
static const std::string kDefaultCacheDir = kDefaultRuntimeBaseDir + "cache";
static const std::string kDefaultCacheLogDir =
    kDefaultRuntimeBaseDir + "log/cache";
static const std::string kDefaultClientLogDir =
    kDefaultRuntimeBaseDir + "log/client";

static const std::string kDefaultDataDir = kDefaultRuntimeBaseDir + "data";
static const std::string kDefaultMetaDBDir = kDefaultRuntimeBaseDir + "meta";

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_CONST_H_
