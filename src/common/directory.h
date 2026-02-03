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

#ifndef DINGOFS_SRC_COMMON_DIRECTORY_H_
#define DINGOFS_SRC_COMMON_DIRECTORY_H_

#include <butil/file_util.h>
#include <fmt/format.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "common/helper.h"

namespace dingofs {

// default dingofs runtime data dir, including cache data, log, meta, etc.
static const std::string kBaseDir =
    (getuid() == 0) ? "/var/dingofs"
                    : fmt::format("{}/.dingofs", Helper::GetHomeDir());

static const std::string kCacheDir = "cache";
static const std::string kLogDir = "log";
static const std::string kDataDir = "data";
static const std::string kMetaDir = "meta";
static const std::string kDbDir = "store";
static const std::string kSocketDir = "run";

inline std::string GetDefaultDir(const std::string& sub_dir) {
  std::filesystem::path base_dir(kBaseDir);
  return (base_dir / sub_dir).string();
}

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_DIRECTORY_H_
