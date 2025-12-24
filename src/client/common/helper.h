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

#ifndef DINGOFS_CLIENT_COMMON_HELPER_H_
#define DINGOFS_CLIENT_COMMON_HELPER_H_

#include <sys/mount.h>

#include <cerrno>
#include <climits>
#include <cstdlib>
#include <string>
#include <vector>

#include "common/blockaccess/accesser_common.h"
#include "common/const.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/options/cache.h"
#include "common/options/client.h"
#include "common/options/common.h"
#include "fmt/format.h"
#include "gflags/gflags.h"

namespace dingofs {
namespace client {

static std::vector<std::pair<std::string, std::string>> GenConfigs(
    const std::string& meta,
    const dingofs::blockaccess::BlockAccessOptions& options) {
  using dingofs::blockaccess::AccesserType;

  std::vector<std::pair<std::string, std::string>> configs;

  // config
  configs.emplace_back("config", fmt::format("[{}]", dingofs::FLAGS_conf));
  // log
  configs.emplace_back(
      "log", fmt::format("[{} {} {}(verbose)]",
                         dingofs::Helper::ExpandPath(
                             ::FLAGS_log_dir.empty() ? dingofs::kDefaultLogDir
                                                     : ::FLAGS_log_dir),
                         dingofs::FLAGS_log_level, dingofs::FLAGS_log_v));
  // meta
  configs.emplace_back("meta", fmt::format("[{}]", meta));
  // storage
  if (options.type == AccesserType::kS3) {
    configs.emplace_back("storage",
                         fmt::format("[s3://{}/{}]",
                                     dingofs::Helper::RemoveHttpPrefix(
                                         options.s3_options.s3_info.endpoint),
                                     options.s3_options.s3_info.bucket_name));

  } else if (options.type == AccesserType::kRados) {
    configs.emplace_back(
        "storage",
        fmt::format("[rados://{}/{}]", options.rados_options.mon_host,
                    options.rados_options.pool_name));

  } else if (options.type == AccesserType::kLocalFile) {
    configs.emplace_back(
        "storage", fmt::format("[local://{}]", options.file_options.path));
  }
  // cache
  if (!dingofs::cache::FLAGS_cache_group.empty()) {
    configs.emplace_back("cache",
                         fmt::format("[{} {}]", dingofs::cache::FLAGS_mds_addrs,
                                     dingofs::cache::FLAGS_cache_group));
  } else if (dingofs::cache::FLAGS_cache_store == "disk") {
    configs.emplace_back(
        "cache",
        fmt::format("[{} {}MB {}%(ratio)]", dingofs::cache::FLAGS_cache_dir,
                    dingofs::cache::FLAGS_cache_size_mb,
                    dingofs::cache::FLAGS_free_space_ratio * 100));
  } else {
    configs.emplace_back("cache", "[]");
  }

  // monitor
  auto hostname = dingofs::Helper::GetHostName();
  configs.emplace_back(
      "monitor",
      fmt::format("[{}:{}]", dingofs::Helper::GetIpByHostName(hostname),
                  dingofs::client::FLAGS_vfs_dummy_server_port));

  return configs;
}

static bool Umount(const std::string& mountpoint) {
  if (umount2(mountpoint.c_str(), 0) != 0) {
    if (errno == EPERM) {  // normal user to umount, use fusemount3 to umount
      std::string command = fmt::format("fusermount3 -u {}", mountpoint);
      if (std::system(command.c_str()) == 0) {  // NOLINT
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  return true;
}

static std::string RedString(const std::string& str) {
  return fmt::format("\x1B[31m{}\033[0m", str);
}

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_COMMON_HELPER_H_
