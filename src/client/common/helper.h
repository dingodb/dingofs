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
#include "common/directory.h"
#include "common/helper.h"
#include "common/logging.h"
#ifdef WITH_CLIENT_BLOCKCACHE
#include "blockcache/common/flag_decls.h"
#else
#include "common/options/cache.h"
#endif
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
  configs.emplace_back("config", fmt::format("[{}]", FLAGS_conf));
  // log
  configs.emplace_back(
      "log", fmt::format("[{} {} {}(verbose)]",
                         ::FLAGS_log_dir.empty() ? GetDefaultDir(kLogDir)
                                                 : ::FLAGS_log_dir,
                         FLAGS_log_level, FLAGS_log_v));
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
#ifdef WITH_CLIENT_BLOCKCACHE
  if (!blockcache::FLAGS_cache_group.empty()) {
    configs.emplace_back("cache",
                         fmt::format("[{} {}]", blockcache::FLAGS_mds_addrs,
                                     blockcache::FLAGS_cache_group));
  } else if (blockcache::FLAGS_cache_store == "disk") {
    std::vector<std::pair<std::string, uint64_t>> cache_dirs;
    Helper::SplitUniteCacheDir(blockcache::FLAGS_cache_dir,
                               blockcache::FLAGS_cache_size_mb, &cache_dirs);
    std::string cache_dir_info;
    for (size_t i = 0; i < cache_dirs.size(); ++i) {
      if (i != 0) {
        cache_dir_info.append(", ");
      }
      fmt::format_to(std::back_inserter(cache_dir_info), "{}({}MB)",
                     cache_dirs[i].first, cache_dirs[i].second);
    }
    configs.emplace_back(
        "cache",
        fmt::format("[{} {} {}%(ratio)]", blockcache::FLAGS_cache_store,
                    cache_dir_info, blockcache::FLAGS_free_space_ratio * 100));
  } else {
    configs.emplace_back("cache", "[]");
  }
#else
  if (!cache::FLAGS_cache_group.empty()) {
    configs.emplace_back("cache", fmt::format("[{} {}]", cache::FLAGS_mds_addrs,
                                              cache::FLAGS_cache_group));
  } else if (cache::FLAGS_cache_store == "disk") {
    configs.emplace_back(
        "cache", fmt::format("[{} {} {}%(ratio)]", cache::FLAGS_cache_store,
                             Helper::GenCacheConfigInfo(),
                             cache::FLAGS_free_space_ratio * 100));
  } else {
    configs.emplace_back("cache", "[]");
  }
#endif

  // monitor
  auto hostname = Helper::GetHostName();
  configs.emplace_back("monitor",
                       fmt::format("[{}:{}]", Helper::GetIpByHostName(hostname),
                                   client::FLAGS_vfs_dummy_server_port));

  return configs;
}

static int Umount(const std::string& mountpoint) {
  if (umount2(mountpoint.c_str(), 0) == 0) return 0;

  if (errno != EPERM) return errno;

  const std::string command = fmt::format("fusermount3 -u {}", mountpoint);
  return std::system(command.c_str()) == 0 ? 0 : errno;
}

static std::string RedString(const std::string& str) {
  return fmt::format("\x1B[31m{}\033[0m", str);
}

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_COMMON_HELPER_H_
