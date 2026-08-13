// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#include "mds/common/helper.h"

#include <fstream>
#include <regex>

#include "common/logging.h"
#include "fmt/core.h"

namespace dingofs {
namespace mds {

static bool IsValidFileAddr(const std::string& url) { return url.substr(0, 7) == "file://"; }
static bool IsValidListAddr(const std::string& url) { return url.substr(0, 7) == "list://"; }
static bool IsValidBareAddr(const std::string& url) {
  // check 127.0.0.1:80 or 127.0.0.1:80,127.0.0.1:81 use regex
  std::regex ip_port_list_regex(R"(^((\d{1,3}\.){3}\d{1,3}:\d{1,5})(,(\d{1,3}\.){3}\d{1,3}:\d{1,5})*$)");
  return std::regex_match(url, ip_port_list_regex);
}

static std::string ParseFileUrl(const std::string& coor_url) {
  CHECK(coor_url.substr(0, 7) == "file://") << "Invalid coor_url: " << coor_url;

  std::string file_path = coor_url.substr(7);

  std::ifstream file(file_path);
  if (!file.is_open()) {
    LOG(ERROR) << fmt::format("Open file({}) failed, maybe not exist!", file_path);
    return {};
  }

  std::string addrs;
  std::string line;
  while (std::getline(file, line)) {
    if (line.empty()) {
      continue;
    }
    if (line.find('#') == 0) {
      continue;
    }

    addrs += line + ",";
  }

  return addrs.empty() ? "" : addrs.substr(0, addrs.size() - 1);
}

static std::string ParseListUrl(const std::string& url) {
  CHECK(url.substr(0, 7) == "list://") << "invalid url: " << url;

  return url.substr(7);
}

std::string Helper::ParseStorageAddr(const std::string& url) {
  std::string storage_addrs;
  if (IsValidFileAddr(url)) {
    storage_addrs = ParseFileUrl(url);

  } else if (IsValidListAddr(url)) {
    storage_addrs = ParseListUrl(url);

  } else if (IsValidBareAddr(url)) {
    storage_addrs = url;
  }

  return storage_addrs;
}

std::vector<uint64_t> Helper::GetMdsIds(const pb::mds::HashPartition& partition) {
  std::vector<uint64_t> mds_ids;
  mds_ids.reserve(partition.distributions().size());

  for (const auto& [mds_id, bucket_set] : partition.distributions()) {
    mds_ids.push_back(mds_id);
  }

  return mds_ids;
}

std::vector<uint64_t> Helper::GetMdsIds(const std::map<uint64_t, BucketSetEntry>& distributions) {
  std::vector<uint64_t> mds_ids;
  mds_ids.reserve(distributions.size());

  for (const auto& [mds_id, bucket_set] : distributions) {
    mds_ids.push_back(mds_id);
  }

  return mds_ids;
}

}  // namespace mds
}  // namespace dingofs
