// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_SRC_COMMON_HELPER_H_
#define DINGOFS_SRC_COMMON_HELPER_H_

#include <cstdint>

#include "butil/endpoint.h"
#include "glog/logging.h"

namespace dingofs {

static const uint32_t kMaxHostNameLength = 255;

class Helper {
 public:
  static std::string GetHostName() {
    char hostname[kMaxHostNameLength];
    int ret = gethostname(hostname, kMaxHostNameLength);
    if (ret < 0) {
      LOG(ERROR) << "[meta.filesystem] get hostname fail, ret=" << ret;
      return "";
    }

    return std::string(hostname);
  }

  static std::string HostName2IP(std::string host_name) {
    butil::ip_t ip;
    auto ret = butil::hostname2ip(host_name.c_str(), &ip);
    if (ret != 0) {
      LOG(ERROR) << "[meta.filesystem] get ip fail, ret=" << ret;
      return "";
    }

    std::string ip_str = butil::ip2str(ip).c_str();
    return ip_str;
  }

  static std::string Char2Addr(const char* p) {
    std::ostringstream oss;
    oss << "0x" << std::hex << std::nouppercase
        << reinterpret_cast<uintptr_t>(p);
    return oss.str();
  }

  static bool ParseMetaURL(const std::string& meta_url, std::string& addrs,
                           std::string& fs_name) {
    size_t pos = meta_url.find('/');
    if (pos == std::string::npos) {
      return false;
    }
    addrs = meta_url.substr(0, pos);
    fs_name = meta_url.substr(pos + 1);

    return true;
  }
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_HELPER_H_