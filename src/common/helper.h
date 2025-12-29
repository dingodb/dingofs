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

#include <arpa/inet.h>
#include <netdb.h>

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <utility>
#include <vector>

#include "butil/endpoint.h"
#include "butil/file_util.h"
#include "butil/strings/string_util.h"
#include "common/types.h"
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

  static std::string GetIpByHostName(const std::string& hostname) {
    struct hostent* host_entry = gethostbyname(hostname.c_str());
    if (host_entry == nullptr) {
      LOG(ERROR) << "can't parse hostname:" << hostname;
      return {};
    }

    char* ip_ptr = inet_ntoa(*((struct in_addr*)host_entry->h_addr_list[0]));

    return std::string(ip_ptr);
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

  // meta-url: type://address/fs_name
  static bool ParseMetaURL(const std::string& meta_url,
                           MetaSystemType& metasystem_type, std::string& addrs,
                           std::string& fs_name, std::string& storage_info) {
    static const std::string kProtocolSep = "://";

    size_t pos = meta_url.find(kProtocolSep);
    if (pos == std::string::npos) {
      return false;
    }
    auto tmp_type = meta_url.substr(0, pos);
    metasystem_type = ParseMetaSystemType(tmp_type);
    CHECK(metasystem_type != MetaSystemType::UNKNOWN)
        << "invalid metasystem type: " << tmp_type;

    pos += kProtocolSep.length();

    if (metasystem_type == MetaSystemType::MDS) {
      // mds://127.0.0.1:7800/testfs
      size_t slash_pos = meta_url.find('/', pos);
      if (slash_pos == std::string::npos) {
        return false;
      }
      addrs = meta_url.substr(pos, slash_pos - pos);
      fs_name = meta_url.substr(slash_pos + 1);

      return true;
    } else if (metasystem_type == MetaSystemType::LOCAL) {
      // local://dingofs?storage=file&path=/tmp/data
      // local://dingofs?storage=s3&ak=<ak>&sk=<sk>&endpoint=<endpoint>&bucketname=<bucketname>
      size_t question_pos = meta_url.find('?', pos);
      if (question_pos == std::string::npos) {
        fs_name = meta_url.substr(pos);
        storage_info = "";

        return true;
      }
      fs_name = meta_url.substr(pos, question_pos - pos);
      storage_info = meta_url.substr(question_pos + 1);

      return true;
    } else if (metasystem_type == MetaSystemType::MEMORY) {
      // memory://memory_fs
      fs_name = meta_url.substr(pos);

      return true;
    } else {
      return false;
    }

    return true;
  }

  // parse ~/.dingofs/path to /home/user/.dingofs/path
  static std::string ExpandPath(const std::string& path) {
    std::string home_dir = butil::GetHomeDir().value();
    if (home_dir.empty()) {
      LOG(FATAL) << "get home dir fail!";
    }
    std::string expand_dir;
    butil::ReplaceChars(path, "~", home_dir, &expand_dir);
    return expand_dir;
  }

  static bool IsExistPath(const std::string& path) {
    return butil::PathExists(butil::FilePath(path));
  }

  static bool CreateDirectory(const std::string& path) {
    butil::FilePath dir_path(path);
    if (!butil::DirectoryExists(dir_path)) {
      LOG(INFO) << "directory not exists: " << path << ", create it now.";
      if (!butil::CreateDirectory(dir_path, true)) {
        return false;
      }
    }

    return true;
  }

  static std::string ToLowerCase(const std::string& str) {
    std::string result = str;
    for (char& c : result) {
      c = tolower(c);
    }
    return result;
  }

  static std::string RemoveHttpPrefix(const std::string& url) {
    std::string result = ToLowerCase(url);

    if (result.find("https://") == 0) {
      result = result.substr(8);
    } else if (result.find("http://") == 0) {
      result = url.substr(7);
    }

    return result;
  }

  static void PrintConfigInfo(
      const std::vector<std::pair<std::string, std::string>>& configs,
      const uint16_t width = 20) {
    if (configs.empty()) {
      return;
    }

    std::cout << "current configuration:\n";

    for (const auto& [key, value] : configs) {
      std::cout << "  " << std::left << std::setw(width) << key << " " << value
                << "\n";
    }

    std::cout << std::setw(0);
  }

};  // class Helper

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_HELPER_H_