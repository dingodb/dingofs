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
#include <fcntl.h>
#include <netdb.h>
#include <pwd.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/strings/str_split.h"
#include "butil/endpoint.h"
#include "butil/file_util.h"
#include "butil/strings/string_split.h"
#include "butil/strings/string_util.h"
#include "common/options/common.h"
#include "common/types.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "google/protobuf/message.h"
#include "google/protobuf/repeated_field.h"
#include "google/protobuf/util/json_util.h"
#include "options/cache.h"
#include "utils/string.h"

namespace dingofs {

static const uint32_t kMaxHostNameLength = 255;

template <typename T>
using PBvector = google::protobuf::RepeatedField<T>;

template <typename T>
using PBPtrvector = google::protobuf::RepeatedPtrField<T>;

class Helper {
 public:
  static int64_t GetPid();
  static int64_t GetThreadID();

  static bool IsEqualIgnoreCase(const std::string& str1,
                                const std::string& str2);
  static std::string ToUpperCase(const std::string& str);

  // string type cast
  static bool StringToBool(const std::string& str);
  static int32_t StringToInt32(const std::string& str);
  static int64_t StringToInt64(const std::string& str);
  static uint64_t StringToUint64(const std::string& str);
  static float StringToFloat(const std::string& str);
  static double StringToDouble(const std::string& str);

  static std::string StringToHex(const std::string& str);
  static std::string StringToHex(const std::string_view& str);
  static std::string HexToString(const std::string& hex_str);

  static bool ParseAddr(const std::string& addr, std::string& host, int& port);

  // local file system operation
  static std::string ConcatPath(const std::string& path1,
                                const std::string& path2);
  static std::vector<std::string> TraverseDirectory(const std::string& path,
                                                    bool ignore_dir = false,
                                                    bool ignore_file = false);
  static std::vector<std::string> TraverseDirectory(const std::string& path,
                                                    const std::string& prefix,
                                                    bool ignore_dir = false,
                                                    bool ignore_file = false);
  static std::string FindFileInDirectory(const std::string& dirpath,
                                         const std::string& prefix);
  static bool CreateDirectories(const std::string& path);
  static bool RemoveFileOrDirectory(const std::string& path);
  static bool RemoveAllFileOrDirectory(const std::string& path);
  static bool Rename(const std::string& src_path, const std::string& dst_path,
                     bool is_force = true);
  static int64_t GetFileSize(const std::string& path);

  static std::string GenerateRandomString(int length);
  static int64_t GenerateRealRandomInteger(int64_t min_value,
                                           int64_t max_value);
  static int64_t GenerateRandomInteger(int64_t min_value, int64_t max_value);
  static float GenerateRandomFloat(float min_value, float max_value);

  static std::string PrefixNext(const std::string& input);
  static std::string EndPointToString(const butil::EndPoint& endpoint);
  static bool SaveFile(const std::string& filepath, const std::string& data);
  static std::string FsModeToString(mode_t mode);
  static bool ProtoToJson(const google::protobuf::Message& message,
                          std::string& json);

  template <typename T>
  static std::vector<T> PbRepeatedToVector(const PBPtrvector<T>& data) {
    // const source: elements are copied.
    return {data.begin(), data.end()};
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(PBPtrvector<T>* data) {
    // mutable source: elements are moved out, leaving them unspecified.
    std::vector<T> vec;
    vec.reserve(data->size());
    for (auto& item : *data) {
      vec.emplace_back(std::move(item));
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(const PBvector<T>& data) {
    std::vector<T> vec;
    vec.reserve(data.size());
    for (auto& item : data) {
      vec.push_back(item);
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(PBvector<T>* data) {
    std::vector<T> vec;
    vec.reserve(data->size());
    for (auto& item : *data) {
      vec.push_back(item);
    }

    return vec;
  }

  template <typename T>
  static void VectorToPbRepeated(const std::vector<T>& vec,
                                 google::protobuf::RepeatedPtrField<T>* out) {
    for (auto& item : vec) {
      *(out->Add()) = item;
    }
  }

  template <typename T>
  static void VectorToPbRepeated(const std::vector<T>& vec, PBvector<T>* out) {
    for (auto& item : vec) {
      out->Add(item);
    }
  }

  template <typename T>
  static std::string VectorToString(const std::vector<T>& vec) {
    std::string str;
    for (uint32_t i = 0; i < vec.size(); ++i) {
      str += fmt::format("{}", vec[i]);
      if (i + 1 < vec.size()) {
        str += ",";
      }
    }
    return str;
  }

  static void PbMapToMap(
      const google::protobuf::Map<std::string, std::string>& pb_map,
      std::map<std::string, std::string>& out) {
    for (const auto& item : pb_map) {
      out[item.first] = item.second;
    }
  }

  static void MapToPbMap(const std::map<std::string, std::string>& map,
                         google::protobuf::Map<std::string, std::string>* out) {
    for (const auto& item : map) {
      (*out)[item.first] = item.second;
    }
  }

  static std::string GetHostName() {
    char hostname[kMaxHostNameLength + 1];
    int ret = gethostname(hostname, kMaxHostNameLength);
    if (ret < 0) {
      LOG(ERROR) << "[meta.filesystem] get hostname fail, ret=" << ret;
      return "";
    }
    // gethostname does not guarantee null termination on truncation.
    hostname[kMaxHostNameLength] = '\0';

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

  static struct passwd* GetSudoUserInfo() {
    const char* sudo_user = std::getenv("SUDO_USER");
    if (sudo_user) {
      return getpwnam(sudo_user);
    }
    return nullptr;
  }

  static uint32_t GetOriginalGid() {
    struct passwd* pw = GetSudoUserInfo();
    if (pw) {
      return pw->pw_gid;
    }

    return getgid();
  }

  static uint32_t GetOriginalUid() {
    struct passwd* pw = GetSudoUserInfo();
    if (pw) {
      return pw->pw_uid;
    }

    return getuid();
  }

  static std::string GetHomeDir() {
    std::string home_dir = butil::GetHomeDir().value();
    if (home_dir.empty()) {
      LOG(FATAL) << "get home dir fail!";
    }

    return home_dir;
  }

  // parse ~/.dingo/path to /home/user/.dingo/path
  static std::string ExpandPath(const std::string& path) {
    // only expand a leading "~" (i.e. "~" or "~/..."), do not touch '~'
    // appearing elsewhere in the path.
    if (path == "~" || (path.size() >= 2 && path[0] == '~' && path[1] == '/')) {
      return GetHomeDir() + path.substr(1);
    }
    return path;
  }

  static bool IsExistPath(const std::string& path) {
    return butil::PathExists(butil::FilePath(path));
  }

  static bool CreateDirectory(const std::string& path) {
    butil::FilePath dir_path(path);
    if (!butil::DirectoryExists(dir_path)) {
      if (!butil::CreateDirectory(dir_path, true)) {
        return false;
      }
    }

    return true;
  }

  static std::string ToCanonicalPath(const std::string& path) {
    try {
      return std::filesystem::weakly_canonical(std::filesystem::path(path))
          .string();
    } catch (const std::filesystem::filesystem_error& ex) {
      LOG(FATAL) << fmt::format(
          "convert to canonical path failed, path: {} error: {}", path,
          ex.what());
    }
  }

  static std::string ToLowerCase(const std::string& str) {
    std::string result = str;
    for (char& c : result) {
      c = tolower(c);
    }
    return result;
  }

  static std::string RemoveHttpPrefix(const std::string& url) {
    std::string lower = ToLowerCase(url);

    if (lower.find("https://") == 0) {
      return url.substr(8);
    } else if (lower.find("http://") == 0) {
      return url.substr(7);
    }

    return url;
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

    std::cout.flush();
  }

  static void SplitUniteCacheDir(
      const std::string& cache_dir, uint64_t default_cache_size_mb,
      std::vector<std::pair<std::string, uint64_t>>* cache_dirs) {
    std::vector<std::string> dirs = absl::StrSplit(cache_dir, ",");

    for (const auto& dir : dirs) {
      uint64_t cache_size_mb = default_cache_size_mb;
      std::vector<std::string> items = absl::StrSplit(dir, ":");
      if (items.size() > 2 ||
          (items.size() == 2 && !utils::Str2Int(items[1], &cache_size_mb))) {
        CHECK(false) << "Invalid cache dir: " << dir;
      } else if (cache_size_mb == 0) {
        CHECK(false) << "Cache size must greater than 0.";
      }

      cache_dirs->emplace_back(items[0], cache_size_mb);
    }
  }

  static std::string GenCacheConfigInfo() {
    std::vector<std::pair<std::string, uint64_t>> cache_dirs;

    Helper::SplitUniteCacheDir(cache::FLAGS_cache_dir,
                               cache::FLAGS_cache_size_mb, &cache_dirs);

    std::string result;
    for (size_t i = 0; i < cache_dirs.size(); ++i) {
      if (i != 0) {
        result.append(", ");
      }
      fmt::format_to(std::back_inserter(result), "{}({}MB)",
                     cache_dirs[i].first, cache_dirs[i].second);
    }

    return result;
  }

  static void SplitString(const std::string& str, char c,
                          std::vector<std::string>& vec) {
    butil::SplitString(str, c, &vec);
  }

  template <typename T>
  static void SplitString(const std::string& str, char c, std::vector<T>& vec) {
    std::vector<std::string> strs;
    SplitString(str, c, strs);
    for (auto& s : strs) {
      try {
        vec.push_back(std::stoll(s));
      } catch (const std::exception& e) {
        LOG(ERROR) << "stoll exception: " << e.what();
      }
    }
  }

  static const char* DescOpenFlags(int flags) {
    if ((flags & O_ACCMODE) == O_RDONLY) {
      return "RDONLY";

    } else if (flags & O_WRONLY) {
      if (flags & O_TRUNC)
        return "WRONLY|TRUNC";
      else if (flags & O_APPEND)
        return "WRONLY|APPEND";
      return "WRONLY";

    } else if (flags & O_RDWR) {
      if (flags & O_TRUNC)
        return "RDWR|TRUNC";
      else if (flags & O_APPEND)
        return "RDWR|APPEND";
      return "RDWR";

    } else if (flags & O_CREAT) {
      return "CREAT";

    } else if (flags & O_TRUNC) {
      return "TRUNC";

    } else if (flags & O_APPEND) {
      return "APPEND";
    }

    return "UNKNOWN";
  }

  static bool IsSmallFile(uint64_t length) {
    return length <= FLAGS_small_file_max_size;
  }

};  // class Helper

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_HELPER_H_