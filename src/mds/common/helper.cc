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

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <random>
#include <regex>
#include <string>
#include <thread>

#include "butil/strings/string_split.h"
#include "common/logging.h"
#include "fmt/core.h"

namespace dingofs {
namespace mds {

int64_t Helper::GetPid() {
  pid_t pid = getpid();

  return static_cast<int64_t>(pid);
}

int64_t Helper::GetThreadID() {
  auto thread_id = std::this_thread::get_id();

  return *(std::thread::native_handle_type*)(&thread_id);
}

bool Helper::IsEqualIgnoreCase(const std::string& str1, const std::string& str2) {
  if (str1.size() != str2.size()) {
    return false;
  }
  return std::equal(str1.begin(), str1.end(), str2.begin(),
                    [](const char c1, const char c2) { return std::tolower(c1) == std::tolower(c2); });
}

std::string Helper::ToUpperCase(const std::string& str) {
  std::string result = str;
  for (char& c : result) {
    c = toupper(c);
  }
  return result;
}

std::string Helper::ToLowerCase(const std::string& str) {
  std::string result = str;
  for (char& c : result) {
    c = tolower(c);
  }
  return result;
}

bool Helper::StringToBool(const std::string& str) { return !(str == "0" || str == "false"); }
int32_t Helper::StringToInt32(const std::string& str) { return std::strtol(str.c_str(), nullptr, 10); }
int64_t Helper::StringToInt64(const std::string& str) { return std::strtoll(str.c_str(), nullptr, 10); }
float Helper::StringToFloat(const std::string& str) { return std::strtof(str.c_str(), nullptr); }
double Helper::StringToDouble(const std::string& str) { return std::strtod(str.c_str(), nullptr); }

void Helper::SplitString(const std::string& str, char c, std::vector<std::string>& vec) {
  butil::SplitString(str, c, &vec);
}

void Helper::SplitString(const std::string& str, char c, std::vector<int64_t>& vec) {
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

std::string Helper::StringToHex(const std::string& str) {
  std::stringstream ss;
  for (const auto& ch : str) {
    ss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(static_cast<unsigned char>(ch));
  }
  return ss.str();
}

std::string Helper::StringToHex(const std::string_view& str) {
  std::stringstream ss;
  for (const auto& ch : str) {
    ss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(static_cast<unsigned char>(ch));
  }
  return ss.str();
}

std::string Helper::HexToString(const std::string& hex_str) {
  std::string result;

  try {
    // The hex_string must be of even length
    for (size_t i = 0; i < hex_str.length(); i += 2) {
      std::string hex_byte = hex_str.substr(i, 2);
      // Convert the hex byte to an integer
      int byte_value = std::stoi(hex_byte, nullptr, 16);
      // Cast the integer to a char and append it to the result string
      result += static_cast<unsigned char>(byte_value);
    }
  } catch (const std::invalid_argument& ia) {
    LOG(ERROR) << "HexToString error Irnvalid argument: " << ia.what() << '\n';
    return "";
  } catch (const std::out_of_range& oor) {
    LOG(ERROR) << "HexToString error Out of Range error: " << oor.what() << '\n';
    return "";
  }

  return result;
}

bool Helper::ParseAddr(const std::string& addr, std::string& host, int& port) {
  std::vector<std::string> vec;
  SplitString(addr, ':', vec);
  if (vec.size() != 2) {
    LOG(ERROR) << "parse addr error, addr: " << addr;
    return false;
  }

  try {
    host = vec[0];
    port = std::stoi(vec[1]);

  } catch (const std::exception& e) {
    LOG(ERROR) << "stoi exception: " << e.what();
    return false;
  }

  return true;
}

std::string Helper::ConcatPath(const std::string& path1, const std::string& path2) {
  std::filesystem::path path_a(path1);
  std::filesystem::path path_b(path2);
  return (path_a / path_b).string();
}

std::vector<std::string> Helper::TraverseDirectory(const std::string& path, bool ignore_dir, bool ignore_file) {
  return TraverseDirectory(path, "", ignore_dir, ignore_file);
}

std::vector<std::string> Helper::TraverseDirectory(const std::string& path, const std::string& prefix, bool ignore_dir,
                                                   bool ignore_file) {
  std::vector<std::string> filenames;
  try {
    if (std::filesystem::exists(path)) {
      for (const auto& fe : std::filesystem::directory_iterator(path)) {
        if (ignore_dir && fe.is_directory()) {
          continue;
        }

        if (ignore_file && fe.is_regular_file()) {
          continue;
        }

        if (prefix.empty()) {
          filenames.push_back(fe.path().filename().string());
        } else {
          // check if the filename start with prefix
          auto filename = fe.path().filename().string();
          if (filename.find(prefix) == 0L) {
            filenames.push_back(filename);
          }
        }
      }
    }
  } catch (std::filesystem::filesystem_error const& ex) {
    LOG(ERROR) << fmt::format("directory_iterator failed, path: {} error: {}", path, ex.what());
  }

  return filenames;
}

std::string Helper::FindFileInDirectory(const std::string& dirpath, const std::string& prefix) {
  try {
    if (std::filesystem::exists(dirpath)) {
      for (const auto& fe : std::filesystem::directory_iterator(dirpath)) {
        auto filename = fe.path().filename().string();
        if (filename.find(prefix) != std::string::npos) {
          return filename;
        }
      }
    }
  } catch (std::filesystem::filesystem_error const& ex) {
    LOG(ERROR) << fmt::format("directory_iterator failed, path: {} prefix: {} error: {}", dirpath, prefix, ex.what());
  }

  return "";
}

bool Helper::CreateDirectory(const std::string& path) {
  std::error_code ec;
  if (!std::filesystem::create_directories(path, ec)) {
    LOG(ERROR) << fmt::format("Create directory failed, error: {} {}", ec.value(), ec.message());
    return false;
  }

  return true;
}

bool Helper::CreateDirectories(const std::string& path) {
  std::error_code ec;
  if (std::filesystem::exists(path)) {
    LOG(INFO) << fmt::format("Directory already exists, path: {}", path);
    return true;
  }

  if (!std::filesystem::create_directories(path, ec)) {
    LOG(ERROR) << fmt::format("Create directory {} failed, error: {} {}", path, ec.value(), ec.message());
    return false;
  }

  return true;
}

bool Helper::RemoveFileOrDirectory(const std::string& path) {
  std::error_code ec;
  if (!std::filesystem::remove(path, ec)) {
    LOG(ERROR) << fmt::format("Remove directory failed, path: {} error: {} {}", path, ec.value(), ec.message());
    return false;
  }

  return true;
}

bool Helper::RemoveAllFileOrDirectory(const std::string& path) {
  std::error_code ec;
  LOG(INFO) << fmt::format("Remove all file or directory, path: {}", path);
  auto num = std::filesystem::remove_all(path, ec);
  if (num == static_cast<std::uintmax_t>(-1)) {
    LOG(ERROR) << fmt::format("Remove all directory failed, path: {} error: {} {}", path, ec.value(), ec.message());
    return false;
  }

  return true;
}

bool Helper::Rename(const std::string& src_path, const std::string& dst_path, bool is_force) {
  std::filesystem::path source_path = src_path;
  std::filesystem::path destination_path = dst_path;

  // Check if the destination path already exists
  if (std::filesystem::exists(destination_path)) {
    if (!is_force) {
      // If is_force is false, return error
      LOG(ERROR) << fmt::format("Destination {} already exists, is_force = false, so cannot rename from {}", dst_path,
                                src_path);
      return false;
    }

    // Remove the existing destination
    RemoveAllFileOrDirectory(dst_path);

    // Check if the removal was successful
    if (std::filesystem::exists(destination_path)) {
      LOG(ERROR) << fmt::format("Failed to remove the existing destination {} ", dst_path);
      return false;
    }
  }

  // Perform the rename operation
  try {
    std::filesystem::rename(source_path, destination_path);
  } catch (const std::exception& ex) {
    LOG(ERROR) << fmt::format("Rename operation failed, src_path: {}, dst_path: {}, error: {}", src_path, dst_path,
                              ex.what());
    return false;
  }

  return true;
}

bool Helper::IsExistPath(const std::string& path) { return std::filesystem::exists(path); }

int64_t Helper::GetFileSize(const std::string& path) {
  try {
    std::uintmax_t size = std::filesystem::file_size(path);
    LOG(INFO) << fmt::format("File size: {} bytes", size);
    return size;
  } catch (const std::filesystem::filesystem_error& ex) {
    LOG(ERROR) << fmt::format("Get file size failed, path: {}, error: {}", path, ex.what());
    return -1;
  }
}

std::string Helper::GenerateRandomString(int length) {
  std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  std::string rand_string;

  uint32_t seed = GenerateRealRandomInteger(0, UINT32_MAX);

  for (int i = 0; i < length; i++) {
    int rand_index = rand_r(&seed) % chars.size();
    rand_string += chars[rand_index];
  }

  return rand_string;
}

int64_t Helper::GenerateRealRandomInteger(int64_t min_value, int64_t max_value) {
  // Create a random number generator engine
  std::random_device rd;      // Obtain a random seed from the hardware
  std::mt19937_64 gen(rd());  // Standard 64-bit mersenne_twister_engine seeded with rd()

  // Create a distribution for the desired range
  std::uniform_int_distribution<int64_t> dis(min_value, max_value);

  // Generate and print a random int64 number
  int64_t random_number = dis(gen);

  return random_number;
}

int64_t Helper::GenerateRandomInteger(int64_t min_value, int64_t max_value) {
  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(min_value, max_value);

  return distrib(rng);
}

float Helper::GenerateRandomFloat(float min_value, float max_value) {
  std::random_device rd;  // Obtain a random seed from the hardware
  std::mt19937 rng(rd());
  std::uniform_real_distribution<> distrib(min_value, max_value);

  return distrib(rng);
}

std::string Helper::PrefixNext(const std::string& input) {
  std::string ret(input.size(), 0);
  int carry = 1;
  for (int i = input.size() - 1; i >= 0; --i) {
    if (static_cast<uint8_t>(input[i]) == (uint8_t)0xFF && carry == 1) {
      ret[i] = 0;
    } else {
      ret[i] = (input[i] + carry);
      carry = 0;
    }
  }

  return (carry == 0) ? ret : input;
}

std::string Helper::EndPointToString(const butil::EndPoint& endpoint) {
  return std::string(butil::endpoint2str(endpoint).c_str());
}

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

bool Helper::SaveFile(const std::string& filepath, const std::string& data) {
  std::ofstream file(filepath);
  if (!file.is_open()) {
    return false;
  }

  file << data;
  file.close();

  return true;
}

std::string Helper::FsModeToString(mode_t mode) {
  std::string result(10, '-');  // 默认初始化为10个'-'字符

  // 检查文件类型
  if (S_ISREG(mode))
    result[0] = '-';  // 普通文件
  else if (S_ISDIR(mode))
    result[0] = 'd';  // 目录
  else if (S_ISLNK(mode))
    result[0] = 'l';  // 符号链接
  else if (S_ISFIFO(mode))
    result[0] = 'p';  // 命名管道
  else if (S_ISSOCK(mode))
    result[0] = 's';  // 套接字
  else if (S_ISCHR(mode))
    result[0] = 'c';  // 字符设备
  else if (S_ISBLK(mode))
    result[0] = 'b';  // 块设备

  // 设置用户权限
  if (mode & S_IRUSR) result[1] = 'r';
  if (mode & S_IWUSR) result[2] = 'w';
  if (mode & S_IXUSR) {
    if (mode & S_ISUID)
      result[3] = 's';  // 设置用户ID
    else
      result[3] = 'x';
  } else if (mode & S_ISUID) {
    result[3] = 'S';  // 大写S表示设置了SUID位但没有执行权限
  }

  // 设置组权限
  if (mode & S_IRGRP) result[4] = 'r';
  if (mode & S_IWGRP) result[5] = 'w';
  if (mode & S_IXGRP) {
    if (mode & S_ISGID)
      result[6] = 's';  // 设置组ID
    else
      result[6] = 'x';
  } else if (mode & S_ISGID) {
    result[6] = 'S';  // 大写S表示设置了SGID位但没有执行权限
  }

  // 设置其他人权限
  if (mode & S_IROTH) result[7] = 'r';
  if (mode & S_IWOTH) result[8] = 'w';
  if (mode & S_IXOTH) {
    if (mode & S_ISVTX)
      result[9] = 't';  // sticky位
    else
      result[9] = 'x';
  } else if (mode & S_ISVTX) {
    result[9] = 'T';  // 大写T表示设置了sticky位但没有执行权限
  }

  return result;
}

bool Helper::ProtoToJson(const google::protobuf::Message& message, std::string& json) {
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  options.preserve_proto_field_names = true;
  return MessageToJsonString(message, &json, options).ok();
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

const char* Helper::DescOpenFlags(int flags) {
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

}  // namespace mds
}  // namespace dingofs