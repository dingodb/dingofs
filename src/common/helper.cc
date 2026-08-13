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

#include "common/helper.h"

#include <sys/types.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <stdexcept>
#include <thread>

#include "common/logging.h"
#include "fmt/core.h"

namespace dingofs {

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

bool Helper::StringToBool(const std::string& str) { return !(str == "0" || str == "false"); }
int32_t Helper::StringToInt32(const std::string& str) { return std::strtol(str.c_str(), nullptr, 10); }
int64_t Helper::StringToInt64(const std::string& str) { return std::strtoll(str.c_str(), nullptr, 10); }
uint64_t Helper::StringToUint64(const std::string& str) { return std::strtoull(str.c_str(), nullptr, 10); }
float Helper::StringToFloat(const std::string& str) { return std::strtof(str.c_str(), nullptr); }
double Helper::StringToDouble(const std::string& str) { return std::strtod(str.c_str(), nullptr); }

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
      int byte_value = std::stoi(hex_byte, nullptr, 16);
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
  Helper::SplitString(addr, ':', vec);
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

  if (std::filesystem::exists(destination_path)) {
    if (!is_force) {
      LOG(ERROR) << fmt::format("Destination {} already exists, is_force = false, so cannot rename from {}", dst_path,
                                src_path);
      return false;
    }

    RemoveAllFileOrDirectory(dst_path);

    if (std::filesystem::exists(destination_path)) {
      LOG(ERROR) << fmt::format("Failed to remove the existing destination {} ", dst_path);
      return false;
    }
  }

  try {
    std::filesystem::rename(source_path, destination_path);
  } catch (const std::exception& ex) {
    LOG(ERROR) << fmt::format("Rename operation failed, src_path: {}, dst_path: {}, error: {}", src_path, dst_path,
                              ex.what());
    return false;
  }

  return true;
}

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
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<int64_t> dis(min_value, max_value);

  return dis(gen);
}

int64_t Helper::GenerateRandomInteger(int64_t min_value, int64_t max_value) {
  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(min_value, max_value);

  return distrib(rng);
}

float Helper::GenerateRandomFloat(float min_value, float max_value) {
  std::random_device rd;
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
  std::string result(10, '-');

  if (S_ISREG(mode))
    result[0] = '-';
  else if (S_ISDIR(mode))
    result[0] = 'd';
  else if (S_ISLNK(mode))
    result[0] = 'l';
  else if (S_ISFIFO(mode))
    result[0] = 'p';
  else if (S_ISSOCK(mode))
    result[0] = 's';
  else if (S_ISCHR(mode))
    result[0] = 'c';
  else if (S_ISBLK(mode))
    result[0] = 'b';

  if (mode & S_IRUSR) result[1] = 'r';
  if (mode & S_IWUSR) result[2] = 'w';
  if (mode & S_IXUSR) {
    if (mode & S_ISUID)
      result[3] = 's';
    else
      result[3] = 'x';
  } else if (mode & S_ISUID) {
    result[3] = 'S';
  }

  if (mode & S_IRGRP) result[4] = 'r';
  if (mode & S_IWGRP) result[5] = 'w';
  if (mode & S_IXGRP) {
    if (mode & S_ISGID)
      result[6] = 's';
    else
      result[6] = 'x';
  } else if (mode & S_ISGID) {
    result[6] = 'S';
  }

  if (mode & S_IROTH) result[7] = 'r';
  if (mode & S_IWOTH) result[8] = 'w';
  if (mode & S_IXOTH) {
    if (mode & S_ISVTX)
      result[9] = 't';
    else
      result[9] = 'x';
  } else if (mode & S_ISVTX) {
    result[9] = 'T';
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

}  // namespace dingofs
