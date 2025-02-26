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

#include "mdsv2/common/helper.h"

#include <filesystem>
#include <fstream>
#include <random>

#include "butil/strings/string_split.h"
#include "fmt/core.h"
#include "mdsv2/common/logging.h"

namespace dingofs {

namespace mdsv2 {

int64_t Helper::TimestampNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::TimestampUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::Timestamp() {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string Helper::NowTime() { return FormatMsTime(TimestampMs(), "%Y-%m-%d %H:%M:%S"); }

std::string Helper::FormatMsTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp(
      (std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str()) << "." << timestamp % 1000;
  return ss.str();
}

std::string Helper::FormatMsTime(int64_t timestamp) { return FormatMsTime(timestamp, "%Y-%m-%d %H:%M:%S"); }

std::string Helper::FormatTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> tp((std::chrono::seconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str());
  return ss.str();
}

std::string Helper::GetNowFormatMsTime() {
  int64_t timestamp = TimestampMs();
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp(
      (std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%dT%H:%M:%S.000Z");
  return ss.str();
}

bool Helper::IsEqualIgnoreCase(const std::string& str1, const std::string& str2) {
  if (str1.size() != str2.size()) {
    return false;
  }
  return std::equal(str1.begin(), str1.end(), str2.begin(),
                    [](const char c1, const char c2) { return std::tolower(c1) == std::tolower(c2); });
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
      DINGO_LOG(ERROR) << "stoll exception: " << e.what();
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
    DINGO_LOG(ERROR) << "HexToString error Irnvalid argument: " << ia.what() << '\n';
    return "";
  } catch (const std::out_of_range& oor) {
    DINGO_LOG(ERROR) << "HexToString error Out of Range error: " << oor.what() << '\n';
    return "";
  }

  return result;
}

bool Helper::ParseAddr(const std::string& addr, std::string& host, int& port) {
  std::vector<std::string> vec;
  SplitString(addr, ':', vec);
  if (vec.size() != 2) {
    DINGO_LOG(ERROR) << "parse addr error, addr: " << addr;
    return false;
  }

  try {
    host = vec[0];
    port = std::stoi(vec[1]);

  } catch (const std::exception& e) {
    DINGO_LOG(ERROR) << "stoi exception: " << e.what();
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
    DINGO_LOG(ERROR) << fmt::format("directory_iterator failed, path: {} error: {}", path, ex.what());
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
    DINGO_LOG(ERROR) << fmt::format("directory_iterator failed, path: {} prefix: {} error: {}", dirpath, prefix,
                                    ex.what());
  }

  return "";
}

bool Helper::CreateDirectory(const std::string& path) {
  std::error_code ec;
  if (!std::filesystem::create_directories(path, ec)) {
    DINGO_LOG(ERROR) << fmt::format("Create directory failed, error: {} {}", ec.value(), ec.message());
    return false;
  }

  return true;
}

bool Helper::CreateDirectories(const std::string& path) {
  std::error_code ec;
  if (std::filesystem::exists(path)) {
    DINGO_LOG(INFO) << fmt::format("Directory already exists, path: {}", path);
    return true;
  }

  if (!std::filesystem::create_directories(path, ec)) {
    DINGO_LOG(ERROR) << fmt::format("Create directory {} failed, error: {} {}", path, ec.value(), ec.message());
    return false;
  }

  return true;
}

bool Helper::RemoveFileOrDirectory(const std::string& path) {
  std::error_code ec;
  if (!std::filesystem::remove(path, ec)) {
    DINGO_LOG(ERROR) << fmt::format("Remove directory failed, path: {} error: {} {}", path, ec.value(), ec.message());
    return false;
  }

  return true;
}

bool Helper::RemoveAllFileOrDirectory(const std::string& path) {
  std::error_code ec;
  DINGO_LOG(INFO) << fmt::format("Remove all file or directory, path: {}", path);
  auto num = std::filesystem::remove_all(path, ec);
  if (num == static_cast<std::uintmax_t>(-1)) {
    DINGO_LOG(ERROR) << fmt::format("Remove all directory failed, path: {} error: {} {}", path, ec.value(),
                                    ec.message());
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
      DINGO_LOG(ERROR) << fmt::format("Destination {} already exists, is_force = false, so cannot rename from {}",
                                      dst_path, src_path);
      return false;
    }

    // Remove the existing destination
    RemoveAllFileOrDirectory(dst_path);

    // Check if the removal was successful
    if (std::filesystem::exists(destination_path)) {
      DINGO_LOG(ERROR) << fmt::format("Failed to remove the existing destination {} ", dst_path);
      return false;
    }
  }

  // Perform the rename operation
  try {
    std::filesystem::rename(source_path, destination_path);
  } catch (const std::exception& ex) {
    DINGO_LOG(ERROR) << fmt::format("Rename operation failed, src_path: {}, dst_path: {}, error: {}", src_path,
                                    dst_path, ex.what());
    return false;
  }

  return true;
}

bool Helper::IsExistPath(const std::string& path) { return std::filesystem::exists(path); }

int64_t Helper::GetFileSize(const std::string& path) {
  try {
    std::uintmax_t size = std::filesystem::file_size(path);
    DINGO_LOG(INFO) << fmt::format("File size: {} bytes", size);
    return size;
  } catch (const std::filesystem::filesystem_error& ex) {
    DINGO_LOG(ERROR) << fmt::format("Get file size failed, path: {}, error: {}", path, ex.what());
    return -1;
  }
}

std::string Helper::GenerateRandomString(int length) {
  std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  std::string rand_string;

  unsigned int seed = time(nullptr);  // Get seed value for rand_r()

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

static bool IsValidFileAddr(const std::string& coor_url) { return coor_url.substr(0, 7) == "file://"; }
static bool IsValidListAddr(const std::string& coor_url) { return coor_url.substr(0, 7) == "list://"; }

static std::string ParseFileUrl(const std::string& coor_url) {
  CHECK(coor_url.substr(0, 7) == "file://") << "Invalid coor_url: " << coor_url;

  std::string file_path = coor_url.substr(7);

  std::ifstream file(file_path);
  if (!file.is_open()) {
    DINGO_LOG(ERROR) << fmt::format("Open file({}) failed, maybe not exist!", file_path);
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

static std::string ParseListUrl(const std::string& coor_url) {
  CHECK(coor_url.substr(0, 7) == "list://") << "Invalid coor_url: " << coor_url;

  return coor_url.substr(7);
}

std::string Helper::ParseCoorAddr(const std::string& coor_url) {
  std::string coor_addrs;
  if (IsValidFileAddr(coor_url)) {
    coor_addrs = ParseFileUrl(coor_url);

  } else if (IsValidListAddr(coor_url)) {
    coor_addrs = ParseListUrl(coor_url);

  } else {
    DINGO_LOG(ERROR) << "Invalid coor_url: " << coor_url;
  }

  return coor_addrs;
}

}  // namespace mdsv2
}  // namespace dingofs