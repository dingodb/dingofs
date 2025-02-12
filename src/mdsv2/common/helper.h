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

#ifndef DINGOFS_MDSV2_COMMON_HELPER_H_
#define DINGOFS_MDSV2_COMMON_HELPER_H_

#include <string>
#include <vector>

namespace dingofs {
namespace mdsv2 {

class Helper {
 public:
  // nanosecond timestamp
  static int64_t TimestampNs();
  // microseconds
  static int64_t TimestampUs();
  // millisecond timestamp
  static int64_t TimestampMs();
  // second timestamp
  static int64_t Timestamp();
  static std::string NowTime();

  // format millisecond timestamp
  static std::string FormatMsTime(int64_t timestamp, const std::string& format);
  static std::string FormatMsTime(int64_t timestamp);
  // format second timestamp
  static std::string FormatTime(int64_t timestamp, const std::string& format);

  // format: "2021-01-01T00:00:00.000Z"
  static std::string GetNowFormatMsTime();

  static bool IsEqualIgnoreCase(const std::string& str1, const std::string& str2);

  // string type cast
  static bool StringToBool(const std::string& str);
  static int32_t StringToInt32(const std::string& str);
  static int64_t StringToInt64(const std::string& str);
  static float StringToFloat(const std::string& str);
  static double StringToDouble(const std::string& str);

  static void SplitString(const std::string& str, char c, std::vector<std::string>& vec);
  static void SplitString(const std::string& str, char c, std::vector<int64_t>& vec);

  static std::string StringToHex(const std::string& str);
  static std::string StringToHex(const std::string_view& str);
  static std::string HexToString(const std::string& hex_str);

  static bool ParseAddr(const std::string& addr, std::string& host, int& port);

  // local file system operation
  static std::string ConcatPath(const std::string& path1, const std::string& path2);
  static std::vector<std::string> TraverseDirectory(const std::string& path, bool ignore_dir = false,
                                                    bool ignore_file = false);
  static std::vector<std::string> TraverseDirectory(const std::string& path, const std::string& prefix,
                                                    bool ignore_dir = false, bool ignore_file = false);
  static std::string FindFileInDirectory(const std::string& dirpath, const std::string& prefix);
  static bool CreateDirectory(const std::string& path);
  static bool CreateDirectories(const std::string& path);
  static bool RemoveFileOrDirectory(const std::string& path);
  static bool RemoveAllFileOrDirectory(const std::string& path);
  static bool Rename(const std::string& src_path, const std::string& dst_path, bool is_force = true);
  static bool IsExistPath(const std::string& path);
  static int64_t GetFileSize(const std::string& path);

  static std::string GenerateRandomString(int length);
  static int64_t GenerateRealRandomInteger(int64_t min_value, int64_t max_value);
  static int64_t GenerateRandomInteger(int64_t min_value, int64_t max_value);
  static float GenerateRandomFloat(float min_value, float max_value);

  static std::string PrefixNext(const std::string& input);
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_COMMON_HELPER_H_
