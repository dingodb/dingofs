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

#include "butil/endpoint.h"
#include "fmt/core.h"
#include "google/protobuf/util/json_util.h"

namespace dingofs {
namespace mdsv2 {

class Helper {
 public:
  static int64_t GetPid();

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
  static std::string FormatTime(int64_t timestamp);

  static std::string FormatNsTime(int64_t timestamp);

  // format: "2021-01-01T00:00:00.000Z"
  static std::string GetNowFormatMsTime();

  static bool IsEqualIgnoreCase(const std::string& str1, const std::string& str2);
  static std::string ToUpperCase(const std::string& str);
  static std::string ToLowerCase(const std::string& str);

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

  static std::string EndPointToString(const butil::EndPoint& endpoint);

  static std::string ParseCoorAddr(const std::string& coor_url);

  static bool SaveFile(const std::string& filepath, const std::string& data);

  static std::string FsModeToString(mode_t mode);

  static bool ProtoToJson(const google::protobuf::Message& message, std::string& json);

  // protobuf transform
  template <typename T>
  static std::vector<T> PbRepeatedToVector(const google::protobuf::RepeatedPtrField<T>& data) {
    std::vector<T> vec;
    vec.reserve(data.size());
    for (auto& item : data) {
      vec.emplace_back(std::move(item));
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(google::protobuf::RepeatedPtrField<T>* data) {
    std::vector<T> vec;
    vec.reserve(data->size());
    for (auto& item : *data) {
      vec.emplace_back(std::move(item));
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(const google::protobuf::RepeatedField<T>& data) {
    std::vector<T> vec;
    vec.reserve(data.size());
    for (auto& item : data) {
      vec.push_back(item);
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(google::protobuf::RepeatedField<T>* data) {
    std::vector<T> vec;
    vec.reserve(data->size());
    for (auto& item : *data) {
      vec.push_back(item);
    }

    return vec;
  }

  template <typename T>
  static void VectorToPbRepeated(const std::vector<T>& vec, google::protobuf::RepeatedPtrField<T>* out) {
    for (auto& item : vec) {
      *(out->Add()) = item;
    }
  }

  template <typename T>
  static void VectorToPbRepeated(const std::vector<T>& vec, google::protobuf::RepeatedField<T>* out) {
    for (auto& item : vec) {
      out->Add(item);
    }
  }

  template <typename T>
  static std::string VectorToString(const std::vector<T>& vec) {
    std::string str;
    for (int i = 0; i < vec.size(); ++i) {
      str += fmt::format("{}", vec[i]);
      if (i + 1 < vec.size()) {
        str += ",";
      }
    }
    return str;
  }
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_COMMON_HELPER_H_
