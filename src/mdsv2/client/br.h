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

#ifndef DINGOFS_MDSV2_CLIENT_BR_H_
#define DINGOFS_MDSV2_CLIENT_BR_H_

#include <sys/types.h>

#include <cstdint>
#include <string>

#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/store_operation.h"

namespace dingofs {
namespace mdsv2 {
namespace br {

class Output {
 public:
  Output() = default;
  virtual ~Output() = default;

  enum class Type : uint8_t {
    kFile,
    kStdout,
    kS3,
  };

  virtual void Append(const std::string& key, const std::string& value) = 0;

  virtual void Flush() = 0;
};

using OutputUPtr = std::unique_ptr<Output>;

class Input {
 public:
  Input() = default;
  virtual ~Input() = default;

  enum class Type : uint8_t {
    kFile,
    kS3,
  };

  virtual bool Read(std::string& key, std::string& value) = 0;
};

using InputUPtr = std::unique_ptr<Input>;

class Backup {
 public:
  Backup() = default;
  ~Backup();

  struct Options {
    Output::Type type = Output::Type::kStdout;  // output type
    bool is_binary = false;                     // output in binary format
    std::string file_path;                      // output file path (if type is kFile)
    std::string bucket_name;                    // S3 bucket name
    std::string object_name;                    // S3 object name
  };

  bool Init(const std::string& coor_addr);
  void Destroy();

  void BackupMetaTable(const Options& options);
  void BackupFsMetaTable(const Options& options, uint32_t fs_id);

 private:
  Status BackupMetaTable(OutputUPtr output);
  Status BackupFsMetaTable(uint32_t fs_id, OutputUPtr output);

  OperationProcessorSPtr operation_processor_;
};

class Restore {
 public:
  Restore() = default;
  ~Restore() = default;

  struct Options {
    Input::Type type = Input::Type::kFile;  // input type
    std::string file_path;                  // input file path (if type is kFile)
    std::string bucket_name;                // S3 bucket name
    std::string object_name;                // S3 object name
  };

  bool Init(const std::string& coor_addr);
  void Destroy();

  void RestoreMetaTable(const Options& options);
  void RestoreFsMetaTable(const Options& options, uint32_t fs_id);

 private:
  Status RestoreMetaTable(InputUPtr input);
  Status RestoreFsMetaTable(uint32_t fs_id, InputUPtr input);

  OperationProcessorSPtr operation_processor_;
};

class BackupCommandRunner {
 public:
  BackupCommandRunner() = default;
  ~BackupCommandRunner() = default;

  struct Options {
    std::string type;
    std::string output_type;
    uint32_t fs_id{0};
    std::string fs_name;
    std::string file_path;
    bool is_binary{false};
    std::string bucket_name;
    std::string object_name;
  };

  static bool Run(const Options& options, const std::string& coor_addr, const std::string& cmd);
};

class RestoreCommandRunner {
 public:
  RestoreCommandRunner() = default;
  ~RestoreCommandRunner() = default;

  struct Options {
    std::string type;
    std::string output_type;
    uint32_t fs_id{0};
    std::string fs_name;
    std::string file_path;
    std::string bucket_name;
    std::string object_name;
  };

  static bool Run(const Options& options, const std::string& coor_addr, const std::string& cmd);
};

}  // namespace br
}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_CLIENT_BR_H_
