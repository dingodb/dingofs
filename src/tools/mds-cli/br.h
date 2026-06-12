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

#ifndef DINGOFS_MDS_CLIENT_BR_H_
#define DINGOFS_MDS_CLIENT_BR_H_

#include <sys/types.h>

#include <cstdint>
#include <string>

#include "mds/common/status.h"
#include "mds/common/type.h"
#include "mds/filesystem/store_operation.h"

namespace dingofs {
namespace mds {
namespace br {

enum class Type : uint8_t {
  kFile,
  kStdout,
  kS3,
};

class Output {
 public:
  Output() = default;
  virtual ~Output() = default;

  virtual bool Init() = 0;
  virtual void Append(const std::string& key, const std::string& value) = 0;
  virtual void Flush() = 0;
};

using OutputUPtr = std::unique_ptr<Output>;

class Input {
 public:
  Input() = default;
  virtual ~Input() = default;

  virtual bool Init() = 0;
  virtual bool IsEof() const = 0;
  virtual Status Read(std::string& key, std::string& value) = 0;
};

using InputUPtr = std::unique_ptr<Input>;

class Backup {
 public:
  Backup() = default;
  ~Backup();

  struct Options {
    Type type = Type::kStdout;  // output type
    bool is_binary = false;     // output in binary format
    std::string file_path;      // output file path (if type is kFile)

    uint32_t fs_id{0};  // file system ID

    S3Info s3_info;
  };

  bool Init(const std::string& coor_addr);
  void Destroy();

  Status BackupMetaTable(const Options& options);
  Status BackupFsMetaTable(const Options& options, uint32_t fs_id);

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
    Type type = Type::kStdout;  // output type
    bool is_binary = false;     // output in binary format
    std::string file_path;      // output file path (if type is kFile)

    uint32_t fs_id{0};  // file system ID
    bool is_force{false};

    S3Info s3_info;
  };

  bool Init(const std::string& coor_addr);
  void Destroy();

  Status RestoreMetaTable(const Options& options);
  Status RestoreFsMetaTable(const Options& options, uint32_t fs_id);

 private:
  Status IsExistMetaTable();
  Status IsExistFsMetaTable(uint32_t fs_id);

  Status CreateMetaTable();
  Status CreateFsMetaTable(uint32_t fs_id, const std::string& fs_name);

  Status GetFsInfo(uint32_t fs_id, FsInfoEntry& fs_info);

  Status RestoreMetaTable(InputUPtr input);
  Status RestoreFsMetaTable(uint32_t fs_id, InputUPtr input, bool is_force);

  OperationProcessorSPtr operation_processor_;
};

class BackupCommandRunner {
 public:
  BackupCommandRunner() = default;
  ~BackupCommandRunner() = default;

  struct Options {
    uint32_t cluster_id{0};
    std::string type;
    std::string output_type;
    uint32_t fs_id{0};
    std::string fs_name;
    std::string file_path;
    bool is_binary{false};

    S3Info s3_info;
  };

  static bool Run(const Options& options, const std::string& coor_addr, const std::string& cmd);
};

class RestoreCommandRunner {
 public:
  RestoreCommandRunner() = default;
  ~RestoreCommandRunner() = default;

  struct Options {
    uint32_t cluster_id{0};
    std::string type;
    std::string input_type;
    uint32_t fs_id{0};
    std::string fs_name;
    std::string file_path;
    bool is_force{false};

    S3Info s3_info;  // S3 information for backup and restore
  };

  static bool Run(const Options& options, const std::string& coor_addr, const std::string& cmd);
};

}  // namespace br
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_CLIENT_BR_H_
