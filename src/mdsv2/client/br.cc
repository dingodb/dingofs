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

#include "mdsv2/client/br.h"

#include <glog/logging.h>

#include <cstdint>
#include <fstream>
#include <memory>

#include "mdsv2/common/codec.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/filesystem/store_operation.h"
#include "mdsv2/storage/dingodb_storage.h"

namespace dingofs {
namespace mdsv2 {
namespace br {

class FileOutput;
using FileOutputUPtr = std::unique_ptr<FileOutput>;

class StdOutput;
using StdOutputUPtr = std::unique_ptr<StdOutput>;

class S3Output;
using S3OutputUPtr = std::unique_ptr<S3Output>;

// output to a file
class FileOutput : public Output {
 public:
  explicit FileOutput(const std::string& file_path) : file_path_(file_path) {
    file_stream_.open(file_path_, std::ios::out | std::ios::binary);
    if (!file_stream_.is_open()) {
      throw std::runtime_error("failed open file: " + file_path_);
    }
  }
  ~FileOutput() override {
    if (file_stream_.is_open()) file_stream_.close();
  }

  static FileOutputUPtr New(const std::string& file_path) { return std::make_unique<FileOutput>(file_path); }

  void Append(const std::string& key, const std::string& value) override {
    file_stream_ << key << "\n" << value << "\n";
  }

  void Flush() override { file_stream_.flush(); }

 private:
  std::string file_path_;
  std::ofstream file_stream_;
};

// output to standard output
class StdOutput : public Output {
 public:
  StdOutput(bool is_binary = false) : is_binary_(is_binary) {}

  static StdOutputUPtr New(bool is_binary = false) { return std::make_unique<StdOutput>(is_binary); }

  void Append(const std::string& key, const std::string& value) override {
    if (is_binary_) {
      std::cout << Helper::StringToHex(key) << ": " << Helper::StringToHex(value) << "\n";

    } else {
      auto desc = MetaCodec::ParseKey(key, value);
      std::cout << fmt::format("key({}) value({})\n", desc.first, desc.second);
    }
  }

  void Flush() override { std::cout.flush(); }

 private:
  bool is_binary_{false};
};

// output to S3
class S3Output : public Output {
 public:
  S3Output(const std::string& bucket_name, const std::string& object_name)
      : bucket_name_(bucket_name), object_name_(object_name) {
    // Initialize S3 client here
  }

  static S3OutputUPtr New(const std::string& bucket_name, const std::string& object_name) {
    return std::make_unique<S3Output>(bucket_name, object_name);
  }

  void Append(const std::string& key, const std::string& value) override {
    // Upload key-value pair to S3
  }

  void Flush() override {
    // Finalize the upload to S3
  }

 private:
  std::string bucket_name_;
  std::string object_name_;
};

Backup::~Backup() { Destroy(); }

bool Backup::Init(const std::string& coor_addr) {
  CHECK(!coor_addr.empty()) << "coor addr is empty.";

  auto kv_storage = DingodbStorage::New();
  CHECK(kv_storage != nullptr) << "new DingodbStorage fail.";

  std::string store_addrs = Helper::ParseCoorAddr(coor_addr);
  if (store_addrs.empty()) {
    return false;
  }

  if (!kv_storage->Init(store_addrs)) {
    return false;
  }

  operation_processor_ = OperationProcessor::New(kv_storage);

  return operation_processor_->Init();
}

void Backup::Destroy() { operation_processor_->Destroy(); }

void Backup::BackupMetaTable(const Options& options) {
  OutputUPtr output;
  switch (options.type) {
    case Output::Type::kFile:
      output = FileOutput::New(options.file_path);
      break;
    case Output::Type::kStdout:
      output = StdOutput::New(options.is_binary);
      break;
    case Output::Type::kS3:
      output = S3Output::New("bucket_name", "object_name");
      break;
    default:
      throw std::runtime_error("unsupported output type");
  }

  auto status = BackupMetaTable(std::move(output));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("backup meta table fail, status({}).", status.error_str());
  }
}

void Backup::BackupFsMetaTable(const Options& options, uint32_t fs_id) {
  if (fs_id == 0) {
    DINGO_LOG(ERROR) << "fs_id is zero, cannot backup fs meta table.";
    return;
  }

  OutputUPtr output;
  switch (options.type) {
    case Output::Type::kFile:
      output = FileOutput::New(options.file_path);
      break;
    case Output::Type::kStdout:
      output = StdOutput::New(options.is_binary);
      break;
    case Output::Type::kS3:
      output = S3Output::New(options.bucket_name, options.object_name);
      break;
    default:
      throw std::runtime_error("unsupported output type");
  }

  auto status = BackupFsMetaTable(fs_id, std::move(output));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("backup fsmeta table fail, status({}).", status.error_str());
  }
}

Status Backup::BackupMetaTable(OutputUPtr output) {
  CHECK(output != nullptr) << "output is nullptr.";

  uint64_t total_count = 0, lock_count = 0, auto_increment_id_count = 0;
  uint64_t mds_heartbeat_count = 0, client_heartbeat_count = 0, fs_count = 0, fs_quota_count = 0;

  Trace trace;
  ScanMetaTableOperation operation(trace, [&](const std::string& key, const std::string& value) -> bool {
    output->Append(key, value);

    if (MetaCodec::IsLockKey(key)) {
      ++lock_count;
    } else if (MetaCodec::IsAutoIncrementIDKey(key)) {
      ++auto_increment_id_count;
    } else if (MetaCodec::IsMdsHeartbeatKey(key)) {
      ++mds_heartbeat_count;
    } else if (MetaCodec::IsClientHeartbeatKey(key)) {
      ++client_heartbeat_count;
    } else if (MetaCodec::IsFsKey(key)) {
      ++fs_count;
    } else if (MetaCodec::IsFsQuotaKey(key)) {
      ++fs_quota_count;
    }

    ++total_count;

    return true;
  });

  auto status = operation_processor_->RunAlone(&operation);

  std::cout << fmt::format(
      "backup meta table done, total_count({}) lock_count({}) auto_increment_id_count({}) mds_heartbeat_count({}) "
      "client_heartbeat_count({}) fs_count({}) fs_quota_count({}).",
      total_count, lock_count, auto_increment_id_count, mds_heartbeat_count, client_heartbeat_count, fs_count,
      fs_quota_count);

  return status;
}

Status Backup::BackupFsMetaTable(uint32_t fs_id, OutputUPtr output) {
  CHECK(output != nullptr) << "output is nullptr.";

  uint64_t total_count = 0, dir_quota_count = 0, inode_count = 0;
  uint64_t file_session_count = 0, del_slice_count = 0, del_file_count = 0;

  Trace trace;
  ScanFsMetaTableOperation operation(trace, fs_id, [&](const std::string& key, const std::string& value) -> bool {
    output->Append(key, value);

    if (MetaCodec::IsDirQuotaKey(key)) {
      ++dir_quota_count;
    } else if (MetaCodec::IsInodeKey(key)) {
      ++inode_count;
    } else if (MetaCodec::IsFileSessionKey(key)) {
      ++file_session_count;
    } else if (MetaCodec::IsDelSliceKey(key)) {
      ++del_slice_count;
    } else if (MetaCodec::IsDelFileKey(key)) {
      ++del_file_count;
    }

    ++total_count;

    return true;
  });

  auto status = operation_processor_->RunAlone(&operation);

  std::cout << fmt::format(
      "backup fsmeta table done, total_count({}) dir_quota_count({}) inode_count({}) file_session_count({}) "
      "del_slice_count({}) del_file_count({}).",
      total_count, dir_quota_count, inode_count, file_session_count, del_slice_count, del_file_count);

  return status;
}

bool Restore::Init(const std::string& coor_addr) {
  CHECK(!coor_addr.empty()) << "coor addr is empty.";

  auto kv_storage = DingodbStorage::New();
  CHECK(kv_storage != nullptr) << "new DingodbStorage fail.";

  std::string store_addrs = Helper::ParseCoorAddr(coor_addr);
  if (store_addrs.empty()) {
    return false;
  }

  if (!kv_storage->Init(store_addrs)) {
    return false;
  }

  operation_processor_ = OperationProcessor::New(kv_storage);

  return operation_processor_->Init();
}

void Restore::Destroy() {
  if (operation_processor_) {
    operation_processor_->Destroy();
  }
}

void Restore::RestoreMetaTable(const Options& options) {}

void Restore::RestoreFsMetaTable(const Options& options, uint32_t fs_id) {}

Status Restore::RestoreMetaTable(InputUPtr input) { return Status::OK(); }

Status Restore::RestoreFsMetaTable(uint32_t fs_id, InputUPtr input) { return Status::OK(); }

bool BackupCommandRunner::Run(const Options& options, const std::string& coor_addr, const std::string& cmd) {
  using Helper = dingofs::mdsv2::Helper;

  if (cmd != "backup") return false;

  if (coor_addr.empty()) {
    std::cout << "coordinator address is empty." << '\n';
    return true;
  }

  dingofs::mdsv2::br::Backup backup;
  if (!backup.Init(coor_addr)) {
    std::cout << "init backup fail." << '\n';
    return true;
  }

  dingofs::mdsv2::br::Backup::Options inner_options;
  inner_options.type = dingofs::mdsv2::br::Output::Type::kStdout;
  if (options.output_type == "file") {
    inner_options.type = dingofs::mdsv2::br::Output::Type::kFile;
    inner_options.file_path = options.file_path;
  } else if (options.output_type == "s3") {
    inner_options.type = dingofs::mdsv2::br::Output::Type::kS3;
  }

  if (options.type == Helper::ToLowerCase("meta")) {
    backup.BackupMetaTable(inner_options);

  } else if (options.type == Helper::ToLowerCase("fsmeta")) {
    backup.BackupFsMetaTable(inner_options, options.fs_id);

  } else {
    std::cout << "unknown type: " << options.type << '\n';
  }

  return true;
}

bool RestoreCommandRunner::Run(const Options& options, const std::string& coor_addr,  // NOLINT
                               const std::string& cmd) {                              // NOLINT
  if (cmd != "restore") return false;

  return true;
}

}  // namespace br
}  // namespace mdsv2
}  // namespace dingofs