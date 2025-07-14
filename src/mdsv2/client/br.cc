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

#include <aws/s3-crt/model/PutBucketLifecycleConfigurationResult.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <vector>

#include "dingofs/error.pb.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/constant.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/filesystem/store_operation.h"
#include "mdsv2/storage/dingodb_storage.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {
namespace br {

const uint32_t kImportKVBatchSize = 1024;

class FileOutput;
using FileOutputUPtr = std::unique_ptr<FileOutput>;

class StdOutput;
using StdOutputUPtr = std::unique_ptr<StdOutput>;

class S3Output;
using S3OutputUPtr = std::unique_ptr<S3Output>;

class FileInput;
using FileInputUPtr = std::unique_ptr<FileInput>;

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

// input from file
class FileInput : public Input {
 public:
  explicit FileInput(const std::string& file_path) : file_path_(file_path) {
    file_stream_.open(file_path_, std::ios::in);
    if (!file_stream_.is_open()) {
      throw std::runtime_error(fmt::format("open file fail: {}", file_path_));
    }
  }
  ~FileInput() override {
    if (file_stream_.is_open()) file_stream_.close();
  }

  static InputUPtr New(const std::string& file_path) { return std::make_unique<FileInput>(file_path); }

  bool IsEof() const override { return file_stream_.eof(); }

  Status Read(std::string& key, std::string& value) override {
    if (file_stream_.eof()) return Status(pb::error::EINTERNAL, "end of file");

    if (!std::getline(file_stream_, key)) return Status(pb::error::EINTERNAL, "read key fail");
    if (!std::getline(file_stream_, value)) return Status(pb::error::EINTERNAL, "read value fail");

    return Status::OK();
  }

 private:
  std::string file_path_;
  std::ifstream file_stream_;
};

// input from S3
class S3Input : public Input {
 public:
  S3Input(const std::string& bucket_name, const std::string& object_name)
      : bucket_name_(bucket_name), object_name_(object_name) {
    // Initialize S3 client and prepare to read from the specified object
  }

  bool IsEof() const override {
    // Check if the end of the S3 object has been reached
    return false;  // Placeholder
  }

  Status Read(std::string& key, std::string& value) override {
    // Read a key-value pair from the S3 object
    return Status::OK();  // Placeholder
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

Status Backup::BackupMetaTable(const Options& options) {
  OutputUPtr output;
  switch (options.type) {
    case Output::Type::kFile:
      output = FileOutput::New(options.file_path);
      break;
    case Output::Type::kStdout:
      output = StdOutput::New(options.is_binary);
      break;
    default:
      return Status(pb::error::EINTERNAL, "unsupported output type");
  }

  return BackupMetaTable(std::move(output));
}

Status Backup::BackupFsMetaTable(const Options& options, uint32_t fs_id) {
  if (fs_id == 0) {
    return Status(pb::error::EINTERNAL, "fs_id is zero");
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
      return Status(pb::error::EINTERNAL, "unsupported output type");
  }

  return BackupFsMetaTable(fs_id, std::move(output));
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
      "client_heartbeat_count({}) fs_count({}) fs_quota_count({}).\n",
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
      "del_slice_count({}) del_file_count({}).\n",
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

Status Restore::RestoreMetaTable(const Options& options) {
  InputUPtr input;
  switch (options.type) {
    case Input::Type::kFile:
      input = FileInput::New(options.file_path);
      break;

    default:
      return Status(pb::error::EINTERNAL, "unsupported input type");
  }

  return RestoreMetaTable(std::move(input));
}

Status Restore::RestoreFsMetaTable(const Options& options, uint32_t fs_id) {
  if (fs_id == 0) {
    return Status(pb::error::EINTERNAL, "fs_id is zero");
  }

  InputUPtr input;
  switch (options.type) {
    case Input::Type::kFile:
      input = FileInput::New(options.file_path);
      break;

    default:
      return Status(pb::error::EINTERNAL, "unsupported input type");
  }

  return RestoreFsMetaTable(fs_id, std::move(input));
}

Status Restore::IsExistMetaTable() {
  auto range = MetaCodec::GetMetaTableRange();
  return operation_processor_->CheckTable(range);
}

Status Restore::IsExistFsMetaTable(uint32_t fs_id) {
  auto range = MetaCodec::GetFsMetaTableRange(fs_id);
  return operation_processor_->CheckTable(range);
}

Status Restore::CreateMetaTable() {
  int64_t table_id = 0;
  auto range = MetaCodec::GetMetaTableRange();
  auto status = operation_processor_->CreateTable(kMetaTableName, range, table_id);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, fmt::format("create meta table fail, {}", status.error_str()));
  }

  return Status::OK();
}

Status Restore::CreateFsMetaTable(uint32_t fs_id, const std::string& fs_name) {
  int64_t table_id = 0;
  auto range = MetaCodec::GetFsMetaTableRange(fs_id);
  auto status = operation_processor_->CreateTable(GenFsMetaTableName(fs_name), range, table_id);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, fmt::format("create fs meta table fail, {}", status.error_str()));
  }

  return Status::OK();
}

Status Restore::GetFsInfo(uint32_t fs_id, FsInfoType& fs_info) {
  Trace trace;
  ScanFsOperation operation(trace);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, fmt::format("scan fs info fail, status({})", status.error_str()));
  }

  auto& result = operation.GetResult();

  for (const auto& fs : result.fs_infoes) {
    if (fs.fs_id() == fs_id) {
      fs_info = fs;
      return Status::OK();
    }
  }

  return Status(pb::error::ENOT_FOUND, "not found fs");
}

Status Restore::RestoreMetaTable(InputUPtr input) {
  CHECK(input != nullptr) << "input is nullptr.";

  // check meta table exist
  // if it does not exist, create it
  auto status = IsExistMetaTable();
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      return status;
    }

    status = CreateMetaTable();
    if (!status.ok()) return status;
  }

  // import meta to table
  uint64_t total_count = 0, lock_count = 0, auto_increment_id_count = 0;
  uint64_t mds_heartbeat_count = 0, client_heartbeat_count = 0, fs_count = 0, fs_quota_count = 0;

  std::vector<KeyValue> kvs;
  while (true) {
    if (input->IsEof()) break;

    KeyValue kv;
    status = input->Read(kv.key, kv.value);
    if (!status.ok()) break;

    if (MetaCodec::IsLockKey(kv.key)) {
      ++lock_count;
    } else if (MetaCodec::IsAutoIncrementIDKey(kv.key)) {
      ++auto_increment_id_count;
    } else if (MetaCodec::IsMdsHeartbeatKey(kv.key)) {
      ++mds_heartbeat_count;
    } else if (MetaCodec::IsClientHeartbeatKey(kv.key)) {
      ++client_heartbeat_count;
    } else if (MetaCodec::IsFsKey(kv.key)) {
      ++fs_count;
    } else if (MetaCodec::IsFsQuotaKey(kv.key)) {
      ++fs_quota_count;
    } else {
      status = Status(pb::error::EINTERNAL, fmt::format("unknown key type: {}", Helper::StringToHex(kv.key)));
      break;
    }

    ++total_count;

    kvs.push_back(std::move(kv));

    if (kvs.size() > kImportKVBatchSize || input->IsEof()) {
      Trace trace;
      ImportKVOperation operation(trace, std::move(kvs));
      status = operation_processor_->RunAlone(&operation);
      if (!status.ok()) {
        break;
      }

      kvs.clear();
    }
  }

  std::cout << fmt::format(
      "restore meta table done, total_count({}) lock_count({}) auto_increment_id_count({}) mds_heartbeat_count({}) "
      "client_heartbeat_count({}) fs_count({}) fs_quota_count({}) status({}).\n",
      total_count, lock_count, auto_increment_id_count, mds_heartbeat_count, client_heartbeat_count, fs_count,
      fs_quota_count, status.error_str());

  return Status::OK();
}

Status Restore::RestoreFsMetaTable(uint32_t fs_id, InputUPtr input) {
  // get fs info
  FsInfoType fs_info;
  auto status = GetFsInfo(fs_id, fs_info);
  if (!status.ok()) return status;

  // check fs meta table exist
  status = IsExistFsMetaTable(fs_id);
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      return status;
    }

    // if it does not exist, create it
    status = CreateFsMetaTable(fs_id, fs_info.fs_name());
    if (!status.ok()) return status;
  }

  // import fs meta to table

  uint64_t total_count = 0, dir_quota_count = 0, inode_count = 0;
  uint64_t file_session_count = 0, del_slice_count = 0, del_file_count = 0;

  std::vector<KeyValue> kvs;
  while (true) {
    if (input->IsEof()) break;

    KeyValue kv;
    status = input->Read(kv.key, kv.value);
    if (!status.ok()) break;

    if (MetaCodec::IsDirQuotaKey(kv.key)) {
      ++dir_quota_count;
    } else if (MetaCodec::IsInodeKey(kv.key)) {
      ++inode_count;
    } else if (MetaCodec::IsFileSessionKey(kv.key)) {
      ++file_session_count;
    } else if (MetaCodec::IsDelSliceKey(kv.key)) {
      ++del_slice_count;
    } else if (MetaCodec::IsDelFileKey(kv.key)) {
      ++del_file_count;
    }

    ++total_count;

    kvs.push_back(std::move(kv));

    if (kvs.size() > kImportKVBatchSize || input->IsEof()) {
      Trace trace;
      ImportKVOperation operation(trace, std::move(kvs));
      status = operation_processor_->RunAlone(&operation);
      if (!status.ok()) {
        break;
      }

      kvs.clear();
    }
  }

  std::cout << fmt::format(
      "restore fsmeta table done, total_count({}) dir_quota_count({}) inode_count({}) file_session_count({}) "
      "del_slice_count({}) del_file_count({}) status({}).\n",
      total_count, dir_quota_count, inode_count, file_session_count, del_slice_count, del_file_count,
      status.error_str());

  return Status::OK();
}

bool BackupCommandRunner::Run(const Options& options, const std::string& coor_addr, const std::string& cmd) {
  using Helper = dingofs::mdsv2::Helper;

  if (cmd != "backup") return false;

  if (coor_addr.empty()) {
    std::cout << "coordinator address is empty." << '\n';
    return true;
  }

  Backup backup;
  if (!backup.Init(coor_addr)) {
    std::cout << "init backup fail." << '\n';
    return true;
  }

  Backup::Options inner_options;
  inner_options.type = Output::Type::kStdout;
  if (options.output_type == "file") {
    inner_options.type = Output::Type::kFile;
    inner_options.file_path = options.file_path;
  } else if (options.output_type == "s3") {
    inner_options.type = Output::Type::kS3;
  } else {
    std::cout << "unknown output type: " << options.output_type << '\n';
    return true;
  }

  if (options.type == Helper::ToLowerCase("meta")) {
    auto status = backup.BackupMetaTable(inner_options);
    if (!status.ok()) {
      std::cout << fmt::format("backup meta table fail, status({}).", status.error_str()) << '\n';
    }

  } else if (options.type == Helper::ToLowerCase("fsmeta")) {
    auto status = backup.BackupFsMetaTable(inner_options, options.fs_id);
    if (!status.ok()) {
      std::cout << fmt::format("backup fsmeta table fail, status({}).", status.error_str()) << '\n';
    }

  } else {
    std::cout << "unknown type: " << options.type << '\n';
  }

  return true;
}

bool RestoreCommandRunner::Run(const Options& options, const std::string& coor_addr,  // NOLINT
                               const std::string& cmd) {                              // NOLINT
  if (cmd != "restore") return false;

  if (coor_addr.empty()) {
    std::cout << "coordinator address is empty." << '\n';
    return true;
  }

  dingofs::mdsv2::br::Restore restore;
  if (!restore.Init(coor_addr)) {
    std::cout << "init restore fail." << '\n';
    return true;
  }

  dingofs::mdsv2::br::Restore::Options inner_options;
  if (options.output_type == "file") {
    inner_options.type = Input::Type::kFile;
    inner_options.file_path = options.file_path;

  } else if (options.output_type == "s3") {
    inner_options.type = Input::Type::kS3;
  } else {
    std::cout << "unknown input type: " << options.output_type << '\n';
    return true;
  }

  if (options.type == Helper::ToLowerCase("meta")) {
    auto status = restore.RestoreMetaTable(inner_options);
    if (!status.ok()) {
      std::cout << fmt::format("restore meta table fail, status({}).", status.error_str()) << '\n';
    }

  } else if (options.type == Helper::ToLowerCase("fsmeta")) {
    auto status = restore.RestoreFsMetaTable(inner_options, options.fs_id);
    if (!status.ok()) {
      std::cout << fmt::format("restore fsmeta table fail, status({}).", status.error_str()) << '\n';
    }

  } else {
    std::cout << "unknown type: " << options.type << '\n';
  }

  return true;
}

}  // namespace br
}  // namespace mdsv2
}  // namespace dingofs