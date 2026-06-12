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

#include "mds/client/br.h"

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "common/blockaccess/block_accesser.h"
#include "common/const.h"
#include "common/logging.h"
#include "dingofs/error.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/codec.h"
#include "mds/common/helper.h"
#include "mds/common/status.h"
#include "mds/common/tracing.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/dingodb_storage.h"
#include "mds/storage/storage.h"

namespace dingofs {
namespace mds {
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

// encode/decode key/value
// format: key length|value length|key|value
static std::string EncodeKeyValue(const std::string& key, const std::string& value) {
  std::string encoded;
  encoded.reserve(4 + 4 + key.size() + value.size());

  // encode key
  uint32_t key_length = key.size();
  encoded.append(reinterpret_cast<const char*>(&key_length), 4);
  encoded.append(key);

  // encode value
  uint32_t value_length = value.size();
  encoded.append(reinterpret_cast<const char*>(&value_length), 4);
  encoded.append(value);

  return encoded;
}

static bool DecodeKeyValue(const std::string& encoded, size_t& offset, std::string& key, std::string& value) {
  if (encoded.size() < offset + 8) return false;

  // parse key length
  uint32_t key_length = *reinterpret_cast<const uint32_t*>(encoded.data() + offset);
  offset += 4;

  if (encoded.size() < offset + key_length + 4) return false;
  key.assign(encoded.data() + offset, key_length);
  offset += key_length;

  // parse value length
  uint32_t value_length = *reinterpret_cast<const uint32_t*>(encoded.data() + offset);
  offset += 4;

  if (encoded.size() < offset + value_length) return false;
  value.assign(encoded.data() + offset, value_length);
  offset += value_length;

  return true;
}

// output to standard output
class StdOutput : public Output {
 public:
  StdOutput(bool is_binary = false) : is_binary_(is_binary) {}

  static StdOutputUPtr New(bool is_binary = false) { return std::make_unique<StdOutput>(is_binary); }

  bool Init() override { return true; }

  void Append(const std::string& key, const std::string& value) override {
    if (is_binary_) {
      std::cout << Helper::StringToHex(key) << ": " << Helper::StringToHex(value) << "\n";

    } else {
      auto desc = MetaCodec::ParseKey(key, value);
      std::cout << fmt::format("{}. key({}) value({})\n", ++count_, desc.first, desc.second);
    }
  }

  void Flush() override { std::cout.flush(); }

 private:
  uint32_t count_{0};  // count of key/value pairs
  bool is_binary_{false};
};

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

  bool Init() override { return true; }
  void Append(const std::string& key, const std::string& value) override { file_stream_ << EncodeKeyValue(key, value); }

  void Flush() override { file_stream_.flush(); }

 private:
  std::string file_path_;
  std::ofstream file_stream_;
};

static blockaccess::BlockAccesserSPtr NewBlockAccesser(const S3Info& s3_info) {
  blockaccess::BlockAccessOptions options;
  options.type = blockaccess::AccesserType::kS3;
  options.s3_options.s3_info = blockaccess::S3Info{
      .ak = s3_info.ak, .sk = s3_info.sk, .endpoint = s3_info.endpoint, .bucket_name = s3_info.bucket_name};

  auto block_accessor = blockaccess::NewShareBlockAccesser(options);
  auto status = block_accessor->Init();
  if (!status.IsOK()) {
    std::cerr << (fmt::format("init block accesser fail, error({}).", status.ToString()));
    return nullptr;
  }

  return block_accessor;
}

// output to S3
class S3Output : public Output {
 public:
  S3Output(const S3Info& s3_info) : s3_info_(s3_info) { data_.reserve(1024 * 1024); }

  static S3OutputUPtr New(const S3Info& s3_info) { return std::make_unique<S3Output>(s3_info); }

  bool Init() override {
    // Initialize S3 client and prepare to upload
    block_accessor_ = NewBlockAccesser(s3_info_);
    return block_accessor_ != nullptr;
  }

  void Append(const std::string& key, const std::string& value) override {
    LOG(INFO) << fmt::format("append key({}) value({}).", Helper::StringToHex(key), Helper::StringToHex(value));
    data_.append(EncodeKeyValue(key, value));
  }

  void Flush() override {
    auto status = block_accessor_->Put(s3_info_.object_name, data_);
    if (!status.ok()) {
      std::cerr << fmt::format("upload S3 fail, error({}).", status.ToString());
    }
  }

 private:
  S3Info s3_info_;
  std::string data_;
  blockaccess::BlockAccesserSPtr block_accessor_;
};

// input from file
class FileInput : public Input {
 public:
  explicit FileInput(const std::string& file_path) : file_path_(file_path) {}
  ~FileInput() override = default;

  static InputUPtr New(const std::string& file_path) { return std::make_unique<FileInput>(file_path); }

  bool Init() override {
    std::ifstream file_stream;
    file_stream.open(file_path_, std::ios::in);
    if (!file_stream.is_open()) {
      std::cerr << fmt::format("open file fail, {}", file_path_);
      return false;
    }

    data_ = std::string((std::istreambuf_iterator<char>(file_stream)), std::istreambuf_iterator<char>());

    file_stream.close();

    return true;
  }

  bool IsEof() const override { return offset_ >= data_.size(); }

  Status Read(std::string& key, std::string& value) override {
    if (IsEof()) return Status(pb::error::EINTERNAL, "end of file");

    if (!DecodeKeyValue(data_, offset_, key, value)) {
      return Status(pb::error::EINTERNAL, "decode key/value fail");
    }

    return Status::OK();
  }

 private:
  std::string file_path_;
  size_t offset_{0};
  std::string data_;
};

// input from S3
class S3Input : public Input {
 public:
  S3Input(const S3Info& s3_info) : s3_info_(s3_info) {}

  static InputUPtr New(const S3Info& s3_info) { return std::make_unique<S3Input>(s3_info); }

  bool Init() override {
    block_accessor_ = NewBlockAccesser(s3_info_);
    if (block_accessor_ == nullptr) {
      return false;
    }

    auto status = block_accessor_->Get(s3_info_.object_name, &data_);
    if (!status.ok()) {
      std::cerr << fmt::format("get S3 object fail, error({}).", status.ToString());
      return false;
    }

    return true;
  }

  bool IsEof() const override { return offset_ >= data_.size(); }

  Status Read(std::string& key, std::string& value) override {
    if (!DecodeKeyValue(data_, offset_, key, value)) {
      return Status(pb::error::EINTERNAL, "decode key/value fail");
    }

    LOG(INFO) << fmt::format("read key({}) value({}).", Helper::StringToHex(key), Helper::StringToHex(value));

    return Status::OK();
  }

 private:
  const S3Info s3_info_;
  size_t offset_{0};
  std::string data_;
  blockaccess::BlockAccesserSPtr block_accessor_;
};

Backup::~Backup() { Destroy(); }

bool Backup::Init(const std::string& coor_addr) {
  CHECK(!coor_addr.empty()) << "coor addr is empty.";

  auto kv_storage = DingodbStorage::New();
  CHECK(kv_storage != nullptr) << "new DingodbStorage fail.";

  std::string store_addrs = Helper::ParseStorageAddr(coor_addr);
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
    case Type::kStdout:
      output = StdOutput::New(options.is_binary);
      break;

    case Type::kFile:
      output = FileOutput::New(options.file_path);
      break;

    case Type::kS3:
      output = S3Output::New(options.s3_info);
      break;

    default:
      return Status(pb::error::EINTERNAL, "unsupported output type");
  }

  if (!output->Init()) {
    return Status(pb::error::EINTERNAL, "init output fail");
  }

  return BackupMetaTable(std::move(output));
}

Status Backup::BackupFsMetaTable(const Options& options, uint32_t fs_id) {
  if (fs_id == 0) {
    return Status(pb::error::EINTERNAL, "fs_id is zero");
  }

  OutputUPtr output;
  switch (options.type) {
    case Type::kStdout:
      output = StdOutput::New(options.is_binary);
      break;
    case Type::kFile:
      output = FileOutput::New(options.file_path);
      break;
    case Type::kS3:
      output = S3Output::New(options.s3_info);
      break;
    default:
      return Status(pb::error::EINTERNAL, "unsupported output type");
  }

  if (!output->Init()) {
    return Status(pb::error::EINTERNAL, "init output fail");
  }

  return BackupFsMetaTable(fs_id, std::move(output));
}

Status Backup::BackupMetaTable(OutputUPtr output) {
  CHECK(output != nullptr) << "output is nullptr.";

  uint64_t total_count = 0, lock_count = 0, auto_increment_id_count = 0;
  uint64_t mds_heartbeat_count = 0, client_heartbeat_count = 0, cache_member_heartbeat_count = 0, fs_count = 0,
           fs_quota_count = 0, fs_oplog_count = 0;

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
    } else if (MetaCodec::IsCacheMemberHeartbeatKey(key)) {
      ++cache_member_heartbeat_count;
    } else if (MetaCodec::IsFsKey(key)) {
      ++fs_count;
    } else if (MetaCodec::IsFsQuotaKey(key)) {
      ++fs_quota_count;
    } else if (MetaCodec::IsFsOpLogKey(key)) {
      ++fs_oplog_count;
    } else {
      std::cerr << fmt::format("unknown key({}).", Helper::StringToHex(key)) << '\n';
      return false;
    }

    ++total_count;

    return true;
  });

  auto status = operation_processor_->RunAlone(&operation);

  if (status.ok()) output->Flush();

  std::cout << fmt::format(
      "backup meta table done.\nsummary total_count({}) lock_count({}) auto_increment_id_count({}) "
      "mds_heartbeat_count({}) client_heartbeat_count({}) cache_member_heartbeat_count({}) fs_count({}) "
      "fs_quota_count({}) fs_oplog_count({}).\n",
      total_count, lock_count, auto_increment_id_count, mds_heartbeat_count, client_heartbeat_count,
      cache_member_heartbeat_count, fs_count, fs_quota_count, fs_oplog_count);

  return status;
}

Status Backup::BackupFsMetaTable(uint32_t fs_id, OutputUPtr output) {
  CHECK(output != nullptr) << "output is nullptr.";

  uint64_t total_count = 0, inode_count = 0, dentry_count = 0, chunk_count = 0;
  uint64_t dir_quota_count = 0, file_session_count = 0, del_slice_count = 0, del_file_count = 0;

  Trace trace;
  ScanFsMetaTableOperation operation(trace, fs_id, [&](const std::string& key, const std::string& value) -> bool {
    output->Append(key, value);

    if (MetaCodec::IsDirQuotaKey(key)) {
      ++dir_quota_count;
    } else if (MetaCodec::IsInodeKey(key)) {
      ++inode_count;
    } else if (MetaCodec::IsDentryKey(key)) {
      ++dentry_count;
    } else if (MetaCodec::IsChunkKey(key)) {
      ++chunk_count;
    } else if (MetaCodec::IsFileSessionKey(key)) {
      ++file_session_count;
    } else if (MetaCodec::IsDelSliceKey(key)) {
      ++del_slice_count;
    } else if (MetaCodec::IsDelFileKey(key)) {
      ++del_file_count;
    } else {
      std::cerr << fmt::format("unknown key({}).", Helper::StringToHex(key)) << '\n';
      return false;
    }

    ++total_count;

    return true;
  });

  auto status = operation_processor_->RunAlone(&operation);

  if (status.ok()) output->Flush();

  std::cout << fmt::format(
      "backup fsmeta table done.\n summary total_count({}) inode_count({}) dentry_count({}) chunk_count({}) "
      "dir_quota_count({}) "
      "file_session_count({}) del_slice_count({}) del_file_count({}).\n",
      total_count, inode_count, dentry_count, chunk_count, dir_quota_count, file_session_count, del_slice_count,
      del_file_count);

  return status;
}

bool Restore::Init(const std::string& coor_addr) {
  CHECK(!coor_addr.empty()) << "coor addr is empty.";

  auto kv_storage = DingodbStorage::New();
  CHECK(kv_storage != nullptr) << "new DingodbStorage fail.";

  std::string store_addrs = Helper::ParseStorageAddr(coor_addr);
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
    case Type::kFile:
      input = FileInput::New(options.file_path);
      break;

    case Type::kS3:
      input = S3Input::New(options.s3_info);
      break;

    default:
      return Status(pb::error::EINTERNAL, "unsupported input type");
  }

  if (!input->Init()) {
    return Status(pb::error::EINTERNAL, "init input fail");
  }

  return RestoreMetaTable(std::move(input));
}

Status Restore::RestoreFsMetaTable(const Options& options, uint32_t fs_id) {
  if (fs_id == 0) {
    return Status(pb::error::EINTERNAL, "fs_id is zero");
  }

  InputUPtr input;
  switch (options.type) {
    case Type::kFile:
      input = FileInput::New(options.file_path);
      break;

    case Type::kS3:
      input = S3Input::New(options.s3_info);
      break;

    default:
      return Status(pb::error::EINTERNAL, "unsupported input type");
  }

  if (!input->Init()) {
    return Status(pb::error::EINTERNAL, "init input fail");
  }

  return RestoreFsMetaTable(fs_id, std::move(input), options.is_force);
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

Status Restore::GetFsInfo(uint32_t fs_id, FsInfoEntry& fs_info) {
  Trace trace;
  ScanFsOperation operation(trace);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, fmt::format("scan fs info fail, status({})", status.error_str()));
  }

  auto& result = operation.GetResult();

  LOG(INFO) << fmt::format("fs_infoes size({}).", result.fs_infoes.size());

  for (const auto& fs : result.fs_infoes) {
    if (fs.fs_id() == fs_id) {
      fs_info = fs;
      return Status::OK();
    }
  }

  return Status(pb::error::ENOT_FOUND, "not found fs, please create it");
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
  uint64_t mds_heartbeat_count = 0, client_heartbeat_count = 0, cache_member_heartbeat_count = 0;
  uint64_t fs_count = 0, fs_quota_count = 0, fs_oplog_count = 0;

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
    } else if (MetaCodec::IsCacheMemberHeartbeatKey(kv.key)) {
      ++cache_member_heartbeat_count;
    } else if (MetaCodec::IsFsKey(kv.key)) {
      ++fs_count;
    } else if (MetaCodec::IsFsQuotaKey(kv.key)) {
      ++fs_quota_count;
    } else if (MetaCodec::IsFsOpLogKey(kv.key)) {
      ++fs_oplog_count;
    } else {
      status = Status(pb::error::EINTERNAL, fmt::format("unknown key({}).", Helper::StringToHex(kv.key)));
      break;
    }

    ++total_count;

    kvs.push_back(std::move(kv));

    if (kvs.size() > kImportKVBatchSize || (!kvs.empty() && input->IsEof())) {
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
      "restore meta table done.\nsummary total_count({}) lock_count({}) auto_increment_id_count({}) "
      "mds_heartbeat_count({}) client_heartbeat_count({}) cache_member_heartbeat_count({}) fs_count({}) "
      "fs_quota_count({}) fs_oplog_count({}) status({}).\n",
      total_count, lock_count, auto_increment_id_count, mds_heartbeat_count, client_heartbeat_count,
      cache_member_heartbeat_count, fs_count, fs_quota_count, fs_oplog_count, status.error_str());

  return Status::OK();
}

Status Restore::RestoreFsMetaTable(uint32_t fs_id, InputUPtr input, bool is_force) {
  // get fs info
  FsInfoEntry fs_info;
  auto status = GetFsInfo(fs_id, fs_info);
  if (!status.ok()) return status;
  if (fs_info.status() == pb::mds::FsStatus::RECYCLING) {
    return Status(pb::error::EINTERNAL, "fs status is recycling, can not restore fs meta table");
  }

  // check fs meta table exist
  status = IsExistFsMetaTable(fs_id);
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      return status;
    }

    // if it does not exist, create it
    status = CreateFsMetaTable(fs_id, fs_info.fs_name());
    if (!status.ok()) return status;
  } else {
    if (!is_force) {
      return Status(pb::error::EINTERNAL, "fs meta table exist, use --is_force to override it");
    }
  }

  // import fs meta to table

  uint64_t total_count = 0, dir_quota_count = 0, inode_count = 0, dentry_count = 0;
  uint64_t chunk_count = 0, file_session_count = 0, del_slice_count = 0, del_file_count = 0;

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
    } else if (MetaCodec::IsDentryKey(kv.key)) {
      ++dentry_count;
    } else if (MetaCodec::IsChunkKey(kv.key)) {
      ++chunk_count;
    } else if (MetaCodec::IsFileSessionKey(kv.key)) {
      ++file_session_count;
    } else if (MetaCodec::IsDelSliceKey(kv.key)) {
      ++del_slice_count;
    } else if (MetaCodec::IsDelFileKey(kv.key)) {
      ++del_file_count;
    } else {
      status = Status(pb::error::EINTERNAL, fmt::format("unknown key({}).", Helper::StringToHex(kv.key)));
      break;
    }

    ++total_count;

    kvs.push_back(std::move(kv));

    if (kvs.size() > kImportKVBatchSize || (!kvs.empty() && input->IsEof())) {
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
      "restore fsmeta table done.\n summary total_count({}) inode_count({}) dentry_count({}) chunk_count({}) "
      "dir_quota_count({}) file_session_count({}) del_slice_count({}) del_file_count({}) status({}).\n",
      total_count, inode_count, dentry_count, chunk_count, dir_quota_count, file_session_count, del_slice_count,
      del_file_count, status.error_str());

  return Status::OK();
}

bool BackupCommandRunner::Run(const Options& options, const std::string& coor_addr, const std::string& cmd) {
  using Helper = dingofs::mds::Helper;

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

  Backup::Options backup_options;
  if (options.output_type == "file") {
    backup_options.type = Type::kFile;
    backup_options.file_path = options.file_path;

  } else if (options.output_type == "s3") {
    backup_options.type = Type::kS3;
    backup_options.s3_info = options.s3_info;
    if (!options.s3_info.Validate()) {
      std::cout << fmt::format("s3 info is invalid, {}.", options.s3_info.ToString()) << '\n';
      return true;
    }

  } else if (options.output_type == "stdout") {
    backup_options.type = Type::kStdout;
    backup_options.is_binary = options.is_binary;

  } else {
    std::cout << "unknown output type: " << options.output_type << '\n';
    return true;
  }

  if (options.type == Helper::ToLowerCase("meta")) {
    auto status = backup.BackupMetaTable(backup_options);
    if (!status.ok()) {
      std::cerr << fmt::format("backup meta table fail, status({}).", status.error_str()) << '\n';
    }

  } else if (options.type == Helper::ToLowerCase("fsmeta")) {
    auto status = backup.BackupFsMetaTable(backup_options, options.fs_id);
    if (!status.ok()) {
      std::cerr << fmt::format("backup fsmeta table fail, status({}).", status.error_str()) << '\n';
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

  Restore restore;
  if (!restore.Init(coor_addr)) {
    std::cout << "init restore fail." << '\n';
    return true;
  }

  Restore::Options restore_options;
  restore_options.is_force = options.is_force;
  if (options.input_type == "file") {
    restore_options.type = Type::kFile;
    restore_options.file_path = options.file_path;
    if (options.file_path.empty() || !Helper::IsExistPath(options.file_path)) {
      std::cerr << fmt::format("file path is empty or not exist.") << '\n';
      return true;
    }

  } else if (options.input_type == "s3") {
    restore_options.type = Type::kS3;
    restore_options.s3_info = options.s3_info;
    if (!options.s3_info.Validate()) {
      std::cout << fmt::format("s3 info is invalid, {}.", options.s3_info.ToString()) << '\n';
      return true;
    }

  } else {
    std::cout << "unknown input type: " << options.input_type << '\n';
    return true;
  }

  if (options.type == Helper::ToLowerCase("meta")) {
    // auto status = restore.RestoreMetaTable(restore_options);
    // if (!status.ok()) {
    //   std::cerr << fmt::format("restore meta table fail, status({}).", status.error_str()) << '\n';
    // }
    std::cerr << "not support restore meta table." << '\n';

  } else if (options.type == Helper::ToLowerCase("fsmeta")) {
    auto status = restore.RestoreFsMetaTable(restore_options, options.fs_id);
    if (!status.ok()) {
      std::cerr << fmt::format("restore fsmeta table fail, status({}).", status.error_str()) << '\n';
    }

  } else {
    std::cout << "unknown type: " << options.type << '\n';
  }

  return true;
}

}  // namespace br
}  // namespace mds
}  // namespace dingofs