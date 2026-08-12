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

#include "tools/mds-cli/br.h"

#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
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

// v1 header (all integers are little-endian):
// magic[8] | version(u32) | header_size(u32) | data_type(u32) | fs_id(u32) |
// record_count(u64) | payload_size(u64) | payload_crc32(u32)
constexpr char kBackupMagic[] = {'D', 'I', 'N', 'G', 'O', 'B', 'R', '\0'};
constexpr uint32_t kBackupVersion = 1;
constexpr uint32_t kBackupHeaderSize = 44;

static void AppendUint32LE(uint32_t value, std::string& out) {
  for (int i = 0; i < 4; ++i) {
    out.push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

static void AppendUint64LE(uint64_t value, std::string& out) {
  for (int i = 0; i < 8; ++i) {
    out.push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

static bool ReadUint32LE(const std::string& data, size_t& offset,
                         uint32_t& value) {
  if (offset > data.size() || data.size() - offset < 4) return false;
  value = 0;
  for (int i = 0; i < 4; ++i) {
    value |= static_cast<uint32_t>(static_cast<unsigned char>(data[offset + i]))
             << (i * 8);
  }
  offset += 4;
  return true;
}

static bool ReadUint64LE(const std::string& data, size_t& offset,
                         uint64_t& value) {
  if (offset > data.size() || data.size() - offset < 8) return false;
  value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= static_cast<uint64_t>(static_cast<unsigned char>(data[offset + i]))
             << (i * 8);
  }
  offset += 8;
  return true;
}

static uint32_t UpdateCrc32(uint32_t crc, const char* data, size_t size) {
  for (size_t i = 0; i < size; ++i) {
    crc ^= static_cast<unsigned char>(data[i]);
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1U)));
    }
  }
  return crc;
}

static uint32_t CalcCrc32(const char* data, size_t size) {
  return UpdateCrc32(0xffffffffU, data, size) ^ 0xffffffffU;
}

static std::string EncodeBackupHeader(DataType data_type, uint32_t fs_id,
                                      uint64_t record_count,
                                      uint64_t payload_size,
                                      uint32_t payload_crc32) {
  std::string header(kBackupMagic, sizeof(kBackupMagic));
  AppendUint32LE(kBackupVersion, header);
  AppendUint32LE(kBackupHeaderSize, header);
  AppendUint32LE(static_cast<uint32_t>(data_type), header);
  AppendUint32LE(fs_id, header);
  AppendUint64LE(record_count, header);
  AppendUint64LE(payload_size, header);
  AppendUint32LE(payload_crc32, header);
  CHECK(header.size() == kBackupHeaderSize);
  return header;
}

// v1 record format: little-endian key length | key | little-endian value
// length | value. Legacy backups use host-endian lengths and have no header.
static std::string EncodeKeyValue(const std::string& key,
                                  const std::string& value) {
  std::string encoded;
  encoded.reserve(sizeof(uint32_t) * 2 + key.size() + value.size());
  AppendUint32LE(key.size(), encoded);
  encoded.append(key);
  AppendUint32LE(value.size(), encoded);
  encoded.append(value);
  return encoded;
}

static bool IsValidRecordSize(const std::string& key,
                              const std::string& value) {
  return key.size() <= std::numeric_limits<uint32_t>::max() &&
         value.size() <= std::numeric_limits<uint32_t>::max();
}

static bool DecodeKeyValue(const std::string& encoded, size_t& offset,
                           bool is_legacy, std::string& key,
                           std::string& value) {
  constexpr size_t kLengthSize = sizeof(uint32_t);
  if (offset > encoded.size() || encoded.size() - offset < kLengthSize) {
    return false;
  }

  uint32_t key_length = 0;
  if (is_legacy) {
    std::memcpy(&key_length, encoded.data() + offset, kLengthSize);
    offset += kLengthSize;
  } else if (!ReadUint32LE(encoded, offset, key_length)) {
    return false;
  }
  if (encoded.size() - offset < key_length) return false;
  key.assign(encoded.data() + offset, key_length);
  offset += key_length;

  uint32_t value_length = 0;
  if (is_legacy) {
    if (encoded.size() - offset < kLengthSize) return false;
    std::memcpy(&value_length, encoded.data() + offset, kLengthSize);
    offset += kLengthSize;
  } else if (!ReadUint32LE(encoded, offset, value_length)) {
    return false;
  }
  if (encoded.size() - offset < value_length) return false;
  value.assign(encoded.data() + offset, value_length);
  offset += value_length;
  return true;
}

struct BackupHeader {
  bool is_legacy{true};
  size_t payload_offset{0};
  uint64_t record_count{0};
};

static Status ParseAndValidateHeader(const std::string& data,
                                     DataType expected_type,
                                     uint32_t expected_fs_id,
                                     BackupHeader& header) {
  if (data.size() < sizeof(kBackupMagic) ||
      std::memcmp(data.data(), kBackupMagic, sizeof(kBackupMagic)) != 0) {
    header = BackupHeader{};
    return Status::OK();
  }

  size_t offset = sizeof(kBackupMagic);
  uint32_t version = 0, header_size = 0, data_type = 0, fs_id = 0;
  uint64_t record_count = 0, payload_size = 0;
  uint32_t payload_crc32 = 0;
  if (!ReadUint32LE(data, offset, version) ||
      !ReadUint32LE(data, offset, header_size) ||
      !ReadUint32LE(data, offset, data_type) ||
      !ReadUint32LE(data, offset, fs_id) ||
      !ReadUint64LE(data, offset, record_count) ||
      !ReadUint64LE(data, offset, payload_size) ||
      !ReadUint32LE(data, offset, payload_crc32)) {
    return Status(pb::error::EINTERNAL, "truncated backup header");
  }
  if (version != kBackupVersion) {
    return Status(pb::error::EINTERNAL,
                  fmt::format("unsupported backup version({})", version));
  }
  if (header_size != kBackupHeaderSize || header_size > data.size()) {
    return Status(pb::error::EINTERNAL,
                  fmt::format("invalid backup header size({})", header_size));
  }
  if (data_type != static_cast<uint32_t>(expected_type)) {
    return Status(pb::error::EINTERNAL, "backup data type mismatch");
  }
  if ((expected_type == DataType::kMeta && fs_id != 0) ||
      (expected_type == DataType::kFsMeta && fs_id != expected_fs_id)) {
    return Status(pb::error::EINTERNAL,
                  fmt::format("backup fs_id({}) does not match expected({})",
                              fs_id, expected_fs_id));
  }
  if (payload_size != data.size() - header_size) {
    return Status(pb::error::EINTERNAL, "backup payload size mismatch");
  }
  if (CalcCrc32(data.data() + header_size, payload_size) != payload_crc32) {
    return Status(pb::error::EINTERNAL, "backup checksum mismatch");
  }

  header = {.is_legacy = false,
            .payload_offset = header_size,
            .record_count = record_count};
  return Status::OK();
}

// output to standard output
class StdOutput : public Output {
 public:
  StdOutput(bool is_binary = false) : is_binary_(is_binary) {}

  static StdOutputUPtr New(bool is_binary = false) {
    return std::make_unique<StdOutput>(is_binary);
  }

  bool Init() override { return true; }

  Status Append(const std::string& key, const std::string& value) override {
    if (is_binary_) {
      std::cout << Helper::StringToHex(key) << ": "
                << Helper::StringToHex(value) << "\n";
    } else {
      auto desc = MetaCodec::ParseKey(key, value);
      std::cout << fmt::format("{}. key({}) value({})\n", ++count_, desc.first,
                               desc.second);
    }

    return std::cout.good() ? Status::OK()
                            : Status(pb::error::EINTERNAL, "write stdout fail");
  }

  Status Flush() override {
    std::cout.flush();
    return std::cout.good() ? Status::OK()
                            : Status(pb::error::EINTERNAL, "flush stdout fail");
  }

 private:
  uint32_t count_{0};  // count of key/value pairs
  bool is_binary_{false};
};

// output to a file
class FileOutput : public Output {
 public:
  FileOutput(const std::string& file_path, DataType data_type, uint32_t fs_id)
      : file_path_(file_path),
        temp_path_(fmt::format("{}.tmp.{}", file_path, getpid())),
        data_type_(data_type),
        fs_id_(fs_id) {}
  ~FileOutput() override {
    if (file_stream_.is_open()) file_stream_.close();
    if (!committed_) std::remove(temp_path_.c_str());
  }

  static FileOutputUPtr New(const std::string& file_path, DataType data_type,
                            uint32_t fs_id) {
    return std::make_unique<FileOutput>(file_path, data_type, fs_id);
  }

  bool Init() override {
    if (file_path_.empty()) return false;
    file_stream_.open(temp_path_, std::ios::out | std::ios::binary);
    if (!file_stream_.is_open()) return false;
    file_stream_ << std::string(kBackupHeaderSize, '\0');
    return file_stream_.good();
  }

  Status Append(const std::string& key, const std::string& value) override {
    if (!IsValidRecordSize(key, value)) {
      return Status(pb::error::EINTERNAL, "key/value is too large");
    }

    auto encoded = EncodeKeyValue(key, value);
    file_stream_ << encoded;
    crc32_ = UpdateCrc32(crc32_, encoded.data(), encoded.size());
    payload_size_ += encoded.size();
    ++record_count_;
    return file_stream_.good()
               ? Status::OK()
               : Status(pb::error::EINTERNAL, "write backup file fail");
  }

  Status Flush() override {
    if (record_count_ == 0) {
      return Status(pb::error::EINTERNAL,
                    "refuse to publish an empty backup file");
    }

    auto header = EncodeBackupHeader(data_type_, fs_id_, record_count_,
                                     payload_size_, crc32_ ^ 0xffffffffU);
    file_stream_.seekp(0);
    file_stream_ << header;
    file_stream_.flush();
    if (!file_stream_.good()) {
      return Status(pb::error::EINTERNAL, "flush backup file fail");
    }
    file_stream_.close();

    if (std::rename(temp_path_.c_str(), file_path_.c_str()) != 0) {
      return Status(pb::error::EINTERNAL, "publish backup file fail");
    }

    committed_ = true;
    return Status::OK();
  }

 private:
  std::string file_path_;
  std::string temp_path_;
  std::ofstream file_stream_;
  DataType data_type_;
  uint32_t fs_id_{0};
  uint64_t record_count_{0};
  uint64_t payload_size_{0};
  uint32_t crc32_{0xffffffffU};
  bool committed_{false};
};

static blockaccess::BlockAccesserSPtr NewBlockAccesser(const S3Info& s3_info) {
  blockaccess::BlockAccessOptions options;
  options.type = blockaccess::AccesserType::kS3;
  options.s3_options.s3_info =
      blockaccess::S3Info{.ak = s3_info.ak,
                          .sk = s3_info.sk,
                          .endpoint = s3_info.endpoint,
                          .bucket_name = s3_info.bucket_name};

  auto block_accessor = blockaccess::NewShareBlockAccesser(options);
  auto status = block_accessor->Init();
  if (!status.IsOK()) {
    std::cerr << (fmt::format("init block accesser fail, error({}).",
                              status.ToString()));
    return nullptr;
  }

  return block_accessor;
}

// output to S3
class S3Output : public Output {
 public:
  S3Output(const S3Info& s3_info, DataType data_type, uint32_t fs_id)
      : s3_info_(s3_info), data_type_(data_type), fs_id_(fs_id) {
    data_.reserve(1024 * 1024);
  }

  static S3OutputUPtr New(const S3Info& s3_info, DataType data_type,
                          uint32_t fs_id) {
    return std::make_unique<S3Output>(s3_info, data_type, fs_id);
  }

  bool Init() override {
    // Initialize S3 client and prepare to upload
    block_accessor_ = NewBlockAccesser(s3_info_);
    return block_accessor_ != nullptr;
  }

  Status Append(const std::string& key, const std::string& value) override {
    if (!IsValidRecordSize(key, value)) {
      return Status(pb::error::EINTERNAL, "key/value is too large");
    }

    LOG(INFO) << fmt::format("append key({}) value({}).",
                             Helper::StringToHex(key),
                             Helper::StringToHex(value));
    data_.append(EncodeKeyValue(key, value));
    ++record_count_;
    return Status::OK();
  }

  Status Flush() override {
    if (data_.empty()) {
      return Status(pb::error::EINTERNAL,
                    "refuse to upload an empty backup object");
    }

    auto header =
        EncodeBackupHeader(data_type_, fs_id_, record_count_, data_.size(),
                           CalcCrc32(data_.data(), data_.size()));
    data_.insert(0, header);
    auto payload =
        blockaccess::PutPayload::Build({{data_.data(), data_.size()}});
    auto status = block_accessor_->Put(s3_info_.object_name, payload);
    if (!status.ok()) {
      return Status(
          pb::error::EINTERNAL,
          fmt::format("upload S3 fail, error({})", status.ToString()));
    }

    return Status::OK();
  }

 private:
  S3Info s3_info_;
  std::string data_;
  blockaccess::BlockAccesserSPtr block_accessor_;
  DataType data_type_;
  uint32_t fs_id_{0};
  uint64_t record_count_{0};
};

// input from file
class FileInput : public Input {
 public:
  explicit FileInput(const std::string& file_path) : file_path_(file_path) {}
  ~FileInput() override = default;

  static InputUPtr New(const std::string& file_path) {
    return std::make_unique<FileInput>(file_path);
  }

  bool Init() override {
    std::ifstream file_stream;
    file_stream.open(file_path_, std::ios::in | std::ios::binary);
    if (!file_stream.is_open()) {
      std::cerr << fmt::format("open file fail, {}", file_path_);
      return false;
    }

    data_ = std::string((std::istreambuf_iterator<char>(file_stream)),
                        std::istreambuf_iterator<char>());

    file_stream.close();

    return true;
  }

  Status ValidateHeader(DataType expected_type,
                        uint32_t expected_fs_id) override {
    auto status =
        ParseAndValidateHeader(data_, expected_type, expected_fs_id, header_);
    if (status.ok()) offset_ = header_.payload_offset;
    return status;
  }

  bool IsEof() const override { return offset_ >= data_.size(); }

  void Reset() override { offset_ = header_.payload_offset; }

  Status Read(std::string& key, std::string& value) override {
    if (IsEof()) return Status(pb::error::EINTERNAL, "end of file");

    if (!DecodeKeyValue(data_, offset_, header_.is_legacy, key, value)) {
      return Status(pb::error::EINTERNAL, "decode key/value fail");
    }

    return Status::OK();
  }

  Status VerifyRecordCount(uint64_t actual_count) const override {
    if (!header_.is_legacy && actual_count != header_.record_count) {
      return Status(pb::error::EINTERNAL,
                    fmt::format("backup record count mismatch, expected({}) "
                                "actual({})",
                                header_.record_count, actual_count));
    }
    return Status::OK();
  }

 private:
  std::string file_path_;
  size_t offset_{0};
  std::string data_;
  BackupHeader header_;
};

// input from S3
class S3Input : public Input {
 public:
  S3Input(const S3Info& s3_info) : s3_info_(s3_info) {}

  static InputUPtr New(const S3Info& s3_info) {
    return std::make_unique<S3Input>(s3_info);
  }

  bool Init() override {
    block_accessor_ = NewBlockAccesser(s3_info_);
    if (block_accessor_ == nullptr) {
      return false;
    }

    auto status = block_accessor_->Get(s3_info_.object_name, &data_);
    if (!status.ok()) {
      std::cerr << fmt::format("get S3 object fail, error({}).",
                               status.ToString());
      return false;
    }

    return true;
  }

  Status ValidateHeader(DataType expected_type,
                        uint32_t expected_fs_id) override {
    auto status =
        ParseAndValidateHeader(data_, expected_type, expected_fs_id, header_);
    if (status.ok()) offset_ = header_.payload_offset;
    return status;
  }

  bool IsEof() const override { return offset_ >= data_.size(); }

  void Reset() override { offset_ = header_.payload_offset; }

  Status Read(std::string& key, std::string& value) override {
    if (IsEof()) return Status(pb::error::EINTERNAL, "end of input");

    if (!DecodeKeyValue(data_, offset_, header_.is_legacy, key, value)) {
      return Status(pb::error::EINTERNAL, "decode key/value fail");
    }

    LOG(INFO) << fmt::format("read key({}) value({}).",
                             Helper::StringToHex(key),
                             Helper::StringToHex(value));

    return Status::OK();
  }

  Status VerifyRecordCount(uint64_t actual_count) const override {
    if (!header_.is_legacy && actual_count != header_.record_count) {
      return Status(pb::error::EINTERNAL,
                    fmt::format("backup record count mismatch, expected({}) "
                                "actual({})",
                                header_.record_count, actual_count));
    }
    return Status::OK();
  }

 private:
  const S3Info s3_info_;
  size_t offset_{0};
  std::string data_;
  blockaccess::BlockAccesserSPtr block_accessor_;
  BackupHeader header_;
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

void Backup::Destroy() {
  if (operation_processor_) {
    operation_processor_->Destroy();
    operation_processor_.reset();
  }
}

Status Backup::BackupMetaTable(const Options& options) {
  OutputUPtr output;
  switch (options.type) {
    case Type::kStdout:
      output = StdOutput::New(options.is_binary);
      break;

    case Type::kFile:
      output = FileOutput::New(options.file_path, DataType::kMeta, 0);
      break;

    case Type::kS3:
      output = S3Output::New(options.s3_info, DataType::kMeta, 0);
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
      output = FileOutput::New(options.file_path, DataType::kFsMeta, fs_id);
      break;
    case Type::kS3:
      output = S3Output::New(options.s3_info, DataType::kFsMeta, fs_id);
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
  uint64_t mds_heartbeat_count = 0, client_heartbeat_count = 0,
           cache_member_heartbeat_count = 0, fs_count = 0, fs_quota_count = 0,
           fs_oplog_count = 0, slice_ref_count = 0;
  Status output_status;

  Trace trace;
  ScanMetaTableOperation operation(
      trace, [&](const std::string& key, const std::string& value) -> bool {
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
        } else if (MetaCodec::IsSliceRefKey(key)) {
          ++slice_ref_count;
        } else {
          output_status =
              Status(pb::error::EINTERNAL,
                     fmt::format("unknown key({})", Helper::StringToHex(key)));
          return false;
        }

        output_status = output->Append(key, value);
        if (!output_status.ok()) return false;

        ++total_count;
        return true;
      });

  auto status = operation_processor_->RunAlone(&operation);
  if (status.ok() && !output_status.ok()) status = output_status;
  if (status.ok()) status = output->Flush();

  std::cout << fmt::format(
      "backup meta table done.\nsummary total_count({}) lock_count({}) "
      "auto_increment_id_count({}) "
      "mds_heartbeat_count({}) client_heartbeat_count({}) "
      "cache_member_heartbeat_count({}) fs_count({}) "
      "fs_quota_count({}) fs_oplog_count({}) slice_ref_count({}) "
      "status({}).\n",
      total_count, lock_count, auto_increment_id_count, mds_heartbeat_count,
      client_heartbeat_count, cache_member_heartbeat_count, fs_count,
      fs_quota_count, fs_oplog_count, slice_ref_count, status.error_str());

  return status;
}

Status Backup::BackupFsMetaTable(uint32_t fs_id, OutputUPtr output) {
  CHECK(output != nullptr) << "output is nullptr.";

  uint64_t total_count = 0, inode_count = 0, dentry_count = 0, chunk_count = 0,
           dir_mutation_count = 0;
  uint64_t dir_quota_count = 0, dir_stat_count = 0, file_session_count = 0,
           del_slice_count = 0, del_file_count = 0;
  Status output_status;

  Trace trace;
  ScanFsMetaTableOperation operation(
      trace, fs_id,
      [&](const std::string& key, const std::string& value) -> bool {
        if (MetaCodec::IsDirQuotaKey(key)) {
          ++dir_quota_count;
        } else if (MetaCodec::IsDirStatKey(key)) {
          ++dir_stat_count;
        } else if (MetaCodec::IsInodeKey(key)) {
          ++inode_count;
        } else if (MetaCodec::IsDirInodeMutationKey(key)) {
          ++dir_mutation_count;
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
          output_status =
              Status(pb::error::EINTERNAL,
                     fmt::format("unknown key({})", Helper::StringToHex(key)));
          return false;
        }

        output_status = output->Append(key, value);
        if (!output_status.ok()) return false;

        ++total_count;
        return true;
      });

  auto status = operation_processor_->RunAlone(&operation);
  if (status.ok() && !output_status.ok()) status = output_status;
  if (status.ok()) status = output->Flush();

  std::cout << fmt::format(
      "backup fsmeta table done.\n summary total_count({}) inode_count({}) "
      "dentry_count({}) chunk_count({}) dir_mutation_count({}) "
      "dir_quota_count({}) dir_stat_count({}) "
      "file_session_count({}) del_slice_count({}) del_file_count({}) "
      "status({}).\n",
      total_count, inode_count, dentry_count, chunk_count, dir_mutation_count,
      dir_quota_count, dir_stat_count, file_session_count, del_slice_count,
      del_file_count, status.error_str());

  return status;
}

Restore::~Restore() { Destroy(); }

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
    operation_processor_.reset();
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
  auto status = input->ValidateHeader(DataType::kMeta, 0);
  if (!status.ok()) return status;

  return RestoreMetaTable(std::move(input), options.is_force);
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
  auto status = input->ValidateHeader(DataType::kFsMeta, fs_id);
  if (!status.ok()) return status;

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
  auto status =
      operation_processor_->CreateTable(kMetaTableName, range, table_id);
  if (!status.ok()) {
    return Status(
        pb::error::EINTERNAL,
        fmt::format("create meta table fail, {}", status.error_str()));
  }

  return Status::OK();
}

Status Restore::CreateFsMetaTable(uint32_t fs_id, const std::string& fs_name) {
  int64_t table_id = 0;
  auto range = MetaCodec::GetFsMetaTableRange(fs_id);
  auto status = operation_processor_->CreateTable(GenFsMetaTableName(fs_name),
                                                  range, table_id);
  if (!status.ok()) {
    return Status(
        pb::error::EINTERNAL,
        fmt::format("create fs meta table fail, {}", status.error_str()));
  }

  return Status::OK();
}

Status Restore::GetFsInfo(uint32_t fs_id, FsInfoEntry& fs_info) {
  Trace trace;
  ScanFsOperation operation(trace);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return Status(
        pb::error::EINTERNAL,
        fmt::format("scan fs info fail, status({})", status.error_str()));
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

namespace {

struct MetaTableStats {
  uint64_t total_count{0};
  uint64_t lock_count{0};
  uint64_t auto_increment_id_count{0};
  uint64_t mds_heartbeat_count{0};
  uint64_t client_heartbeat_count{0};
  uint64_t cache_member_heartbeat_count{0};
  uint64_t fs_count{0};
  uint64_t fs_quota_count{0};
  uint64_t fs_oplog_count{0};
  uint64_t slice_ref_count{0};
};

struct FsMetaTableStats {
  uint64_t total_count{0};
  uint64_t dir_quota_count{0};
  uint64_t dir_stat_count{0};
  uint64_t inode_count{0};
  uint64_t dir_mutation_count{0};
  uint64_t dentry_count{0};
  uint64_t chunk_count{0};
  uint64_t file_session_count{0};
  uint64_t del_slice_count{0};
  uint64_t del_file_count{0};
};

bool IsKeyInRange(const std::string& key, const Range& range) {
  return key >= range.start && key < range.end;
}

template <typename T>
bool IsValidProtoValue(const std::string& value) {
  T message;
  return message.ParseFromString(value);
}

Status InvalidValueStatus(const std::string& key) {
  return Status(pb::error::EINTERNAL, fmt::format("invalid value for key({})",
                                                  Helper::StringToHex(key)));
}

Status ValidateMetaTableInput(Input* input, MetaTableStats& stats) {
  const auto range = MetaCodec::GetMetaTableRange();
  while (!input->IsEof()) {
    std::string key;
    std::string value;
    auto status = input->Read(key, value);
    if (!status.ok()) return status;
    if (key.empty() || value.empty()) {
      return Status(pb::error::EINTERNAL,
                    "backup contains an empty key or value");
    }
    if (!IsKeyInRange(key, range)) {
      return Status(pb::error::EINTERNAL,
                    fmt::format("key({}) is outside meta table range",
                                Helper::StringToHex(key)));
    }

    bool is_valid_value = false;
    if (MetaCodec::IsLockKey(key)) {
      ++stats.lock_count;
      is_valid_value = value.size() == sizeof(int64_t) + sizeof(uint64_t) * 2;
    } else if (MetaCodec::IsAutoIncrementIDKey(key)) {
      ++stats.auto_increment_id_count;
      is_valid_value = value.size() == sizeof(uint64_t);
    } else if (MetaCodec::IsMdsHeartbeatKey(key)) {
      ++stats.mds_heartbeat_count;
      is_valid_value = IsValidProtoValue<MdsEntry>(value);
    } else if (MetaCodec::IsClientHeartbeatKey(key)) {
      ++stats.client_heartbeat_count;
      is_valid_value = IsValidProtoValue<ClientEntry>(value);
    } else if (MetaCodec::IsCacheMemberHeartbeatKey(key)) {
      ++stats.cache_member_heartbeat_count;
      is_valid_value = IsValidProtoValue<CacheMemberEntry>(value);
    } else if (MetaCodec::IsFsKey(key)) {
      ++stats.fs_count;
      is_valid_value = IsValidProtoValue<FsInfoEntry>(value);
    } else if (MetaCodec::IsFsQuotaKey(key)) {
      ++stats.fs_quota_count;
      is_valid_value = IsValidProtoValue<QuotaEntry>(value);
    } else if (MetaCodec::IsFsOpLogKey(key)) {
      ++stats.fs_oplog_count;
      is_valid_value = IsValidProtoValue<FsOpLog>(value);
    } else if (MetaCodec::IsSliceRefKey(key)) {
      ++stats.slice_ref_count;
      is_valid_value = IsValidProtoValue<SliceRefEntry>(value);
    } else {
      return Status(pb::error::EINTERNAL,
                    fmt::format("unknown key({})", Helper::StringToHex(key)));
    }
    if (!is_valid_value) return InvalidValueStatus(key);

    ++stats.total_count;
  }

  auto status = input->VerifyRecordCount(stats.total_count);
  input->Reset();
  if (!status.ok()) return status;
  return stats.total_count == 0
             ? Status(pb::error::EINTERNAL, "backup is empty")
             : Status::OK();
}

Status ValidateFsMetaTableInput(uint32_t fs_id, Input* input,
                                FsMetaTableStats& stats) {
  const auto range = MetaCodec::GetFsMetaTableRange(fs_id);
  while (!input->IsEof()) {
    std::string key;
    std::string value;
    auto status = input->Read(key, value);
    if (!status.ok()) return status;
    if (key.empty() || value.empty()) {
      return Status(pb::error::EINTERNAL,
                    "backup contains an empty key or value");
    }
    if (!IsKeyInRange(key, range)) {
      return Status(pb::error::EINTERNAL,
                    fmt::format("key({}) does not belong to fs_id({})",
                                Helper::StringToHex(key), fs_id));
    }

    bool is_valid_value = false;
    if (MetaCodec::IsDirQuotaKey(key)) {
      ++stats.dir_quota_count;
      is_valid_value = IsValidProtoValue<QuotaEntry>(value);
    } else if (MetaCodec::IsDirStatKey(key)) {
      ++stats.dir_stat_count;
      is_valid_value = IsValidProtoValue<DirStatEntry>(value);
    } else if (MetaCodec::IsInodeKey(key)) {
      ++stats.inode_count;
      is_valid_value = IsValidProtoValue<AttrEntry>(value);
    } else if (MetaCodec::IsDirInodeMutationKey(key)) {
      ++stats.dir_mutation_count;
      is_valid_value = IsValidProtoValue<AttrMutationEntry>(value);
    } else if (MetaCodec::IsDentryKey(key)) {
      ++stats.dentry_count;
      is_valid_value = IsValidProtoValue<DentryEntry>(value);
    } else if (MetaCodec::IsChunkKey(key)) {
      ++stats.chunk_count;
      is_valid_value = IsValidProtoValue<ChunkEntry>(value);
    } else if (MetaCodec::IsFileSessionKey(key)) {
      ++stats.file_session_count;
      is_valid_value = IsValidProtoValue<FileSessionEntry>(value);
    } else if (MetaCodec::IsDelSliceKey(key)) {
      ++stats.del_slice_count;
      is_valid_value = IsValidProtoValue<TrashSliceList>(value);
    } else if (MetaCodec::IsDelFileKey(key)) {
      ++stats.del_file_count;
      is_valid_value = IsValidProtoValue<AttrEntry>(value);
    } else {
      return Status(pb::error::EINTERNAL,
                    fmt::format("unknown key({})", Helper::StringToHex(key)));
    }
    if (!is_valid_value) return InvalidValueStatus(key);

    ++stats.total_count;
  }

  auto status = input->VerifyRecordCount(stats.total_count);
  input->Reset();
  if (!status.ok()) return status;
  return stats.total_count == 0
             ? Status(pb::error::EINTERNAL, "backup is empty")
             : Status::OK();
}

Status ImportInput(OperationProcessorSPtr operation_processor, Input* input) {
  std::vector<KeyValue> kvs;
  kvs.reserve(kImportKVBatchSize);

  while (!input->IsEof()) {
    KeyValue kv;
    auto status = input->Read(kv.key, kv.value);
    if (!status.ok()) return status;
    kvs.push_back(std::move(kv));

    if (kvs.size() >= kImportKVBatchSize || input->IsEof()) {
      Trace trace;
      ImportKVOperation operation(trace, std::move(kvs));
      status = operation_processor->RunAlone(&operation);
      if (!status.ok()) return status;
      kvs.clear();
      kvs.reserve(kImportKVBatchSize);
    }
  }

  return Status::OK();
}

}  // namespace

Status Restore::RestoreMetaTable(InputUPtr input, bool is_force) {
  CHECK(input != nullptr) << "input is nullptr.";

  MetaTableStats stats;
  auto status = ValidateMetaTableInput(input.get(), stats);
  if (!status.ok()) return status;

  const auto range = MetaCodec::GetMetaTableRange();
  status = IsExistMetaTable();
  if (status.ok()) {
    if (!is_force) {
      return Status(pb::error::EINTERNAL,
                    "meta table exists, use --is_force to replace it");
    }
    status = operation_processor_->GetKVStorage()->DropTable(range);
    if (!status.ok()) return status;
  } else if (status.error_code() != pb::error::ENOT_FOUND) {
    return status;
  }

  status = CreateMetaTable();
  if (!status.ok()) return status;

  status = ImportInput(operation_processor_, input.get());
  std::cout << fmt::format(
      "restore meta table done.\nsummary total_count({}) lock_count({}) "
      "auto_increment_id_count({}) mds_heartbeat_count({}) "
      "client_heartbeat_count({}) cache_member_heartbeat_count({}) "
      "fs_count({}) fs_quota_count({}) fs_oplog_count({}) "
      "slice_ref_count({}) status({}).\n",
      stats.total_count, stats.lock_count, stats.auto_increment_id_count,
      stats.mds_heartbeat_count, stats.client_heartbeat_count,
      stats.cache_member_heartbeat_count, stats.fs_count, stats.fs_quota_count,
      stats.fs_oplog_count, stats.slice_ref_count, status.error_str());

  return status;
}

Status Restore::RestoreFsMetaTable(uint32_t fs_id, InputUPtr input,
                                   bool is_force) {
  CHECK(input != nullptr) << "input is nullptr.";

  FsMetaTableStats stats;
  auto status = ValidateFsMetaTableInput(fs_id, input.get(), stats);
  if (!status.ok()) return status;

  FsInfoEntry fs_info;
  status = GetFsInfo(fs_id, fs_info);
  if (!status.ok()) return status;
  if (fs_info.status() == pb::mds::FsStatus::RECYCLING) {
    return Status(pb::error::EINTERNAL,
                  "fs status is recycling, can not restore fs meta table");
  }

  const auto range = MetaCodec::GetFsMetaTableRange(fs_id);
  status = IsExistFsMetaTable(fs_id);
  if (status.ok()) {
    if (!is_force) {
      return Status(pb::error::EINTERNAL,
                    "fs meta table exists, use --is_force to replace it");
    }
    status = operation_processor_->GetKVStorage()->DropTable(range);
    if (!status.ok()) return status;
  } else if (status.error_code() != pb::error::ENOT_FOUND) {
    return status;
  }

  status = CreateFsMetaTable(fs_id, fs_info.fs_name());
  if (!status.ok()) return status;

  status = ImportInput(operation_processor_, input.get());
  std::cout << fmt::format(
      "restore fsmeta table done.\n summary total_count({}) inode_count({}) "
      "dir_mutation_count({}) dentry_count({}) chunk_count({}) "
      "dir_quota_count({}) dir_stat_count({}) file_session_count({}) "
      "del_slice_count({}) del_file_count({}) status({}).\n",
      stats.total_count, stats.inode_count, stats.dir_mutation_count,
      stats.dentry_count, stats.chunk_count, stats.dir_quota_count,
      stats.dir_stat_count, stats.file_session_count, stats.del_slice_count,
      stats.del_file_count, status.error_str());

  return status;
}

bool BackupCommandRunner::Run(const Options& options,
                              const std::string& coor_addr,
                              const std::string& cmd) {
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
      std::cout << fmt::format("s3 info is invalid, {}.",
                               options.s3_info.ToString())
                << '\n';
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
      std::cerr << fmt::format("backup meta table fail, status({}).",
                               status.error_str())
                << '\n';
    }

  } else if (options.type == Helper::ToLowerCase("fsmeta")) {
    auto status = backup.BackupFsMetaTable(backup_options, options.fs_id);
    if (!status.ok()) {
      std::cerr << fmt::format("backup fsmeta table fail, status({}).",
                               status.error_str())
                << '\n';
    }

  } else {
    std::cout << "unknown type: " << options.type << '\n';
  }

  return true;
}

bool RestoreCommandRunner::Run(const Options& options,
                               const std::string& coor_addr,  // NOLINT
                               const std::string& cmd) {      // NOLINT
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
      std::cout << fmt::format("s3 info is invalid, {}.",
                               options.s3_info.ToString())
                << '\n';
      return true;
    }

  } else {
    std::cout << "unknown input type: " << options.input_type << '\n';
    return true;
  }

  if (options.type == Helper::ToLowerCase("meta")) {
    std::cerr << "not support restore meta table." << '\n';

  } else if (options.type == Helper::ToLowerCase("fsmeta")) {
    auto status = restore.RestoreFsMetaTable(restore_options, options.fs_id);
    if (!status.ok()) {
      std::cerr << fmt::format("restore fsmeta table fail, status({}).",
                               status.error_str())
                << '\n';
    }

  } else {
    std::cout << "unknown type: " << options.type << '\n';
  }

  return true;
}

}  // namespace br
}  // namespace mds
}  // namespace dingofs
