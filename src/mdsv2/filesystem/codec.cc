// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mdsv2/filesystem/codec.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "fmt/core.h"
#include "glog/logging.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/serial_helper.h"

namespace dingofs {
namespace mdsv2 {

// all key prefix
static const char* const kPrefix = "xDINGOFS:";
static const size_t kPrefixSize = std::char_traits<char>::length(kPrefix);

static const char kDelimiter = ':';

enum KeyType : unsigned char {
  kTypeFS = 1,
  kTypeDentryOrDir = 2,
  kTypeFile = 3,
  kTypeFsQuota = 4,
  kTypeDirQuota = 5,
};

void MetaDataCodec::GetFsTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFS);
  end_key = kPrefix;
  end_key.push_back(kTypeFS + 1);
}

void MetaDataCodec::GetDentryTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeDentryOrDir);
  start_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeDentryOrDir);
  end_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaDataCodec::GetFileInodeTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFile);
  start_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeFile);
  end_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaDataCodec::GetFsQuotaRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFsQuota);
  end_key = kPrefix;
  end_key.push_back(kTypeFsQuota + 1);
}

void MetaDataCodec::GetDirQuotaRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeDirQuota);
  start_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeDirQuota);
  end_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

// format: [$prefix, $type, $kDelimiter, $name]
// size: >= 1+1+1 = 3
std::string MetaDataCodec::EncodeFSKey(const std::string& name) {
  CHECK(!name.empty()) << fmt::format("FS name is empty.", name);

  std::string key;
  key.reserve(kPrefixSize + 2 + name.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeFS);
  key.push_back(kDelimiter);
  key.append(name);

  return key;
}

void MetaDataCodec::DecodeFSKey(const std::string& key, std::string& name) {
  CHECK(key.size() > (kPrefixSize + 2)) << fmt::format("Key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeFS) << "Key type is invalid.";
  CHECK(key.at(kPrefixSize + 1) == kDelimiter) << "Delimiter is invalid.";

  name = key.substr(kPrefixSize + 2);
}

std::string MetaDataCodec::EncodeFSValue(const pb::mdsv2::FsInfo& fs_info) { return fs_info.SerializeAsString(); }

pb::mdsv2::FsInfo MetaDataCodec::DecodeFSValue(const std::string& value) {
  pb::mdsv2::FsInfo fs_info;
  CHECK(fs_info.ParseFromString(value)) << "Parse fs info fail.";
  return fs_info;
}

// format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino, $kDelimiter, $name]
/// size: >= kPrefixSize+1+1+4+1+8+1+1
std::string MetaDataCodec::EncodeDentryKey(uint32_t fs_id, uint64_t ino, const std::string& name) {
  CHECK(fs_id > 0) << fmt::format("Invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 16 + name.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeDentryOrDir);
  key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kDelimiter);
  SerialHelper::WriteLong(ino, key);
  key.push_back(kDelimiter);
  key.append(name);

  return key;
}

void MetaDataCodec::DecodeDentryKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, std::string& name) {
  CHECK(key.size() >= (kPrefixSize + 16))
      << fmt::format("Key({}) length({}) is invalid.", Helper::StringToHex(key), key.size());
  CHECK(key.at(kPrefixSize) == KeyType::kTypeDentryOrDir) << "Key type is invalid.";
  CHECK(key.at(kPrefixSize + 1) == kDelimiter) << "Delimiter is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 2, kPrefixSize + 6));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 7, kPrefixSize + 15));
  name = key.substr(kPrefixSize + 16);
}

std::string MetaDataCodec::EncodeDentryValue(const pb::mdsv2::Dentry& dentry) { return dentry.SerializeAsString(); }

pb::mdsv2::Dentry MetaDataCodec::DecodeDentryValue(const std::string& value) {
  pb::mdsv2::Dentry dentry;
  CHECK(dentry.ParseFromString(value)) << "Parse dentry fail.";
  return dentry;
}

void MetaDataCodec::EncodeDentryRange(uint32_t fs_id, uint64_t ino, std::string& start_key, std::string& end_key) {
  CHECK(fs_id > 0) << fmt::format("Invalid fs_id {}.", fs_id);
  CHECK(ino > 0) << fmt::format("Invalid ino {}.", ino);

  start_key.reserve(kPrefixSize + 16);

  start_key.append(kPrefix);
  start_key.push_back(KeyType::kTypeDentryOrDir);
  start_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, start_key);
  start_key.push_back(kDelimiter);
  SerialHelper::WriteLong(ino, start_key);

  end_key.reserve(kPrefixSize + 16);

  end_key.append(kPrefix);
  end_key.push_back(KeyType::kTypeDentryOrDir);
  end_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, end_key);
  end_key.push_back(kDelimiter);
  SerialHelper::WriteLong(ino + 1, end_key);
}

uint32_t MetaDataCodec::InodeKeyLength() { return kPrefixSize + 15; }

static std::string EncodeInodeKeyImpl(int fs_id, uint64_t ino, KeyType type) {
  CHECK(fs_id > 0) << fmt::format("Invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 15);

  key.append(kPrefix);
  key.push_back(type);
  key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kDelimiter);
  SerialHelper::WriteLong(ino, key);

  return key;
}

std::string MetaDataCodec::EncodeInodeKey(uint32_t fs_id, uint64_t ino) {
  return EncodeInodeKeyImpl(fs_id, ino, ino % 2 == 0 ? KeyType::kTypeDentryOrDir : KeyType::kTypeFile);
}

void MetaDataCodec::DecodeInodeKey(const std::string& key, uint32_t& fs_id, uint64_t& ino) {
  CHECK(key.size() == (kPrefixSize + 15)) << fmt::format("Key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize + 1) == kDelimiter) << "Delimiter is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 2, kPrefixSize + 6));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 7, kPrefixSize + 15));
}

std::string MetaDataCodec::EncodeInodeValue(const pb::mdsv2::Inode& inode) { return inode.SerializeAsString(); }

pb::mdsv2::Inode MetaDataCodec::DecodeInodeValue(const std::string& value) {
  pb::mdsv2::Inode inode;
  CHECK(inode.ParseFromString(value)) << "Parse inode fail.";
  return std::move(inode);
}

// fs format: [$prefix, $type, $kDelimiter, $fs_id]
std::string MetaDataCodec::EncodeFsQuotaKey(uint32_t fs_id) {
  CHECK(fs_id > 0) << fmt::format("Invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 15);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeFsQuota);
  key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, key);

  return key;
}

void MetaDataCodec::DecodeFsQuotaKey(const std::string& key, uint32_t& fs_id) {
  CHECK(key.size() == (kPrefixSize + 15)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeFsQuota) << "key type is invalid.";
  CHECK(key.at(kPrefixSize + 1) == kDelimiter) << "delimiter is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 2, kPrefixSize + 6));
}

std::string MetaDataCodec::EncodeFsQuotaValue(const pb::mdsv2::Quota& quota) { return quota.SerializeAsString(); }

pb::mdsv2::Quota MetaDataCodec::DecodeFsQuotaValue(const std::string& value) {
  pb::mdsv2::Quota quota;
  CHECK(quota.ParseFromString(value)) << "parse quota fail.";
  return quota;
}

// dir format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino]
std::string MetaDataCodec::EncodeDirQuotaKey(uint32_t fs_id, uint64_t ino) {
  CHECK(fs_id > 0) << fmt::format("Invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 15);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeDirQuota);
  key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kDelimiter);
  SerialHelper::WriteLong(ino, key);

  return key;
}

void MetaDataCodec::DecodeDirQuotaKey(const std::string& key, uint32_t& fs_id, uint64_t& ino) {
  CHECK(key.size() == (kPrefixSize + 15)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeDirQuota) << "key type is invalid.";
  CHECK(key.at(kPrefixSize + 1) == kDelimiter) << "delimiter is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 2, kPrefixSize + 6));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 7, kPrefixSize + 15));
}

std::string MetaDataCodec::EncodeDirQuotaValue(const pb::mdsv2::Quota& quota) { return quota.SerializeAsString(); }

pb::mdsv2::Quota MetaDataCodec::DecodeDirQuotaValue(const std::string& value) {
  pb::mdsv2::Quota quota;
  CHECK(quota.ParseFromString(value)) << "parse quota fail.";
  return quota;
}

}  // namespace mdsv2
}  // namespace dingofs
