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
#include <string>
#include <string_view>

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
  kTypeDentry = 2,
  kTypeFileInode = 3,
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
  start_key.push_back(kTypeDentry);
  start_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeDentry);
  end_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaDataCodec::GetFileInodeTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFileInode);
  start_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeFileInode);
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
/// size: >= 1+1+4+1+8+1+1 = 17
std::string MetaDataCodec::EncodeDentryKey(int fs_id, uint64_t ino, const std::string& name) {
  CHECK(fs_id > 0) << fmt::format("Invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 16 + name.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeDentry);
  key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kDelimiter);
  SerialHelper::WriteLong(ino, key);
  key.push_back(kDelimiter);
  key.append(name);

  return key;
}

void MetaDataCodec::DecodeDentryKey(const std::string& key, int& fs_id, uint64_t& ino, std::string& name) {
  CHECK(key.size() >= (kPrefixSize + 17)) << fmt::format("Key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeDentry) << "Key type is invalid.";
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

void MetaDataCodec::EncodeDentryRange(int fs_id, uint64_t ino, std::string& start_key, std::string& end_key) {
  CHECK(fs_id > 0) << fmt::format("Invalid fs_id {}.", fs_id);
  CHECK(ino > 0) << fmt::format("Invalid ino {}.", ino);

  start_key.reserve(kPrefixSize + 16);

  start_key.append(kPrefix);
  start_key.push_back(KeyType::kTypeDentry);
  start_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, start_key);
  start_key.push_back(kDelimiter);
  SerialHelper::WriteLong(ino, start_key);

  end_key.reserve(kPrefixSize + 16);

  end_key.append(kPrefix);
  end_key.push_back(KeyType::kTypeDentry);
  end_key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, end_key);
  end_key.push_back(kDelimiter);
  SerialHelper::WriteLong(ino + 1, end_key);
}

// format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino]
// size: 1+1+4+1+8 = 15
std::string MetaDataCodec::EncodeDirInodeKey(int fs_id, uint64_t ino) {
  CHECK(fs_id > 0) << fmt::format("Invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 15);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeDentry);
  key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kDelimiter);
  SerialHelper::WriteLong(ino, key);

  return key;
}

void MetaDataCodec::DecodeDirInodeKey(const std::string& key, int& fs_id, uint64_t& ino) {
  CHECK(key.size() == (kPrefixSize + 15)) << fmt::format("Key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeDentry) << "Key type is invalid.";
  CHECK(key.at(kPrefixSize + 1) == kDelimiter) << "Delimiter is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 2, kPrefixSize + 6));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 7, kPrefixSize + 15));
}

std::string MetaDataCodec::EncodeDirInodeValue(const pb::mdsv2::Inode& inode) { return inode.SerializeAsString(); }

pb::mdsv2::Inode MetaDataCodec::DecodeDirInodeValue(const std::string& value) {
  pb::mdsv2::Inode inode;
  CHECK(inode.ParseFromString(value)) << "Parse inode fail.";
  return inode;
}

// format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino]
// size: 1+1+4+1+8 = 15
std::string MetaDataCodec::EncodeFileInodeKey(int fs_id, uint64_t ino) {
  CHECK(fs_id > 0) << fmt::format("Invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 15);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeFileInode);
  key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kDelimiter);
  SerialHelper::WriteLong(ino, key);

  return key;
}

void MetaDataCodec::DecodeFileInodeKey(const std::string& key, int& fs_id, uint64_t& ino) {
  CHECK(key.size() == (kPrefixSize + 15)) << fmt::format("Key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeFileInode) << "Key type is invalid.";
  CHECK(key.at(kPrefixSize + 1) == kDelimiter) << "Delimiter is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 2, kPrefixSize + 6));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 7, kPrefixSize + 15));
}

std::string MetaDataCodec::EncodeFileInodeValue(const pb::mdsv2::Inode& inode) { return inode.SerializeAsString(); }

pb::mdsv2::Inode MetaDataCodec::DecodeFileInodeValue(const std::string& value) {
  pb::mdsv2::Inode inode;
  CHECK(inode.ParseFromString(value)) << "Parse inode fail.";
  return inode;
}

}  // namespace mdsv2
}  // namespace dingofs
