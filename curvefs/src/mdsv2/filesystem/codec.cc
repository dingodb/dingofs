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

#include "curvefs/src/mdsv2/filesystem/codec.h"

#include <string>

#include "curvefs/src/mdsv2/common/helper.h"
#include "curvefs/src/mdsv2/common/serial_helper.h"
#include "fmt/core.h"
#include "glog/logging.h"

namespace dingofs {
namespace mdsv2 {

static const char kDelimiter = ':';

enum KeyType : unsigned char {
  kTypeFS = 1,
  kTypeDentry = 2,
  kTypeFileInode = 3,
  kTypeFsQuota = 4,
  kTypeDirQuota = 5,
};

// format: [$prefix, $kDelimiter, $name]
// size: >= 1+1+1 = 3
std::string MetaDataCodec::EncodeFSKey(const std::string& name) {
  CHECK(!name.empty()) << fmt::format("FS name is empty.", name);

  std::string key;
  key.reserve(6);

  key.push_back(KeyType::kTypeFS);
  key.push_back(kDelimiter);
  key.append(name);

  return key;
}

void MetaDataCodec::DecodeFSKey(const std::string& key, std::string& name) {
  CHECK(key.size() >= 3) << fmt::format("Key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(0) == KeyType::kTypeFS) << "Key type is invalid.";
  CHECK(key.at(1) == kDelimiter) << "Delimiter is invalid.";

  name = key.substr(2);
}

// format: []$prefix, $kDelimiter, $fs_id, $kDelimiter, $inode_id, $kDelimiter, $name]
/// size: >= 1+1+4+1+8+1+1 = 17
std::string MetaDataCodec::EncodeDentryKey(int fs_id, uint64_t inode_id, const std::string& name) {
  CHECK(fs_id > 0) << fmt::format("Invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(16 + name.size());

  key.push_back(KeyType::kTypeDentry);
  key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kDelimiter);
  SerialHelper::WriteLong(inode_id, key);
  key.push_back(kDelimiter);
  key.append(name);

  return key;
}

void MetaDataCodec::DecodeDentryKey(const std::string& key, int& fs_id, uint64_t& inode_id, std::string& name) {
  CHECK(key.size() >= 17) << fmt::format("Key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(0) == KeyType::kTypeDentry) << "Key type is invalid.";
  CHECK(key.at(1) == kDelimiter) << "Delimiter is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(2, 6));
  inode_id = SerialHelper::ReadLong(key.substr(8, 16));
  name = key.substr(17);
}

// format: [$prefix, $kDelimiter, $fs_id, $kDelimiter, $inode_id]
// size: 1+1+4+1+8 = 15
std::string MetaDataCodec::EncodeDirInodeKey(int fs_id, uint64_t inode_id) {
  CHECK(fs_id > 0) << fmt::format("Invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(15);

  key.push_back(KeyType::kTypeDentry);
  key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kDelimiter);
  SerialHelper::WriteLong(inode_id, key);

  return key;
}

void MetaDataCodec::DecodeDirInodeKey(const std::string& key, int& fs_id, uint64_t& inode_id) {
  CHECK(key.size() == 15) << fmt::format("Key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(0) == KeyType::kTypeDentry) << "Key type is invalid.";
  CHECK(key.at(1) == kDelimiter) << "Delimiter is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(2, 6));
  inode_id = SerialHelper::ReadLong(key.substr(8, 16));
}

// format: [$prefix, $kDelimiter, $fs_id, $kDelimiter, $inode_id]
// size: 1+1+4+1+8 = 15
std::string MetaDataCodec::EncodeFileInodeKey(int fs_id, uint64_t inode_id) {
  CHECK(fs_id > 0) << fmt::format("Invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(15);

  key.push_back(KeyType::kTypeFileInode);
  key.push_back(kDelimiter);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kDelimiter);
  SerialHelper::WriteLong(inode_id, key);

  return key;
}

void MetaDataCodec::DecodeFileInodeKey(const std::string& key, int& fs_id, uint64_t& inode_id) {
  CHECK(key.size() == 15) << fmt::format("Key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(0) == KeyType::kTypeFileInode) << "Key type is invalid.";
  CHECK(key.at(1) == kDelimiter) << "Delimiter is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(2, 6));
  inode_id = SerialHelper::ReadLong(key.substr(8, 16));
}

}  // namespace mdsv2
}  // namespace dingofs
