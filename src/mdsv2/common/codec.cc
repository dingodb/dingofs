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

#include "mdsv2/common/codec.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/serial_helper.h"

namespace dingofs {
namespace mdsv2 {

// all key prefix
static const char* const kPrefix = "xDINGOFS:";
static const size_t kPrefixSize = std::char_traits<char>::length(kPrefix);

// not continuous for don't make region merge (region range not continuous)
enum KeyType : unsigned char {
  kTypeLock = 1,
  kTypeAutoIncrement = 3,
  kTypeHeartbeat = 5,
  kTypeFS = 7,
  kTypeDentryOrInode = 9,
  kTypeQuota = 11,
  kTypeFsStats = 15,
  kTypeFileSession = 17,
  kTypeTrashChunk = 19,
  kTypeDelFile = 21,
};

void MetaCodec::GetLockTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeLock);

  end_key = kPrefix;
  end_key.push_back(kTypeLock + 1);
}

void MetaCodec::GetAutoIncrementTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeAutoIncrement);

  end_key = kPrefix;
  end_key.push_back(kTypeAutoIncrement + 1);
}

void MetaCodec::GetHeartbeatTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeHeartbeat);

  end_key = kPrefix;
  end_key.push_back(kTypeHeartbeat + 1);
}

void MetaCodec::GetHeartbeatMdsRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeHeartbeat);
  start_key.push_back(pb::mdsv2::ROLE_MDS);

  end_key = kPrefix;
  end_key.push_back(kTypeHeartbeat);
  end_key.push_back(pb::mdsv2::ROLE_MDS + 1);
}

void MetaCodec::GetHeartbeatClientRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeHeartbeat);
  start_key.push_back(pb::mdsv2::ROLE_CLIENT);

  end_key = kPrefix;
  end_key.push_back(kTypeHeartbeat);
  end_key.push_back(pb::mdsv2::ROLE_CLIENT + 1);
}

void MetaCodec::GetFsTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFS);

  end_key = kPrefix;
  end_key.push_back(kTypeFS + 1);
}

void MetaCodec::GetDentryTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeDentryOrInode);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeDentryOrInode);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaCodec::GetDentryTableRange(uint32_t fs_id, Ino ino, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeDentryOrInode);
  SerialHelper::WriteInt(fs_id, start_key);
  SerialHelper::WriteULong(ino, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeDentryOrInode);
  SerialHelper::WriteInt(fs_id, end_key);
  SerialHelper::WriteULong(ino + 1, end_key);
}

void MetaCodec::GetQuotaTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeQuota);

  end_key = kPrefix;
  end_key.push_back(kTypeQuota + 1);
}

void MetaCodec::GetDirQuotaRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeQuota);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeQuota);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaCodec::GetFsStatsTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFsStats);

  end_key = kPrefix;
  end_key.push_back(kTypeFsStats + 1);
}

void MetaCodec::GetFsStatsRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFsStats);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeFsStats);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaCodec::GetFileSessionTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFileSession);

  end_key = kPrefix;
  end_key.push_back(kTypeFileSession + 1);
}

void MetaCodec::GetFsFileSessionRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFileSession);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeFileSession);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaCodec::GetFileSessionRange(uint32_t fs_id, Ino ino, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFileSession);
  SerialHelper::WriteInt(fs_id, start_key);
  SerialHelper::WriteULong(ino, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeFileSession);
  SerialHelper::WriteInt(fs_id, end_key);
  SerialHelper::WriteULong(ino + 1, end_key);
}

void MetaCodec::GetTrashChunkTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeTrashChunk);

  end_key = kPrefix;
  end_key.push_back(kTypeTrashChunk + 1);
}

void MetaCodec::GetTrashChunkRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeTrashChunk);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeTrashChunk);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaCodec::GetTrashChunkRange(uint32_t fs_id, Ino ino, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeTrashChunk);
  SerialHelper::WriteInt(fs_id, start_key);
  SerialHelper::WriteULong(ino, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeTrashChunk);
  SerialHelper::WriteInt(fs_id, end_key);
  SerialHelper::WriteULong(ino + 1, end_key);
}

void MetaCodec::GetTrashChunkRange(uint32_t fs_id, Ino ino, uint64_t chunk_index, std::string& start_key,
                                   std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeTrashChunk);
  SerialHelper::WriteInt(fs_id, start_key);
  SerialHelper::WriteULong(ino, start_key);
  SerialHelper::WriteULong(chunk_index, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeTrashChunk);
  SerialHelper::WriteInt(fs_id, end_key);
  SerialHelper::WriteULong(ino, end_key);
  SerialHelper::WriteULong(chunk_index + 1, end_key);
}

void MetaCodec::GetDelFileTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeDelFile);

  end_key = kPrefix;
  end_key.push_back(kTypeDelFile + 1);
}

void MetaCodec::GetDelFileTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeDelFile);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeDelFile);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

// format: [$prefix, $type, $name]
std::string MetaCodec::EncodeLockKey(const std::string& name) {
  CHECK(!name.empty()) << fmt::format("lock name is empty.", name);

  std::string key;
  key.reserve(kPrefixSize + 1 + name.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeLock);
  key.append(name);

  return std::move(key);
}

void MetaCodec::DecodeLockKey(const std::string& key, std::string& name) {
  CHECK(key.size() > (kPrefixSize + 1)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeLock) << "key type is invalid.";

  name = key.substr(kPrefixSize + 1);
}

// format: [$mds_id, $expire_time_ms]
std::string MetaCodec::EncodeLockValue(int64_t mds_id, uint64_t expire_time_ms) {
  std::string value;

  SerialHelper::WriteLong(mds_id, value);
  SerialHelper::WriteULong(expire_time_ms, value);

  return std::move(value);
}

void MetaCodec::DecodeLockValue(const std::string& value, int64_t& mds_id, uint64_t& expire_time_ms) {
  CHECK(value.size() == 16) << fmt::format("value({}) length is invalid.", Helper::StringToHex(value));

  mds_id = SerialHelper::ReadLong(value);
  expire_time_ms = SerialHelper::ReadULong(value.substr(8));
}

std::string MetaCodec::EncodeAutoIncrementKey(const std::string& name) {
  CHECK(!name.empty()) << fmt::format("autoincrement name is empty.", name);

  std::string key;
  key.reserve(kPrefixSize + 1 + name.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeAutoIncrement);
  key.append(name);

  return std::move(key);
}

void MetaCodec::DecodeAutoIncrementKey(const std::string& key, std::string& name) {
  CHECK(key.size() > (kPrefixSize + 1)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeAutoIncrement) << "key type is invalid.";

  name = key.substr(kPrefixSize + 1);
}

std::string MetaCodec::EncodeAutoIncrementValue(uint64_t id) {
  std::string value;

  SerialHelper::WriteULong(id, value);

  return std::move(value);
}

void MetaCodec::DecodeAutoIncrementValue(const std::string& value, uint64_t& id) {
  CHECK(value.size() == 16) << fmt::format("value({}) length is invalid.", Helper::StringToHex(value));

  id = SerialHelper::ReadULong(value);
}

// format: [$prefix, $type, $role, $mds_id]
std::string MetaCodec::EncodeHeartbeatKey(int64_t mds_id) {
  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeHeartbeat);
  SerialHelper::WriteInt(pb::mdsv2::ROLE_MDS, key);
  SerialHelper::WriteLong(mds_id, key);

  return key;
}

// or format: [$prefix, $type, $role, $client_mountpoint]
std::string MetaCodec::EncodeHeartbeatKey(const std::string& client_mountpoint) {
  std::string key;
  key.reserve(kPrefixSize + 32 + client_mountpoint.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeHeartbeat);
  SerialHelper::WriteInt(pb::mdsv2::ROLE_CLIENT, key);
  key.append(client_mountpoint);

  return key;
}

void MetaCodec::DecodeHeartbeatKey(const std::string& key, int64_t& mds_id) {
  CHECK(key.size() == (kPrefixSize + 13)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeHeartbeat) << "key type is invalid.";

  mds_id = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
}

void MetaCodec::DecodeHeartbeatKey(const std::string& key, std::string& client_mountpoint) {
  CHECK(key.size() > (kPrefixSize + 5)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeHeartbeat) << "key type is invalid.";

  client_mountpoint = key.substr(kPrefixSize + 5);
}

std::string MetaCodec::EncodeHeartbeatValue(const pb::mdsv2::MDS& mds) { return mds.SerializeAsString(); }

std::string MetaCodec::EncodeHeartbeatValue(const pb::mdsv2::Client& client) { return client.SerializeAsString(); }

void MetaCodec::DecodeHeartbeatValue(const std::string& value, pb::mdsv2::MDS& mds) {
  CHECK(mds.ParseFromString(value)) << "parse mds fail.";
}

void MetaCodec::DecodeHeartbeatValue(const std::string& value, pb::mdsv2::Client& client) {
  CHECK(client.ParseFromString(value)) << "parse client fail.";
}

// format: [$prefix, $type, $name]
// size: >= 1+1+1 = 3
std::string MetaCodec::EncodeFSKey(const std::string& name) {
  CHECK(!name.empty()) << fmt::format("fs name is empty.", name);

  std::string key;
  key.reserve(kPrefixSize + 32 + name.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeFS);
  key.append(name);

  return std::move(key);
}

void MetaCodec::DecodeFSKey(const std::string& key, std::string& name) {
  CHECK(key.size() > (kPrefixSize + 1)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeFS) << "key type is invalid.";

  name = key.substr(kPrefixSize + 1);
}

std::string MetaCodec::EncodeFSValue(const pb::mdsv2::FsInfo& fs_info) { return fs_info.SerializeAsString(); }

pb::mdsv2::FsInfo MetaCodec::DecodeFSValue(const std::string& value) {
  pb::mdsv2::FsInfo fs_info;
  CHECK(fs_info.ParseFromString(value)) << "parse fs info fail.";
  return fs_info;
}

// format: [$prefix, $type, $fs_id, $ino, $name]
std::string MetaCodec::EncodeDentryKey(uint32_t fs_id, Ino ino, const std::string& name) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 32 + name.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeDentryOrInode);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteULong(ino, key);
  key.append(name);

  return key;
}

void MetaCodec::DecodeDentryKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, std::string& name) {
  CHECK(key.size() >= (kPrefixSize + 13))
      << fmt::format("key({}) length({}) is invalid.", Helper::StringToHex(key), key.size());
  CHECK(key.at(kPrefixSize) == KeyType::kTypeDentryOrInode) << "Key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
  name = key.substr(kPrefixSize + 13);
}

std::string MetaCodec::EncodeDentryValue(const pb::mdsv2::Dentry& dentry) { return dentry.SerializeAsString(); }

pb::mdsv2::Dentry MetaCodec::DecodeDentryValue(const std::string& value) {
  pb::mdsv2::Dentry dentry;
  CHECK(dentry.ParseFromString(value)) << "parse dentry fail.";
  return dentry;
}

void MetaCodec::EncodeDentryRange(uint32_t fs_id, Ino ino, std::string& start_key, std::string& end_key) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);
  CHECK(ino > 0) << fmt::format("invalid ino {}.", ino);

  start_key.reserve(kPrefixSize + 32);

  start_key.append(kPrefix);
  start_key.push_back(KeyType::kTypeDentryOrInode);
  SerialHelper::WriteInt(fs_id, start_key);
  SerialHelper::WriteULong(ino, start_key);

  end_key.reserve(kPrefixSize + 32);

  end_key.append(kPrefix);
  end_key.push_back(KeyType::kTypeDentryOrInode);
  SerialHelper::WriteInt(fs_id, end_key);
  SerialHelper::WriteULong(ino + 1, end_key);
}

uint32_t MetaCodec::InodeKeyLength() { return kPrefixSize + 13; }

static std::string EncodeInodeKeyImpl(int fs_id, Ino ino, KeyType type) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(type);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteULong(ino, key);

  return key;
}

std::string MetaCodec::EncodeInodeKey(uint32_t fs_id, Ino ino) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeDentryOrInode);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteULong(ino, key);

  return key;
}

void MetaCodec::DecodeInodeKey(const std::string& key, uint32_t& fs_id, uint64_t& ino) {
  CHECK(key.size() == (kPrefixSize + 13)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
}

std::string MetaCodec::EncodeInodeValue(const AttrType& attr) { return attr.SerializeAsString(); }

AttrType MetaCodec::DecodeInodeValue(const std::string& value) {
  AttrType attr;
  CHECK(attr.ParseFromString(value)) << "parse attr fail.";
  return std::move(attr);
}

// fs format: [$prefix, $type, $fs_id]
std::string MetaCodec::EncodeFsQuotaKey(uint32_t fs_id) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeQuota);
  SerialHelper::WriteInt(fs_id, key);

  return key;
}

void MetaCodec::DecodeFsQuotaKey(const std::string& key, uint32_t& fs_id) {
  CHECK(key.size() == (kPrefixSize + 5)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeQuota) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
}

std::string MetaCodec::EncodeFsQuotaValue(const QuotaEntry& quota) { return quota.SerializeAsString(); }

QuotaEntry MetaCodec::DecodeFsQuotaValue(const std::string& value) {
  QuotaEntry quota;
  CHECK(quota.ParseFromString(value)) << "parse quota fail.";
  return quota;
}

uint32_t MetaCodec::DirQuotaKeyLength() {
  return kPrefixSize + 13;  // prefix + type + fs_id + ino
}

// dir format: [$prefix, $type, $fs_id, $ino]
std::string MetaCodec::EncodeDirQuotaKey(uint32_t fs_id, Ino ino) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeQuota);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteULong(ino, key);

  return key;
}

void MetaCodec::DecodeDirQuotaKey(const std::string& key, uint32_t& fs_id, uint64_t& ino) {
  CHECK(key.size() == (kPrefixSize + 13)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeQuota) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
}

std::string MetaCodec::EncodeDirQuotaValue(const QuotaEntry& quota) { return quota.SerializeAsString(); }

QuotaEntry MetaCodec::DecodeDirQuotaValue(const std::string& value) {
  QuotaEntry quota;
  CHECK(quota.ParseFromString(value)) << "parse quota fail.";
  return std::move(quota);
}

std::string MetaCodec::EncodeFsStatsKey(uint32_t fs_id, uint64_t time_ns) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeFsStats);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteULong(time_ns, key);

  return key;
}

void MetaCodec::DecodeFsStatsKey(const std::string& key, uint32_t& fs_id, uint64_t& time_ns) {
  CHECK(key.size() == (kPrefixSize + 13)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeFsStats) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  time_ns = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
}

std::string MetaCodec::EncodeFsStatsValue(const pb::mdsv2::FsStatsData& stats) { return stats.SerializeAsString(); }

pb::mdsv2::FsStatsData MetaCodec::DecodeFsStatsValue(const std::string& value) {
  pb::mdsv2::FsStatsData stats;
  CHECK(stats.ParseFromString(value)) << "parse fs stats fail.";
  return std::move(stats);
}

std::string MetaCodec::EncodeFileSessionKey(uint32_t fs_id, Ino ino, const std::string& session_id) {
  std::string key;
  key.reserve(kPrefixSize + 32 + session_id.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeFileSession);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteULong(ino, key);
  ;
  key.append(session_id);

  return key;
}

void MetaCodec::DecodeFileSessionKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, std::string& session_id) {
  CHECK(key.size() == (kPrefixSize + 13 + session_id.size()))
      << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeFileSession) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
  session_id = key.substr(kPrefixSize + 13);
}

std::string MetaCodec::EncodeFileSessionValue(const pb::mdsv2::FileSession& file_session) {
  return file_session.SerializeAsString();
}

pb::mdsv2::FileSession MetaCodec::DecodeFileSessionValue(const std::string& value) {
  pb::mdsv2::FileSession file_session;
  CHECK(file_session.ParseFromString(value)) << "parse file session fail.";
  return std::move(file_session);
}

std::string MetaCodec::EncodeTrashChunkKey(uint32_t fs_id, Ino ino, uint64_t chunk_index, uint64_t time_ns) {
  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeTrashChunk);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteULong(ino, key);
  SerialHelper::WriteULong(chunk_index, key);
  SerialHelper::WriteULong(time_ns, key);

  return std::move(key);
}
void MetaCodec::DecodeTrashChunkKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, uint64_t& chunk_index,
                                    uint64_t& time_ns) {
  CHECK(key.size() == (kPrefixSize + 29)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeTrashChunk) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadULong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
  chunk_index = SerialHelper::ReadULong(key.substr(kPrefixSize + 13, kPrefixSize + 21));
  time_ns = SerialHelper::ReadULong(key.substr(kPrefixSize + 21, kPrefixSize + 29));
}

std::string MetaCodec::EncodeTrashChunkValue(const pb::mdsv2::TrashSliceList& slice) {
  return slice.SerializeAsString();
}

pb::mdsv2::TrashSliceList MetaCodec::DecodeTrashChunkValue(const std::string& value) {
  pb::mdsv2::TrashSliceList slice_list;
  CHECK(slice_list.ParseFromString(value)) << "parse trash slice list fail.";
  return std::move(slice_list);
}

std::string MetaCodec::EncodeDelFileKey(uint32_t fs_id, Ino ino) {
  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeDelFile);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteULong(ino, key);

  return std::move(key);
}

void MetaCodec::DecodeDelFileKey(const std::string& key, uint32_t& fs_id, uint64_t& ino) {
  CHECK(key.size() == (kPrefixSize + 13)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeDelFile) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
}

std::string MetaCodec::EncodeDelFileValue(const AttrType& attr) { return attr.SerializeAsString(); }

AttrType MetaCodec::DecodeDelFileValue(const std::string& value) {
  AttrType attr;
  CHECK(attr.ParseFromString(value)) << "parse attr fail.";
  return std::move(attr);
}

}  // namespace mdsv2
}  // namespace dingofs
