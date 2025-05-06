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
  kTypeHeartbeat = 3,
  kTypeFS = 5,
  kTypeDentryOrDir = 7,
  kTypeFile = 9,
  kTypeFsQuota = 11,
  kTypeDirQuota = 13,
  kTypeFsStats = 15,
  kTypeFileSession = 17,
  kTypeChunk = 19,
  kTypeTrashChunk = 21,
  kTypeDelFile = 23,
};

void MetaDataCodec::GetLockTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeLock);

  end_key = kPrefix;
  end_key.push_back(kTypeLock + 1);
}

void MetaDataCodec::GetHeartbeatTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeHeartbeat);

  end_key = kPrefix;
  end_key.push_back(kTypeHeartbeat + 1);
}

void MetaDataCodec::GetHeartbeatMdsRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeHeartbeat);
  start_key.push_back(pb::mdsv2::ROLE_MDS);

  end_key = kPrefix;
  end_key.push_back(kTypeHeartbeat);
  end_key.push_back(pb::mdsv2::ROLE_MDS + 1);
}

void MetaDataCodec::GetHeartbeatClientRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeHeartbeat);
  start_key.push_back(pb::mdsv2::ROLE_CLIENT);

  end_key = kPrefix;
  end_key.push_back(kTypeHeartbeat);
  end_key.push_back(pb::mdsv2::ROLE_CLIENT + 1);
}

void MetaDataCodec::GetFsTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFS);

  end_key = kPrefix;
  end_key.push_back(kTypeFS + 1);
}

void MetaDataCodec::GetDentryTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeDentryOrDir);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeDentryOrDir);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaDataCodec::GetFileInodeTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFile);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeFile);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaDataCodec::GetQuotaTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFsQuota);

  end_key = kPrefix;
  end_key.push_back(kTypeFsQuota + 1);
}

void MetaDataCodec::GetDirQuotaRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeDirQuota);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeDirQuota);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaDataCodec::GetFsStatsTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFsStats);

  end_key = kPrefix;
  end_key.push_back(kTypeFsStats + 1);
}

void MetaDataCodec::GetFsStatsRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFsStats);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeFsStats);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaDataCodec::GetFileSessionTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFileSession);

  end_key = kPrefix;
  end_key.push_back(kTypeFileSession + 1);
}

void MetaDataCodec::GetFsFileSessionRange(uint32_t fs_id, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFileSession);
  SerialHelper::WriteInt(fs_id, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeFileSession);
  SerialHelper::WriteInt(fs_id + 1, end_key);
}

void MetaDataCodec::GetFileSessionRange(uint32_t fs_id, uint64_t ino, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeFileSession);
  SerialHelper::WriteInt(fs_id, start_key);
  SerialHelper::WriteLong(ino, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeFileSession);
  SerialHelper::WriteInt(fs_id, end_key);
  SerialHelper::WriteLong(ino + 1, end_key);
}

void MetaDataCodec::GetChunkTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeChunk);

  end_key = kPrefix;
  end_key.push_back(kTypeChunk + 1);
}

void MetaDataCodec::GetChunkRange(uint32_t fs_id, uint64_t ino, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeChunk);
  SerialHelper::WriteInt(fs_id, start_key);
  SerialHelper::WriteULong(ino, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeChunk);
  SerialHelper::WriteInt(fs_id, end_key);
  SerialHelper::WriteULong(ino + 1, end_key);
}

void MetaDataCodec::GetChunkRange(uint32_t fs_id, uint64_t ino, uint64_t chunk_index, std::string& start_key,
                                  std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeChunk);
  SerialHelper::WriteInt(fs_id, start_key);
  SerialHelper::WriteULong(ino, start_key);
  SerialHelper::WriteULong(chunk_index, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeChunk);
  SerialHelper::WriteInt(fs_id, end_key);
  SerialHelper::WriteULong(ino, end_key);
  SerialHelper::WriteULong(chunk_index + 1, end_key);
}

void MetaDataCodec::GetTrashChunkTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeTrashChunk);

  end_key = kPrefix;
  end_key.push_back(kTypeTrashChunk + 1);
}

void MetaDataCodec::GetTrashChunkRange(uint32_t fs_id, uint64_t ino, std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeTrashChunk);
  SerialHelper::WriteInt(fs_id, start_key);
  SerialHelper::WriteLong(ino, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeTrashChunk);
  SerialHelper::WriteInt(fs_id, end_key);
  SerialHelper::WriteLong(ino + 1, end_key);
}

void MetaDataCodec::GetTrashChunkRange(uint32_t fs_id, uint64_t ino, uint64_t chunk_index, std::string& start_key,
                                       std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeTrashChunk);
  SerialHelper::WriteInt(fs_id, start_key);
  SerialHelper::WriteLong(ino, start_key);
  SerialHelper::WriteULong(chunk_index, start_key);

  end_key = kPrefix;
  end_key.push_back(kTypeTrashChunk);
  SerialHelper::WriteInt(fs_id, end_key);
  SerialHelper::WriteLong(ino, end_key);
  SerialHelper::WriteULong(chunk_index + 1, end_key);
}

void MetaDataCodec::GetDelFileTableRange(std::string& start_key, std::string& end_key) {
  start_key = kPrefix;
  start_key.push_back(kTypeDelFile);

  end_key = kPrefix;
  end_key.push_back(kTypeDelFile + 1);
}

// format: [$prefix, $type, $name]
std::string MetaDataCodec::EncodeLockKey(const std::string& name) {
  CHECK(!name.empty()) << fmt::format("lock name is empty.", name);

  std::string key;
  key.reserve(kPrefixSize + 1 + name.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeLock);
  key.append(name);

  return std::move(key);
}

void MetaDataCodec::DecodeLockKey(const std::string& key, std::string& name) {
  CHECK(key.size() > (kPrefixSize + 1)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeLock) << "key type is invalid.";

  name = key.substr(kPrefixSize + 1);
}

// format: [$mds_id, $expire_time_ms]
std::string MetaDataCodec::EncodeLockValue(int64_t mds_id, uint64_t expire_time_ms) {
  std::string value;

  SerialHelper::WriteLong(mds_id, value);
  SerialHelper::WriteULong(expire_time_ms, value);

  return std::move(value);
}

void MetaDataCodec::DecodeLockValue(const std::string& value, int64_t& mds_id, uint64_t& expire_time_ms) {
  CHECK(value.size() == 16) << fmt::format("value({}) length is invalid.", Helper::StringToHex(value));

  mds_id = SerialHelper::ReadLong(value);
  expire_time_ms = SerialHelper::ReadULong(value.substr(8));
}

// format: [$prefix, $type, $role, $mds_id]
std::string MetaDataCodec::EncodeHeartbeatKey(int64_t mds_id) {
  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeHeartbeat);
  SerialHelper::WriteInt(pb::mdsv2::ROLE_MDS, key);
  SerialHelper::WriteLong(mds_id, key);

  return key;
}

// or format: [$prefix, $type, $role, $client_mountpoint]
std::string MetaDataCodec::EncodeHeartbeatKey(const std::string& client_mountpoint) {
  std::string key;
  key.reserve(kPrefixSize + 32 + client_mountpoint.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeHeartbeat);
  SerialHelper::WriteInt(pb::mdsv2::ROLE_CLIENT, key);
  key.append(client_mountpoint);

  return key;
}

void MetaDataCodec::DecodeHeartbeatKey(const std::string& key, int64_t& mds_id) {
  CHECK(key.size() == (kPrefixSize + 13)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeHeartbeat) << "key type is invalid.";

  mds_id = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
}

void MetaDataCodec::DecodeHeartbeatKey(const std::string& key, std::string& client_mountpoint) {
  CHECK(key.size() > (kPrefixSize + 5)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeHeartbeat) << "key type is invalid.";

  client_mountpoint = key.substr(kPrefixSize + 5);
}

std::string MetaDataCodec::EncodeHeartbeatValue(const pb::mdsv2::MDS& mds) { return mds.SerializeAsString(); }

std::string MetaDataCodec::EncodeHeartbeatValue(const pb::mdsv2::Client& client) { return client.SerializeAsString(); }

void MetaDataCodec::DecodeHeartbeatValue(const std::string& value, pb::mdsv2::MDS& mds) {
  CHECK(mds.ParseFromString(value)) << "parse mds fail.";
}

void MetaDataCodec::DecodeHeartbeatValue(const std::string& value, pb::mdsv2::Client& client) {
  CHECK(client.ParseFromString(value)) << "parse client fail.";
}

// format: [$prefix, $type, $name]
// size: >= 1+1+1 = 3
std::string MetaDataCodec::EncodeFSKey(const std::string& name) {
  CHECK(!name.empty()) << fmt::format("fs name is empty.", name);

  std::string key;
  key.reserve(kPrefixSize + 32 + name.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeFS);
  key.append(name);

  return std::move(key);
}

void MetaDataCodec::DecodeFSKey(const std::string& key, std::string& name) {
  CHECK(key.size() > (kPrefixSize + 1)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeFS) << "key type is invalid.";

  name = key.substr(kPrefixSize + 1);
}

std::string MetaDataCodec::EncodeFSValue(const pb::mdsv2::FsInfo& fs_info) { return fs_info.SerializeAsString(); }

pb::mdsv2::FsInfo MetaDataCodec::DecodeFSValue(const std::string& value) {
  pb::mdsv2::FsInfo fs_info;
  CHECK(fs_info.ParseFromString(value)) << "parse fs info fail.";
  return fs_info;
}

// format: [$prefix, $type, $fs_id, $ino, $name]
std::string MetaDataCodec::EncodeDentryKey(uint32_t fs_id, uint64_t ino, const std::string& name) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 32 + name.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeDentryOrDir);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteULong(ino, key);
  key.append(name);

  return key;
}

void MetaDataCodec::DecodeDentryKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, std::string& name) {
  CHECK(key.size() >= (kPrefixSize + 13))
      << fmt::format("key({}) length({}) is invalid.", Helper::StringToHex(key), key.size());
  CHECK(key.at(kPrefixSize) == KeyType::kTypeDentryOrDir) << "Key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
  name = key.substr(kPrefixSize + 13);
}

std::string MetaDataCodec::EncodeDentryValue(const pb::mdsv2::Dentry& dentry) { return dentry.SerializeAsString(); }

pb::mdsv2::Dentry MetaDataCodec::DecodeDentryValue(const std::string& value) {
  pb::mdsv2::Dentry dentry;
  CHECK(dentry.ParseFromString(value)) << "parse dentry fail.";
  return dentry;
}

void MetaDataCodec::EncodeDentryRange(uint32_t fs_id, uint64_t ino, std::string& start_key, std::string& end_key) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);
  CHECK(ino > 0) << fmt::format("invalid ino {}.", ino);

  start_key.reserve(kPrefixSize + 32);

  start_key.append(kPrefix);
  start_key.push_back(KeyType::kTypeDentryOrDir);
  SerialHelper::WriteInt(fs_id, start_key);
  SerialHelper::WriteULong(ino, start_key);

  end_key.reserve(kPrefixSize + 32);

  end_key.append(kPrefix);
  end_key.push_back(KeyType::kTypeDentryOrDir);
  SerialHelper::WriteInt(fs_id, end_key);
  SerialHelper::WriteULong(ino + 1, end_key);
}

uint32_t MetaDataCodec::InodeKeyLength() { return kPrefixSize + 13; }

static std::string EncodeInodeKeyImpl(int fs_id, uint64_t ino, KeyType type) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(type);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteLong(ino, key);

  return key;
}

std::string MetaDataCodec::EncodeInodeKey(uint32_t fs_id, uint64_t ino) {
  return EncodeInodeKeyImpl(fs_id, ino, ino % 2 == 1 ? KeyType::kTypeDentryOrDir : KeyType::kTypeFile);
}

void MetaDataCodec::DecodeInodeKey(const std::string& key, uint32_t& fs_id, uint64_t& ino) {
  CHECK(key.size() == (kPrefixSize + 13)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
}

std::string MetaDataCodec::EncodeInodeValue(const pb::mdsv2::Inode& inode) { return inode.SerializeAsString(); }

pb::mdsv2::Inode MetaDataCodec::DecodeInodeValue(const std::string& value) {
  pb::mdsv2::Inode inode;
  CHECK(inode.ParseFromString(value)) << "parse inode fail.";
  return std::move(inode);
}

// fs format: [$prefix, $type, $fs_id]
std::string MetaDataCodec::EncodeFsQuotaKey(uint32_t fs_id) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeFsQuota);
  SerialHelper::WriteInt(fs_id, key);

  return key;
}

void MetaDataCodec::DecodeFsQuotaKey(const std::string& key, uint32_t& fs_id) {
  CHECK(key.size() == (kPrefixSize + 5)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeFsQuota) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
}

std::string MetaDataCodec::EncodeFsQuotaValue(const pb::mdsv2::Quota& quota) { return quota.SerializeAsString(); }

pb::mdsv2::Quota MetaDataCodec::DecodeFsQuotaValue(const std::string& value) {
  pb::mdsv2::Quota quota;
  CHECK(quota.ParseFromString(value)) << "parse quota fail.";
  return quota;
}

// dir format: [$prefix, $type, $fs_id, $ino]
std::string MetaDataCodec::EncodeDirQuotaKey(uint32_t fs_id, uint64_t ino) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeDirQuota);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteLong(ino, key);

  return key;
}

void MetaDataCodec::DecodeDirQuotaKey(const std::string& key, uint32_t& fs_id, uint64_t& ino) {
  CHECK(key.size() == (kPrefixSize + 13)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeDirQuota) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
}

std::string MetaDataCodec::EncodeDirQuotaValue(const pb::mdsv2::Quota& quota) { return quota.SerializeAsString(); }

pb::mdsv2::Quota MetaDataCodec::DecodeDirQuotaValue(const std::string& value) {
  pb::mdsv2::Quota quota;
  CHECK(quota.ParseFromString(value)) << "parse quota fail.";
  return std::move(quota);
}

std::string MetaDataCodec::EncodeFsStatsKey(uint32_t fs_id, uint64_t time_ns) {
  CHECK(fs_id > 0) << fmt::format("invalid fs_id {}.", fs_id);

  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeFsStats);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteLong(time_ns, key);

  return key;
}

void MetaDataCodec::DecodeFsStatsKey(const std::string& key, uint32_t& fs_id, uint64_t& time_ns) {
  CHECK(key.size() == (kPrefixSize + 13)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeFsStats) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  time_ns = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
}

std::string MetaDataCodec::EncodeFsStatsValue(const pb::mdsv2::FsStatsData& stats) { return stats.SerializeAsString(); }

pb::mdsv2::FsStatsData MetaDataCodec::DecodeFsStatsValue(const std::string& value) {
  pb::mdsv2::FsStatsData stats;
  CHECK(stats.ParseFromString(value)) << "parse fs stats fail.";
  return std::move(stats);
}

std::string MetaDataCodec::EncodeFileSessionKey(uint32_t fs_id, uint64_t ino, const std::string& session_id) {
  std::string key;
  key.reserve(kPrefixSize + 32 + session_id.size());

  key.append(kPrefix);
  key.push_back(KeyType::kTypeFileSession);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteLong(ino, key);
  ;
  key.append(session_id);

  return key;
}

void MetaDataCodec::DecodeFileSessionKey(const std::string& key, uint32_t& fs_id, uint64_t& ino,
                                         std::string& session_id) {
  CHECK(key.size() == (kPrefixSize + 13 + session_id.size()))
      << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeFileSession) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
  session_id = key.substr(kPrefixSize + 13);
}

std::string MetaDataCodec::EncodeFileSessionValue(const pb::mdsv2::FileSession& file_session) {
  return file_session.SerializeAsString();
}

pb::mdsv2::FileSession MetaDataCodec::DecodeFileSessionValue(const std::string& value) {
  pb::mdsv2::FileSession file_session;
  CHECK(file_session.ParseFromString(value)) << "parse file session fail.";
  return std::move(file_session);
}

std::string MetaDataCodec::EncodeChunkKey(uint32_t fs_id, uint64_t ino, uint64_t chunk_index) {
  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeChunk);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteULong(ino, key);
  SerialHelper::WriteULong(chunk_index, key);

  return std::move(key);
}

void MetaDataCodec::DecodeChunkKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, uint64_t& chunk_index) {
  CHECK(key.size() == (kPrefixSize + 21)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeChunk) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadULong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
  chunk_index = SerialHelper::ReadULong(key.substr(kPrefixSize + 13, kPrefixSize + 21));
}

std::string MetaDataCodec::EncodeChunkValue(const pb::mdsv2::Chunk& chunk) { return chunk.SerializeAsString(); }

pb::mdsv2::Chunk MetaDataCodec::DecodeChunkValue(const std::string& value) {
  pb::mdsv2::Chunk chunk;
  CHECK(chunk.ParseFromString(value)) << "parse chunk fail.";

  return std::move(chunk);
}

std::string MetaDataCodec::EncodeTrashChunkKey(uint32_t fs_id, uint64_t ino, uint64_t chunk_index, uint64_t time_ns) {
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
void MetaDataCodec::DecodeTrashChunkKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, uint64_t& chunk_index,
                                        uint64_t& time_ns) {
  CHECK(key.size() == (kPrefixSize + 29)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeTrashChunk) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadULong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
  chunk_index = SerialHelper::ReadULong(key.substr(kPrefixSize + 13, kPrefixSize + 21));
  time_ns = SerialHelper::ReadULong(key.substr(kPrefixSize + 21, kPrefixSize + 29));
}

std::string MetaDataCodec::EncodeTrashChunkValue(const pb::mdsv2::TrashSliceList& slice) {
  return slice.SerializeAsString();
}

pb::mdsv2::TrashSliceList MetaDataCodec::DecodeTrashChunkValue(const std::string& value) {
  pb::mdsv2::TrashSliceList slice_list;
  CHECK(slice_list.ParseFromString(value)) << "parse trash slice list fail.";
  return std::move(slice_list);
}

std::string MetaDataCodec::EncodeDelFileKey(uint32_t fs_id, uint64_t ino) {
  std::string key;
  key.reserve(kPrefixSize + 32);

  key.append(kPrefix);
  key.push_back(KeyType::kTypeDelFile);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteLong(ino, key);

  return std::move(key);
}

void MetaDataCodec::DecodeDelFileKey(const std::string& key, uint32_t& fs_id, uint64_t& ino) {
  CHECK(key.size() == (kPrefixSize + 13)) << fmt::format("key({}) length is invalid.", Helper::StringToHex(key));
  CHECK(key.at(kPrefixSize) == KeyType::kTypeDelFile) << "key type is invalid.";

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1, kPrefixSize + 5));
  ino = SerialHelper::ReadLong(key.substr(kPrefixSize + 5, kPrefixSize + 13));
}

std::string MetaDataCodec::EncodeDelFileValue(const pb::mdsv2::Inode& inode) { return inode.SerializeAsString(); }

pb::mdsv2::Inode MetaDataCodec::DecodeDelFileValue(const std::string& value) {
  pb::mdsv2::Inode inode;
  CHECK(inode.ParseFromString(value)) << "parse inode fail.";
  return std::move(inode);
}

}  // namespace mdsv2
}  // namespace dingofs
