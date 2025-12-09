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

#include "mds/common/codec.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/helper.h"
#include "mds/common/serial_helper.h"

namespace dingofs {
namespace mds {

// all key prefix
static const char* const kPrefix = "xDINGOFS:";
static const size_t kPrefixSize = std::char_traits<char>::length(kPrefix);

// table:
//      kTableMeta: all filesystem shared
//      kTableFsStats: store fs stats for client upload, all filesystem shared
//      kTableFsMeta+{fs_id}: store fs meta, each filesystem has its own table
enum TableID : unsigned char {
  kTableMeta = 1,
  kTableFsStats = 3,
  kTableFsMeta = 5,
};

// meta type:
//      kMetaLock: lock, used for distributed lock
//      kMetaAutoIncrementID: auto increment id, used for fs id, ino
//      kMetaHeartbeat: heartbeat, used for mds/client/cache_member heartbeat
//      kMetaFs: fs info, used for filesystem info
//      kMetaFsQuota: fs quota, used for filesystem quota
//      kMetaFsDirQuota: directory quota, used for directory quota
//      kMetaFsInode: fs inode, used for file/directory inode
//     kMetaFsFileSession: fs file session, used for file session
//      kMetaFsStats: fs stats, used for filesystem stats
//      kMetaFsDelSlice: fs deleted slice, used for deleted file data slice
//      kMetaFsDelFile: fs deleted file, used for deleted file
enum MetaType : unsigned char {
  kMetaLock = 1,
  kMetaAutoIncrementID = 3,
  kMetaHeartbeat = 5,
  kMetaFs = 7,
  kMetaFsQuota = 9,
  kMetaFsDirQuota = 11,
  kMetaFsInode = 13,
  kMetaFsFileSession = 15,
  kMetaFsDelSlice = 17,
  kMetaFsDelFile = 19,
  kMetaFsStats = 21,
  kMetaFsOpLog = 23,
  kMetaCacheMember = 25,
};

// inode meta type:
//      kFsInodeAttr: inode basic attributes
//      kFsInodeDentry: dentry, file/directory name
//      kFsInodeChunk: chunk, file data chunk
// key locality:
//      directory: kFsInodeAttr kFsInodeDentry
//      file: kFsInodeAttr kFsInodeChunk
enum FsInodeType : unsigned char {
  kFsInodeAttr = 1,
  kFsInodeDentry = 3,
  kFsInodeChunk = 5,
};

Range MetaCodec::GetMetaTableRange() {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableMeta);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableMeta + 1);

  return range;
}

Range MetaCodec::GetFsStatsTableRange() {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsStats);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsStats + 1);

  return range;
}

Range MetaCodec::GetFsMetaTableRange(uint32_t fs_id) {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, start);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id + 1, end);

  return range;
}

Range MetaCodec::GetLockRange() {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableMeta);
  range.start.push_back(kMetaLock);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableMeta);
  end.push_back(kMetaLock + 1);

  return range;
}

Range MetaCodec::GetAutoIncrementIDRange() {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableMeta);
  start.push_back(kMetaAutoIncrementID);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableMeta);
  end.push_back(kMetaAutoIncrementID + 1);

  return range;
}

Range MetaCodec::GetHeartbeatMdsRange() {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableMeta);
  start.push_back(kMetaHeartbeat);
  start.push_back(pb::mds::ROLE_MDS);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableMeta);
  end.push_back(kMetaHeartbeat);
  end.push_back(pb::mds::ROLE_MDS + 1);

  return range;
}

Range MetaCodec::GetHeartbeatClientRange() {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableMeta);
  start.push_back(kMetaHeartbeat);
  start.push_back(pb::mds::ROLE_CLIENT);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableMeta);
  end.push_back(kMetaHeartbeat);
  end.push_back(pb::mds::ROLE_CLIENT + 1);

  return range;
}

Range MetaCodec::GetHeartbeatCacheMemberRange() {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableMeta);
  start.push_back(kMetaHeartbeat);
  start.push_back(pb::mds::ROLE_CACHE_MEMBER);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableMeta);
  end.push_back(kMetaHeartbeat);
  end.push_back(pb::mds::ROLE_CACHE_MEMBER + 1);

  return range;
}

Range MetaCodec::GetFsRange() {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableMeta);
  start.push_back(kMetaFs);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableMeta);
  end.push_back(kMetaFs + 1);

  return range;
}

Range MetaCodec::GetFsQuotaRange() {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableMeta);
  start.push_back(kMetaFsQuota);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableMeta);
  end.push_back(kMetaFsQuota + 1);

  return range;
}

Range MetaCodec::GetFsConfigLogRange(uint32_t fs_id) {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableMeta);
  start.push_back(kMetaFsOpLog);
  SerialHelper::WriteInt(fs_id, start);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableMeta);
  end.push_back(kMetaFsOpLog);
  SerialHelper::WriteInt(fs_id + 1, end);

  return range;
}

Range MetaCodec::GetDentryRange(uint32_t fs_id, Ino ino, bool include_parent) {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, start);
  start.push_back(kMetaFsInode);
  SerialHelper::WriteULong(ino, start);
  if (include_parent) {
    start.push_back(kFsInodeAttr);
  } else {
    start.push_back(kFsInodeDentry);
  }

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, end);
  end.push_back(kMetaFsInode);
  SerialHelper::WriteULong(ino, end);
  end.push_back(kFsInodeDentry + 1);

  return range;
}

Range MetaCodec::GetChunkRange(uint32_t fs_id, Ino ino) {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, start);
  start.push_back(kMetaFsInode);
  SerialHelper::WriteULong(ino, start);
  start.push_back(kFsInodeChunk);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, end);
  end.push_back(kMetaFsInode);
  SerialHelper::WriteULong(ino, end);
  end.push_back(kFsInodeChunk + 1);

  return range;
}

Range MetaCodec::GetFileSessionRange(uint32_t fs_id) {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, start);
  start.push_back(kMetaFsFileSession);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, end);
  end.push_back(kMetaFsFileSession + 1);

  return range;
}

Range MetaCodec::GetFileSessionRange(uint32_t fs_id, Ino ino) {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, start);
  start.push_back(kMetaFsFileSession);
  SerialHelper::WriteULong(ino, start);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, end);
  end.push_back(kMetaFsFileSession);
  SerialHelper::WriteULong(ino + 1, end);

  return range;
}

Range MetaCodec::GetDirQuotaRange(uint32_t fs_id) {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, start);
  start.push_back(kMetaFsDirQuota);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, end);
  end.push_back(kMetaFsDirQuota + 1);

  return range;
}

Range MetaCodec::GetDelSliceRange(uint32_t fs_id) {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, start);
  start.push_back(kMetaFsDelSlice);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, end);
  end.push_back(kMetaFsDelSlice + 1);

  return range;
}

Range MetaCodec::GetDelSliceRange(uint32_t fs_id, Ino ino) {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, start);
  start.push_back(kMetaFsDelSlice);
  SerialHelper::WriteULong(ino, start);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, end);
  end.push_back(kMetaFsDelSlice);
  SerialHelper::WriteULong(ino + 1, end);

  return range;
}

Range MetaCodec::GetDelSliceRange(uint32_t fs_id, Ino ino, uint64_t chunk_index) {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, start);
  start.push_back(kMetaFsDelSlice);
  SerialHelper::WriteULong(ino, start);
  SerialHelper::WriteULong(chunk_index, start);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, end);
  end.push_back(kMetaFsDelSlice);
  SerialHelper::WriteULong(ino, end);
  SerialHelper::WriteULong(chunk_index + 1, end);

  return range;
}

Range MetaCodec::GetDelFileTableRange(uint32_t fs_id) {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, start);
  start.push_back(kMetaFsDelFile);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, end);
  end.push_back(kMetaFsDelFile + 1);

  return range;
}

Range MetaCodec::GetFsStatsRange() {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsStats);
  start.push_back(kMetaFsStats);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsStats);
  end.push_back(kMetaFsStats + 1);

  return range;
}

Range MetaCodec::GetFsStatsRange(uint32_t fs_id) {
  Range range;

  auto& start = range.start;
  start = kPrefix;
  start.push_back(kTableFsStats);
  start.push_back(kMetaFsStats);
  SerialHelper::WriteInt(fs_id, start);

  auto& end = range.end;
  end = kPrefix;
  end.push_back(kTableFsStats);
  end.push_back(kMetaFsStats);
  SerialHelper::WriteInt(fs_id + 1, end);

  return range;
}

// lock format: ${prefix} kTableMeta kMetaLock {name}
static const uint32_t kLockKeyHeaderSize = kPrefixSize + 2;  // prefix + table id + meta type

bool MetaCodec::IsLockKey(const std::string& key) {
  if (key.size() <= kLockKeyHeaderSize) {
    return false;
  }

  if (key.at(kPrefixSize) != kTableMeta || key.at(kPrefixSize + 1) != kMetaLock) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeLockKey(const std::string& name) {
  std::string key;
  key.reserve(kLockKeyHeaderSize + name.size());

  key.append(kPrefix);
  key.push_back(kTableMeta);
  key.push_back(kMetaLock);
  key.append(name);

  return key;
}

void MetaCodec::DecodeLockKey(const std::string& key, std::string& name) {
  CHECK(IsLockKey(key)) << fmt::format("invalid lock key({}).", Helper::StringToHex(key));

  name = key.substr(kLockKeyHeaderSize);
}

std::string MetaCodec::EncodeLockValue(int64_t mds_id, uint64_t epoch, uint64_t expire_time_ms) {
  std::string value;
  value.reserve(24);

  SerialHelper::WriteLong(mds_id, value);
  SerialHelper::WriteULong(epoch, value);
  SerialHelper::WriteULong(expire_time_ms, value);

  return value;
}

void MetaCodec::DecodeLockValue(const std::string& value, int64_t& mds_id, uint64_t& epoch, uint64_t& expire_time_ms) {
  CHECK(value.size() == 24) << fmt::format("invalid lock value size({}).", value.size());

  mds_id = SerialHelper::ReadLong(value.substr(0, 8));
  epoch = SerialHelper::ReadULong(value.substr(8, 8));
  expire_time_ms = SerialHelper::ReadULong(value.substr(16, 8));
}

// auto increment id format: ${prefix} kTableMeta kMetaAutoIncrementID {name}
static const uint32_t kAutoIncrementIDKeyHeaderSize = kPrefixSize + 2;  // prefix + table id + meta type

bool MetaCodec::IsAutoIncrementIDKey(const std::string& key) {
  if (key.size() <= kAutoIncrementIDKeyHeaderSize) {
    return false;
  }

  if (key.at(kPrefixSize) != kTableMeta || key.at(kPrefixSize + 1) != kMetaAutoIncrementID) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeAutoIncrementIDKey(const std::string& name) {
  std::string key;
  key.reserve(kAutoIncrementIDKeyHeaderSize + name.size());

  key.append(kPrefix);
  key.push_back(kTableMeta);
  key.push_back(kMetaAutoIncrementID);
  key.append(name);

  return key;
}

void MetaCodec::DecodeAutoIncrementIDKey(const std::string& key, std::string& name) {
  CHECK(IsAutoIncrementIDKey(key)) << fmt::format("invalid auto increment id key({}).", Helper::StringToHex(key));

  name = key.substr(kAutoIncrementIDKeyHeaderSize);
}

std::string MetaCodec::EncodeAutoIncrementIDValue(uint64_t id) {
  std::string value;

  SerialHelper::WriteULong(id, value);

  return value;
}

void MetaCodec::DecodeAutoIncrementIDValue(const std::string& value, uint64_t& id) {
  CHECK(value.size() == 8) << fmt::format("value({}) length is invalid.", Helper::StringToHex(value));

  id = SerialHelper::ReadULong(value);
}

// heartbeat(mds) format: ${prefix} kTableMeta kMetaHeartbeat kRoleMds {mds_id}
// heartbeat(client) format: ${prefix} kTableMeta kMetaHeartbeat kRoleClient {client_id}
// heartbeat(cache_member) format: ${prefix} kTableMeta kMetaHeartbeat KROLE_CACHE_MEMBER {member_id}
static const uint32_t kHeartbeatKeyMdsSize = kPrefixSize + 1 + 1 + 1 + 8;
static const uint32_t kHeartbeatClientKeySize = kPrefixSize + 1 + 1 + 1 + 36;
static const uint32_t kHeartbeatCacheMemberKeySize = kPrefixSize + 1 + 1 + 1 + 36;
bool MetaCodec::IsMdsHeartbeatKey(const std::string& key) {
  if (key.size() != kHeartbeatKeyMdsSize) {
    return false;
  }

  if (key.at(kPrefixSize) != kTableMeta || key.at(kPrefixSize + 1) != kMetaHeartbeat ||
      key.at(kPrefixSize + 2) != pb::mds::ROLE_MDS) {
    return false;
  }

  return true;
}

bool MetaCodec::IsClientHeartbeatKey(const std::string& key) {
  if (key.size() != kHeartbeatClientKeySize) {
    return false;
  }

  if (key.at(kPrefixSize) != kTableMeta || key.at(kPrefixSize + 1) != kMetaHeartbeat ||
      key.at(kPrefixSize + 2) != pb::mds::ROLE_CLIENT) {
    return false;
  }

  return true;
}

bool MetaCodec::IsCacheMemberHeartbeatKey(const std::string& key) {
  if (key.size() != kHeartbeatCacheMemberKeySize) {
    return false;
  }

  if (key.at(kPrefixSize) != kTableMeta || key.at(kPrefixSize + 1) != kMetaHeartbeat ||
      key.at(kPrefixSize + 2) != pb::mds::ROLE_CACHE_MEMBER) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeHeartbeatKey(int64_t mds_id) {
  std::string key;
  key.reserve(kHeartbeatKeyMdsSize);

  key.append(kPrefix);
  key.push_back(kTableMeta);
  key.push_back(kMetaHeartbeat);
  key.push_back(pb::mds::ROLE_MDS);
  SerialHelper::WriteLong(mds_id, key);

  return key;
}

std::string MetaCodec::EncodeHeartbeatKey(const std::string& client_id) {
  CHECK(client_id.size() == 36) << fmt::format("client_id({}) length is invalid.", Helper::StringToHex(client_id));

  std::string key;
  key.reserve(kHeartbeatClientKeySize);

  key.append(kPrefix);
  key.push_back(kTableMeta);
  key.push_back(kMetaHeartbeat);
  key.push_back(pb::mds::ROLE_CLIENT);
  key.append(client_id);

  return key;
}

std::string MetaCodec::EncodeHeartbeatCacheMemberKey(const std::string& member_id) {
  CHECK(member_id.size() == 36) << fmt::format("member_id({}) length is invalid.", Helper::StringToHex(member_id));

  std::string key;
  key.reserve(kHeartbeatCacheMemberKeySize);

  key.append(kPrefix);
  key.push_back(kTableMeta);
  key.push_back(kMetaHeartbeat);
  key.push_back(pb::mds::ROLE_CACHE_MEMBER);
  key.append(member_id);

  return key;
}

void MetaCodec::DecodeHeartbeatKey(const std::string& key, int64_t& mds_id) {
  CHECK(IsMdsHeartbeatKey(key)) << fmt::format("invalid mds heartbeat key({}).", Helper::StringToHex(key));

  mds_id = SerialHelper::ReadLong(key.substr(kPrefixSize + 1 + 1 + 1));
}

void MetaCodec::DecodeHeartbeatKey(const std::string& key, std::string& client_id) {
  CHECK(IsClientHeartbeatKey(key)) << fmt::format("invalid client heartbeat key({}).", Helper::StringToHex(key));

  client_id = key.substr(kPrefixSize + 1 + 1 + 1);
}

void MetaCodec::DecodeHeartbeatCacheMemberKey(const std::string& key, std::string& member_id) {
  CHECK(IsCacheMemberHeartbeatKey(key)) << fmt::format("invalid cache_member heartbeat key({}).",
                                                       Helper::StringToHex(key));

  member_id = key.substr(kPrefixSize + 1 + 1 + 1);
}

std::string MetaCodec::EncodeHeartbeatValue(const MdsEntry& mds) { return mds.SerializeAsString(); }

std::string MetaCodec::EncodeHeartbeatValue(const ClientEntry& client) { return client.SerializeAsString(); }

std::string MetaCodec::EncodeHeartbeatValue(const CacheMemberEntry& cache_member) {
  return cache_member.SerializeAsString();
}

MdsEntry MetaCodec::DecodeHeartbeatMdsValue(const std::string& value) {
  MdsEntry mds;
  CHECK(mds.ParseFromString(value)) << "parse mds heartbeat value fail.";

  return mds;
}

ClientEntry MetaCodec::DecodeHeartbeatClientValue(const std::string& value) {
  ClientEntry client;
  CHECK(client.ParseFromString(value)) << "parse client heartbeat value fail.";

  return client;
}

CacheMemberEntry MetaCodec::DecodeHeartbeatCacheMemberValue(const std::string& value) {
  CacheMemberEntry cache_member;
  CHECK(cache_member.ParseFromString(value)) << "parse cache_member heartbeat value fail.";

  return cache_member;
}

// fs format: ${prefix} kTableMeta kMetaFs {name}
static const uint32_t kFsKeyHeaderSize = kPrefixSize + 2;

bool MetaCodec::IsFsKey(const std::string& key) {
  if (key.size() <= kFsKeyHeaderSize) {
    return false;
  }

  if (key.at(kPrefixSize) != kTableMeta || key.at(kPrefixSize + 1) != kMetaFs) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeFsKey(const std::string& name) {
  std::string key;
  key.reserve(kFsKeyHeaderSize + name.size());

  key.append(kPrefix);
  key.push_back(kTableMeta);
  key.push_back(kMetaFs);
  key.append(name);

  return key;
}

void MetaCodec::DecodeFsKey(const std::string& key, std::string& name) {
  CHECK(IsFsKey(key)) << fmt::format("invalid fs key({}).", Helper::StringToHex(key));

  name = key.substr(kFsKeyHeaderSize);
}

std::string MetaCodec::EncodeFsValue(const FsInfoEntry& fs_info) { return fs_info.SerializeAsString(); }

FsInfoEntry MetaCodec::DecodeFsValue(const std::string& value) {
  FsInfoEntry fs_info;
  CHECK(fs_info.ParseFromString(value)) << "parse fs info fail.";

  return fs_info;
}

// fs quota format: ${prefix} kTableMeta kMetaFsQuota {fs_id}
static const uint32_t kFsQuotaKeySize = kPrefixSize + 1 + 1 + 4;

bool MetaCodec::IsFsQuotaKey(const std::string& key) {
  if (key.size() != kFsQuotaKeySize) {
    return false;
  }

  if (key.at(kPrefixSize) != kTableMeta || key.at(kPrefixSize + 1) != kMetaFsQuota) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeFsQuotaKey(uint32_t fs_id) {
  std::string key;
  key.reserve(kFsQuotaKeySize);

  key.append(kPrefix);
  key.push_back(kTableMeta);
  key.push_back(kMetaFsQuota);
  SerialHelper::WriteInt(fs_id, key);

  return key;
}

void MetaCodec::DecodeFsQuotaKey(const std::string& key, uint32_t& fs_id) {
  CHECK(IsFsQuotaKey(key)) << fmt::format("invalid fs quota key({}).", Helper::StringToHex(key));

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1 + 1));
}

std::string MetaCodec::EncodeFsQuotaValue(const QuotaEntry& quota) { return quota.SerializeAsString(); }

QuotaEntry MetaCodec::DecodeFsQuotaValue(const std::string& value) {
  QuotaEntry quota;
  CHECK(quota.ParseFromString(value)) << "parse fs quota fail.";

  return quota;
}

// fs config log format: ${prefix} kTableMeta kMetaFsOpLog {fs_id} {time_ns}
static const uint32_t kFsConfigLogKeySize = kPrefixSize + 1 + 1 + 4 + 8;

bool MetaCodec::IsFsOpLogKey(const std::string& key) {
  if (key.size() != kFsConfigLogKeySize) {
    return false;
  }

  if (key.at(kPrefixSize) != kTableMeta || key.at(kPrefixSize + 1) != kMetaFsOpLog) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeFsOpLogKey(uint32_t fs_id, uint64_t time_ns) {
  std::string key;
  key.reserve(kFsConfigLogKeySize);

  key.append(kPrefix);
  key.push_back(kTableMeta);
  key.push_back(kMetaFsOpLog);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteULong(time_ns, key);

  return key;
}

void MetaCodec::DecodeFsOpLogKey(const std::string& key, uint32_t& fs_id, uint64_t& time_ns) {
  CHECK(IsFsOpLogKey(key)) << fmt::format("invalid fs config log key({}).", Helper::StringToHex(key));

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1));
  time_ns = SerialHelper::ReadULong(key.substr(kPrefixSize + 1 + 1 + 4));
}

std::string MetaCodec::EncodeFsOpLogValue(const FsOpLog& entry) { return entry.SerializeAsString(); }

FsOpLog MetaCodec::DecodeFsOpLogValue(const std::string& value) {
  FsOpLog entry;
  CHECK(entry.ParseFromString(value)) << "parse fs config log fail.";

  return entry;
}

// inode attr format: ${prefix} kTableFsMeta {fs_id} kMetaFsInode {ino} kFsInodeAttr
static const uint32_t kInodeKeySize = kPrefixSize + 1 + 4 + 1 + 8 + 1;

bool MetaCodec::IsInodeKey(const std::string& key) {
  if (key.size() != kInodeKeySize) {
    return false;
  }

  // Check the prefix, table id, and meta type
  if (key.at(kPrefixSize) != kTableFsMeta || key.at(kPrefixSize + 1 + 4) != kMetaFsInode) {
    return false;
  }

  // Check the inode type
  if (key.at(key.size() - 1) != kFsInodeAttr) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeInodeKey(uint32_t fs_id, Ino ino) {
  std::string key;
  key.reserve(kInodeKeySize);

  key.append(kPrefix);
  key.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kMetaFsInode);
  SerialHelper::WriteULong(ino, key);
  key.push_back(kFsInodeAttr);

  return key;
}

void MetaCodec::DecodeInodeKey(const std::string& key, uint32_t& fs_id, uint64_t& ino) {
  CHECK(IsInodeKey(key)) << fmt::format("invalid inode key({}).", Helper::StringToHex(key));

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1));
  ino = SerialHelper::ReadULong(key.substr(kPrefixSize + 1 + 4 + 1));
}

std::string MetaCodec::EncodeInodeValue(const AttrEntry& attr) { return attr.SerializeAsString(); }

AttrEntry MetaCodec::DecodeInodeValue(const std::string& value) {
  AttrEntry attr;
  CHECK(attr.ParseFromString(value)) << "parse inode attr fail.";

  return attr;
}

// dentry format: ${prefix} kTableFsMeta {fs_id} kMetaFsInode {ino} kFsInodeDentry {name}
static const uint32_t kDentryKeyHeaderSize = kPrefixSize + 1 + 4 + 1 + 8 + 1;

bool MetaCodec::IsDentryKey(const std::string& key) {
  if (key.size() <= kDentryKeyHeaderSize) {
    return false;
  }

  // Check the prefix, table id, and meta type
  if (key.at(kPrefixSize) != kTableFsMeta || key.at(kPrefixSize + 1 + 4) != kMetaFsInode) {
    return false;
  }

  // Check the inode type
  if (key.at(kDentryKeyHeaderSize - 1) != kFsInodeDentry) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeDentryKey(uint32_t fs_id, Ino ino, const std::string& name) {
  std::string key;
  key.reserve(kDentryKeyHeaderSize + name.size());

  key.append(kPrefix);
  key.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kMetaFsInode);
  SerialHelper::WriteULong(ino, key);
  key.push_back(kFsInodeDentry);
  key.append(name);

  return key;
}

void MetaCodec::DecodeDentryKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, std::string& name) {
  CHECK(IsDentryKey(key)) << fmt::format("invalid dentry key({}).", Helper::StringToHex(key));

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1));
  ino = SerialHelper::ReadULong(key.substr(kPrefixSize + 1 + 4 + 1));

  name = key.substr(kDentryKeyHeaderSize);
}

std::string MetaCodec::EncodeDentryValue(const DentryEntry& dentry) { return dentry.SerializeAsString(); }

DentryEntry MetaCodec::DecodeDentryValue(const std::string& value) {
  DentryEntry dentry;
  CHECK(dentry.ParseFromString(value)) << "parse dentry fail.";

  return dentry;
}

// inode chunk format: ${prefix} kTableFsMeta  {fs_id}  kMetaFsInode {ino} kFsInodeChunk {chunk_index}
static const uint32_t kChunkKeySize = kPrefixSize + 1 + 4 + 1 + 8 + 1 + 8;

bool MetaCodec::IsChunkKey(const std::string& key) {
  if (key.size() != kChunkKeySize) {
    return false;
  }

  // Check the prefix, table id, and meta type
  if (key.at(kPrefixSize) != kTableFsMeta || key.at(kPrefixSize + 1 + 4) != kMetaFsInode) {
    return false;
  }

  // Check the inode type
  if (key.at(kChunkKeySize - 8 - 1) != kFsInodeChunk) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeChunkKey(uint32_t fs_id, Ino ino, uint64_t chunk_index) {
  std::string key;
  key.reserve(kChunkKeySize);

  key.append(kPrefix);
  key.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kMetaFsInode);
  SerialHelper::WriteULong(ino, key);
  key.push_back(kFsInodeChunk);
  SerialHelper::WriteULong(chunk_index, key);

  return key;
}

void MetaCodec::DecodeChunkKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, uint64_t& chunk_index) {
  CHECK(IsChunkKey(key)) << fmt::format("invalid chunk key({}).", Helper::StringToHex(key));

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1));
  ino = SerialHelper::ReadULong(key.substr(kPrefixSize + 1 + 4 + 1));
  chunk_index = SerialHelper::ReadULong(key.substr(kChunkKeySize - 8));
}

std::string MetaCodec::EncodeChunkValue(const ChunkEntry& chunk) { return chunk.SerializeAsString(); }

ChunkEntry MetaCodec::DecodeChunkValue(const std::string& value) {
  ChunkEntry chunk;
  CHECK(chunk.ParseFromString(value)) << "parse chunk fail.";

  return chunk;
}

// inode file session format: ${prefix} kTableFsMeta {fs_id} kMetaFsFileSession {ino} {session_id}
static const uint32_t kFileSessionKeySize = kPrefixSize + 1 + 4 + 1 + 8 + 36;

bool MetaCodec::IsFileSessionKey(const std::string& key) {
  if (key.size() != kFileSessionKeySize) {
    return false;
  }

  // Check the prefix, table id, and meta type
  if (key.at(kPrefixSize) != kTableFsMeta || key.at(kPrefixSize + 1 + 4) != kMetaFsFileSession) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeFileSessionKey(uint32_t fs_id, Ino ino, const std::string& session_id) {
  std::string key;
  key.reserve(kFileSessionKeySize);

  key.append(kPrefix);
  key.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kMetaFsFileSession);
  SerialHelper::WriteULong(ino, key);
  key.append(session_id);

  return key;
}

void MetaCodec::DecodeFileSessionKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, std::string& session_id) {
  CHECK(IsFileSessionKey(key)) << fmt::format("invalid file session key({}).", Helper::StringToHex(key));

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1));
  ino = SerialHelper::ReadULong(key.substr(kPrefixSize + 1 + 4 + 1));
  session_id = key.substr(kFileSessionKeySize - 36);
}

std::string MetaCodec::EncodeFileSessionValue(const FileSessionEntry& file_session) {
  return file_session.SerializeAsString();
}

FileSessionEntry MetaCodec::DecodeFileSessionValue(const std::string& value) {
  FileSessionEntry file_session;
  CHECK(file_session.ParseFromString(value)) << "parse file session fail.";

  return file_session;
}

// dir quota format: ${prefix} kTableFsMeta {fs_id} kMetaFsDirQuota {ino}
static const uint32_t kDirQuotaKeySize = kPrefixSize + 1 + 4 + 1 + 8;

bool MetaCodec::IsDirQuotaKey(const std::string& key) {
  if (key.size() != kDirQuotaKeySize) {
    return false;
  }
  // Check the prefix, table id, and meta type
  if (key.at(kPrefixSize) != kTableFsMeta || key.at(kPrefixSize + 1 + 4) != kMetaFsDirQuota) {
    return false;
  }
  return true;
}

std::string MetaCodec::EncodeDirQuotaKey(uint32_t fs_id, Ino ino) {
  std::string key;
  key.reserve(kDirQuotaKeySize);

  key.append(kPrefix);
  key.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kMetaFsDirQuota);
  SerialHelper::WriteULong(ino, key);

  return key;
}

void MetaCodec::DecodeDirQuotaKey(const std::string& key, uint32_t& fs_id, Ino& ino) {
  CHECK(IsDirQuotaKey(key)) << fmt::format("invalid dir quota key({}).", Helper::StringToHex(key));

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1));
  ino = SerialHelper::ReadULong(key.substr(kPrefixSize + 1 + 4 + 1));
}

std::string MetaCodec::EncodeDirQuotaValue(const QuotaEntry& dir_quota) { return dir_quota.SerializeAsString(); }

QuotaEntry MetaCodec::DecodeDirQuotaValue(const std::string& value) {
  QuotaEntry dir_quota;
  CHECK(dir_quota.ParseFromString(value)) << "parse dir quota fail.";

  return dir_quota;
}

// inode delslice format: ${prefix} kTableFsMeta {fs_id} kMetaFsDelSlice {ino} {chunk_index} {time_ns}
static const uint32_t kDelSliceKeySize = kPrefixSize + 1 + 4 + 1 + 8 + 8 + 8;

bool MetaCodec::IsDelSliceKey(const std::string& key) {
  if (key.size() != kDelSliceKeySize) {
    return false;
  }

  // Check the prefix, table id, and meta type
  if (key.at(kPrefixSize) != kTableFsMeta || key.at(kPrefixSize + 1 + 4) != kMetaFsDelSlice) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeDelSliceKey(uint32_t fs_id, Ino ino, uint64_t chunk_index, uint64_t time_ns) {
  std::string key;
  key.reserve(kDelSliceKeySize);

  key.append(kPrefix);
  key.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kMetaFsDelSlice);
  SerialHelper::WriteULong(ino, key);
  SerialHelper::WriteULong(chunk_index, key);
  SerialHelper::WriteULong(time_ns, key);

  return key;
}

void MetaCodec::DecodeDelSliceKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, uint64_t& chunk_index,
                                  uint64_t& time_ns) {
  CHECK(IsDelSliceKey(key)) << fmt::format("invalid del slice key({}).", Helper::StringToHex(key));

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1));
  ino = SerialHelper::ReadULong(key.substr(kPrefixSize + 1 + 4 + 1));
  chunk_index = SerialHelper::ReadULong(key.substr(kPrefixSize + 1 + 4 + 1 + 8));
  time_ns = SerialHelper::ReadULong(key.substr(kPrefixSize + 1 + 4 + 1 + 8 + 8));
}

std::string MetaCodec::EncodeDelSliceValue(const TrashSliceList& slice_list) { return slice_list.SerializeAsString(); }

TrashSliceList MetaCodec::DecodeDelSliceValue(const std::string& value) {
  TrashSliceList slice_list;
  CHECK(slice_list.ParseFromString(value)) << "parse del slice fail.";

  return slice_list;
}

// inode delfile format: ${prefix} kTableFsMeta {fs_id} kMetaFsDelFile {ino}
static const uint32_t kDelFileKeySize = kPrefixSize + 1 + 4 + 1 + 8;

bool MetaCodec::IsDelFileKey(const std::string& key) {
  if (key.size() != kDelFileKeySize) {
    return false;
  }

  // Check the prefix, table id, and meta type
  if (key.at(kPrefixSize) != kTableFsMeta || key.at(kPrefixSize + 1 + 4) != kMetaFsDelFile) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeDelFileKey(uint32_t fs_id, Ino ino) {
  std::string key;
  key.reserve(kDelFileKeySize);

  key.append(kPrefix);
  key.push_back(kTableFsMeta);
  SerialHelper::WriteInt(fs_id, key);
  key.push_back(kMetaFsDelFile);
  SerialHelper::WriteULong(ino, key);

  return key;
}

void MetaCodec::DecodeDelFileKey(const std::string& key, uint32_t& fs_id, Ino& ino) {
  CHECK(IsDelFileKey(key)) << fmt::format("invalid del file key({}).", Helper::StringToHex(key));

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1));
  ino = SerialHelper::ReadULong(key.substr(kPrefixSize + 1 + 4 + 1));
}

std::string MetaCodec::EncodeDelFileValue(const AttrEntry& attr) { return attr.SerializeAsString(); }

AttrEntry MetaCodec::DecodeDelFileValue(const std::string& value) {
  AttrEntry attr;
  CHECK(attr.ParseFromString(value)) << "parse del file attr fail.";

  return attr;
}

// fs stats format: ${prefix} kTableFsStats kMetaFsStats {fs_id} {time_ns}
static const uint32_t kFsStatsKeySize = kPrefixSize + 2 + 4 + 8;

bool MetaCodec::IsFsStatsKey(const std::string& key) {
  if (key.size() != kFsStatsKeySize) {
    return false;
  }

  // Check the prefix, table id, and meta type
  if (key.at(kPrefixSize) != kTableFsStats || key.at(kPrefixSize + 1) != kMetaFsStats) {
    return false;
  }

  return true;
}

std::string MetaCodec::EncodeFsStatsKey(uint32_t fs_id, uint64_t time_ns) {
  std::string key;
  key.reserve(kFsStatsKeySize);

  key.append(kPrefix);
  key.push_back(kTableFsStats);
  key.push_back(kMetaFsStats);
  SerialHelper::WriteInt(fs_id, key);
  SerialHelper::WriteULong(time_ns, key);

  return key;
}

void MetaCodec::DecodeFsStatsKey(const std::string& key, uint32_t& fs_id, uint64_t& time_ns) {
  CHECK(IsFsStatsKey(key)) << fmt::format("invalid fs stats key({}).", Helper::StringToHex(key));

  fs_id = SerialHelper::ReadInt(key.substr(kPrefixSize + 1 + 1));
  time_ns = SerialHelper::ReadULong(key.substr(kPrefixSize + 1 + 1 + 4));
}

std::string MetaCodec::EncodeFsStatsValue(const FsStatsDataEntry& stats) { return stats.SerializeAsString(); }

FsStatsDataEntry MetaCodec::DecodeFsStatsValue(const std::string& value) {
  FsStatsDataEntry stats;
  CHECK(stats.ParseFromString(value)) << "parse fs stats fail.";

  return stats;
}

bool MetaCodec::IsMetaTableKey(const std::string& key) {
  if (key.size() <= kPrefixSize + 1) {
    return false;
  }

  if (key.substr(0, kPrefixSize) != kPrefix || key.at(kPrefixSize) != kTableMeta) {
    return false;
  }

  return true;
}

bool MetaCodec::IsFsStatsTableKey(const std::string& key) {
  if (key.size() <= kPrefixSize + 1) {
    return false;
  }

  if (key.substr(0, kPrefixSize) != kPrefix || key.at(kPrefixSize) != kTableFsStats) {
    return false;
  }

  return true;
}

bool MetaCodec::IsFsMetaTableKey(const std::string& key) {
  if (key.size() <= kPrefixSize + 1) {
    return false;
  }

  if (key.substr(0, kPrefixSize) != kPrefix || key.at(kPrefixSize) != kTableFsMeta) {
    return false;
  }

  return true;
}

std::pair<std::string, std::string> MetaCodec::ParseMetaTableKey(const std::string& key, const std::string& value) {
  std::string key_desc, value_desc;

  MetaType meta_type = static_cast<MetaType>(key.at(kPrefixSize + 1));
  switch (meta_type) {
    case kMetaLock: {
      std::string name;
      DecodeLockKey(key, name);

      key_desc = fmt::format("{} kTableMeta kMetaLock {}", kPrefix, name);

      int64_t mds_id;
      uint64_t epoch;
      uint64_t expire_time_ms;
      DecodeLockValue(value, mds_id, epoch, expire_time_ms);

      value_desc = fmt::format("{} {} {}", mds_id, epoch, expire_time_ms);
    } break;

    case kMetaAutoIncrementID: {
      std::string name;
      DecodeAutoIncrementIDKey(key, name);

      key_desc = fmt::format("{} kTableMeta kMetaAutoIncrementID {}", kPrefix, name);

      uint64_t next_id;
      DecodeAutoIncrementIDValue(value, next_id);

      value_desc = fmt::format("{} {}", next_id);
    } break;

    case kMetaHeartbeat: {
      pb::mds::Role role = static_cast<pb::mds::Role>(key.at(kPrefixSize + 2));
      if (role == pb::mds::ROLE_MDS) {
        int64_t mds_id;
        DecodeHeartbeatKey(key, mds_id);

        key_desc = fmt::format("{} kTableMeta kMetaHeartbeat kRoleMds {}", kPrefix, mds_id);

        auto mds_info = DecodeHeartbeatMdsValue(value);
        value_desc = mds_info.ShortDebugString();

      } else if (role == pb::mds::ROLE_CLIENT) {
        std::string client_id;
        DecodeHeartbeatKey(key, client_id);

        key_desc = fmt::format("{} kTableMeta kMetaHeartbeat kRoleClient {}", kPrefix, client_id);

        auto client_info = DecodeHeartbeatClientValue(value);
        value_desc = client_info.ShortDebugString();

      } else if (role == pb::mds::ROLE_CACHE_MEMBER) {
        std::string member_id;
        DecodeHeartbeatCacheMemberKey(key, member_id);

        key_desc = fmt::format("{} kTableMeta kMetaHeartbeat kRoleCacheMember {}", kPrefix, member_id);

        auto cache_member = DecodeHeartbeatCacheMemberValue(value);
        value_desc = cache_member.ShortDebugString();
      }

    } break;

    case kMetaFs: {
      std::string name;
      DecodeFsKey(key, name);
      key_desc = fmt::format("{} kTableMeta kMetaFs {}", kPrefix, name);

      auto fs_info = DecodeFsValue(value);
      value_desc = fs_info.ShortDebugString();

    } break;

    case kMetaFsQuota: {
      uint32_t fs_id;
      DecodeFsQuotaKey(key, fs_id);
      key_desc = fmt::format("{} kTableMeta kMetaFsQuota {}", kPrefix, fs_id);

      auto quota = DecodeFsQuotaValue(value);
      value_desc = quota.ShortDebugString();

    } break;

    case kMetaFsOpLog: {
      uint32_t fs_id;
      uint64_t time_ns;
      DecodeFsOpLogKey(key, fs_id, time_ns);
      key_desc = fmt::format("{} kTableMeta kMetaFsOpLog {} {}", kPrefix, fs_id, time_ns);

      auto op_log = DecodeFsOpLogValue(value);
      value_desc = op_log.ShortDebugString();
    } break;

    default:
      CHECK(false) << fmt::format("invalid meta type({}) key({}).", static_cast<int>(meta_type),
                                  Helper::StringToHex(key));
  }

  return std::make_pair(std::move(key_desc), std::move(value_desc));
}

std::pair<std::string, std::string> MetaCodec::ParseFsStatsTableKey(const std::string& key, const std::string& value) {
  CHECK(IsFsStatsKey(key)) << fmt::format("invalid fs stats key({}).", Helper::StringToHex(key));

  std::string key_desc, value_desc;

  uint32_t fs_id;
  uint64_t time_ns;
  DecodeFsStatsKey(key, fs_id, time_ns);
  key_desc = fmt::format("{} kTableFsStats kMetaFsStats {} {}", kPrefix, fs_id, time_ns);

  auto fs_stats = DecodeFsStatsValue(value);
  value_desc = fs_stats.ShortDebugString();

  return std::make_pair(std::move(key_desc), std::move(value_desc));
}

std::pair<std::string, std::string> MetaCodec::ParseFsMetaTableKey(const std::string& key, const std::string& value) {
  CHECK(IsFsMetaTableKey(key)) << fmt::format("invalid fs meta key({}).", Helper::StringToHex(key));

  std::string key_desc, value_desc;

  MetaType meta_type = static_cast<MetaType>(key.at(kPrefixSize + 1 + 4));
  switch (meta_type) {
    case kMetaFsInode: {
      FsInodeType inode_type = static_cast<FsInodeType>(key.at(kPrefixSize + 1 + 4 + 1 + 8));
      switch (inode_type) {
        case kFsInodeAttr: {
          uint32_t fs_id;
          Ino ino;
          DecodeInodeKey(key, fs_id, ino);

          key_desc = fmt::format("{} kTableFsMeta {} kMetaFsInode {} kFsInodeAttr", kPrefix, fs_id, ino);

          auto attr = DecodeInodeValue(value);
          value_desc = attr.ShortDebugString();
        } break;

        case kFsInodeDentry: {
          uint32_t fs_id;
          Ino ino;
          std::string name;
          DecodeDentryKey(key, fs_id, ino, name);

          key_desc = fmt::format("{} kTableFsMeta {} kMetaFsInode {} kFsInodeDentry {}", kPrefix, fs_id, ino, name);

          auto dentry = DecodeDentryValue(value);
          value_desc = dentry.ShortDebugString();
        } break;

        case kFsInodeChunk: {
          uint32_t fs_id;
          Ino ino;
          uint64_t chunk_index;
          DecodeChunkKey(key, fs_id, ino, chunk_index);

          key_desc =
              fmt::format("{} kTableFsMeta {} kMetaFsInode {} kFsInodeChunk {}", kPrefix, fs_id, ino, chunk_index);

          auto chunk = DecodeChunkValue(value);
          value_desc = chunk.ShortDebugString();
        } break;

        default:
          CHECK(false) << fmt::format("invalid inode type({}) key({}).", static_cast<int>(inode_type),
                                      Helper::StringToHex(key));
      }
    } break;

    case kMetaFsFileSession: {
      uint32_t fs_id;
      Ino ino;
      std::string session_id;
      DecodeFileSessionKey(key, fs_id, ino, session_id);

      key_desc = fmt::format("{} kTableFsMeta {} kMetaFsFileSession {} {}", kPrefix, fs_id, ino, session_id);

      auto file_session = DecodeFileSessionValue(value);
      value_desc = file_session.ShortDebugString();
    } break;

    case kMetaFsDirQuota: {
      uint32_t fs_id;
      Ino ino;
      DecodeDirQuotaKey(key, fs_id, ino);

      key_desc = fmt::format("{} kTableFsMeta {} kMetaFsDirQuota {}", kPrefix, fs_id, ino);

      auto dir_quota = DecodeDirQuotaValue(value);
      value_desc = dir_quota.ShortDebugString();

    } break;
    case kMetaFsDelSlice: {
      uint32_t fs_id;
      Ino ino;
      uint64_t chunk_index;
      uint64_t time_ns;
      DecodeDelSliceKey(key, fs_id, ino, chunk_index, time_ns);

      key_desc = fmt::format("{} kTableFsMeta {} kMetaFsDelSlice {} {} {}", kPrefix, fs_id, ino, chunk_index, time_ns);

      auto del_slice = DecodeDelSliceValue(value);
      value_desc = del_slice.ShortDebugString();
    } break;

    case kMetaFsDelFile: {
      uint32_t fs_id;
      Ino ino;
      DecodeDelFileKey(key, fs_id, ino);

      key_desc = fmt::format("{} kTableFsMeta {} kMetaFsDelFile {}", kPrefix, fs_id, ino);

      auto del_file = DecodeDelFileValue(value);
      value_desc = del_file.ShortDebugString();
    } break;

    default:
      CHECK(false) << fmt::format("invalid meta type({}) key({}).", static_cast<int>(meta_type),
                                  Helper::StringToHex(key));
  }

  return std::make_pair(std::move(key_desc), std::move(value_desc));
}

std::pair<std::string, std::string> MetaCodec::ParseKey(const std::string& key, const std::string& value) {
  TableID table_id = static_cast<TableID>(key.at(kPrefixSize));
  switch (table_id) {
    case kTableMeta:
      return ParseMetaTableKey(key, value);
    case kTableFsStats:

      return ParseFsStatsTableKey(key, value);
    case kTableFsMeta:
      return ParseFsMetaTableKey(key, value);

    default:
      CHECK(false) << fmt::format("invalid table id({}) key({}).", static_cast<int>(table_id),
                                  Helper::StringToHex(key));
  }

  return {};
}

}  // namespace mds
}  // namespace dingofs
