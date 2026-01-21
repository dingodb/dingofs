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

#ifndef DINGOFS_MDS_FILESYSTEM_CODEC_H_
#define DINGOFS_MDS_FILESYSTEM_CODEC_H_

#include <sys/types.h>

#include <cstdint>
#include <string>
#include <utility>

#include "mds/common/type.h"

namespace dingofs {
namespace mds {

class MetaCodec {
 public:
  static void SetClusterID(uint32_t cluster_id);
  static uint32_t GetClusterID();

  // format: ${prefix} kTableMeta
  static Range GetMetaTableRange();
  // format: ${prefix} kTableFsStats
  static Range GetFsStatsTableRange();
  // format: ${prefix} kTableFsMeta {fs_id}
  static Range GetFsMetaTableRange(uint32_t fs_id);

  // format: ${prefix} kTableMeta kMetaLock
  static Range GetLockRange();
  // format: ${prefix} kTableMeta kMetaAutoIncrement
  static Range GetAutoIncrementIDRange();
  // format: ${prefix} kTableMeta kMetaHeartbeat kRoleMds
  static Range GetHeartbeatMdsRange();
  // format: ${prefix} kTableMeta kMetaHeartbeat kRoleClient
  static Range GetHeartbeatClientRange();
  // format: ${prefix} kTableMeta kMetaHeartbeat KRoleCacheMember
  static Range GetHeartbeatCacheMemberRange();

  // format: ${prefix} kTableMeta kMetaFs
  static Range GetFsRange();
  // format: ${prefix} kTableMeta kMetaFsQuota
  static Range GetFsQuotaRange();

  // format: ${prefix} kTableMeta kMetaFsOpLog {fs_id} {time_ns}
  static Range GetFsConfigLogRange(uint32_t fs_id);

  // format: ${prefix} kTableFsMeta {fs_id} kMetaFsInode {ino} kFsInodeDentry
  static Range GetDentryRange(uint32_t fs_id, Ino ino, bool include_parent);
  // format: ${prefix} kTableFsMeta {fs_id} kMetaFsInode {ino} kFsInodeChunk
  static Range GetChunkRange(uint32_t fs_id, Ino ino);
  // format: ${prefix} kTableFsMeta {fs_id} kMetaFsFileSession {ino}
  static Range GetFileSessionRange(uint32_t fs_id);
  static Range GetFileSessionRange(uint32_t fs_id, Ino ino);

  // format: ${prefix} kTableFsMeta {fs_id} kMetaDirQuota
  static Range GetDirQuotaRange(uint32_t fs_id);

  // format: ${prefix} kTableFsMeta {fs_id} kMetaFsDelSlice
  static Range GetDelSliceRange(uint32_t fs_id);
  // format: ${prefix} kTableFsMeta {fs_id} kMetaFsDelSlice {ino}
  static Range GetDelSliceRange(uint32_t fs_id, Ino ino);
  // format: ${prefix} kTableFsMeta {fs_id} kMetaFsDelSlice {ino} {chunk_index}
  static Range GetDelSliceRange(uint32_t fs_id, Ino ino, uint64_t chunk_index);

  // format: ${prefix} kTableFsMeta {fs_id} kMetaFsDelFile
  static Range GetDelFileTableRange(uint32_t fs_id);

  // format: ${prefix} kTableFsStats kMetaFsStats
  static Range GetFsStatsRange();
  // format: ${prefix} kTableFsStats kMetaFsStats {fs_id}
  static Range GetFsStatsRange(uint32_t fs_id);

  // format: ${prefix} kTableFsMeta {fs_id} kMetaFsTinyFileData
  static Range GetTinyFileDataRange(uint32_t fs_id);

  // lock format: ${prefix} kTableMeta kMetaLock {name}
  static bool IsLockKey(const std::string& key);
  static std::string EncodeLockKey(const std::string& name);
  static void DecodeLockKey(const std::string& key, std::string& name);
  static std::string EncodeLockValue(int64_t mds_id, uint64_t epoch, uint64_t expire_time_ms);
  static void DecodeLockValue(const std::string& value, int64_t& mds_id, uint64_t& epoch, uint64_t& expire_time_ms);

  // auto increment id format: ${prefix} kTableMeta kMetaAutoIncrement {name}
  static bool IsAutoIncrementIDKey(const std::string& key);
  static std::string EncodeAutoIncrementIDKey(const std::string& name);
  static void DecodeAutoIncrementIDKey(const std::string& key, std::string& name);
  static std::string EncodeAutoIncrementIDValue(uint64_t id);
  static void DecodeAutoIncrementIDValue(const std::string& value, uint64_t& id);

  // heartbeat(mds) format: ${prefix} kTableMeta kMetaHeartbeat kRoleMds {mds_id}
  // heartbeat(client) format: ${prefix} kTableMeta kMetaHeartbeat kRoleClient {client_id}
  // heartbeat(client) format: ${prefix} kTableMeta kMetaHeartbeat kRoleCacheMember {member_id}
  static bool IsMdsHeartbeatKey(const std::string& key);
  static bool IsClientHeartbeatKey(const std::string& key);
  static bool IsCacheMemberHeartbeatKey(const std::string& key);
  static std::string EncodeHeartbeatKey(int64_t mds_id);
  static std::string EncodeHeartbeatKey(const std::string& client_id);
  static std::string EncodeHeartbeatCacheMemberKey(const std::string& member_id);
  static void DecodeHeartbeatKey(const std::string& key, int64_t& mds_id);
  static void DecodeHeartbeatKey(const std::string& key, std::string& client_id);
  static void DecodeHeartbeatCacheMemberKey(const std::string& key, std::string& member_id);
  static std::string EncodeHeartbeatValue(const MdsEntry& mds);
  static std::string EncodeHeartbeatValue(const ClientEntry& client);
  static std::string EncodeHeartbeatValue(const CacheMemberEntry& cache_member);
  static MdsEntry DecodeHeartbeatMdsValue(const std::string& value);
  static ClientEntry DecodeHeartbeatClientValue(const std::string& value);
  static CacheMemberEntry DecodeHeartbeatCacheMemberValue(const std::string& value);

  // fs format: ${prefix} kTableMeta kMetaFs {name}
  static bool IsFsKey(const std::string& key);
  static std::string EncodeFsKey(const std::string& name);
  static void DecodeFsKey(const std::string& key, std::string& name);
  static std::string EncodeFsValue(const FsInfoEntry& fs_info);
  static FsInfoEntry DecodeFsValue(const std::string& value);

  // fs quota format: ${prefix} kTableMeta kMetaFsQuota {fs_id}
  static bool IsFsQuotaKey(const std::string& key);
  static std::string EncodeFsQuotaKey(uint32_t fs_id);
  static void DecodeFsQuotaKey(const std::string& key, uint32_t& fs_id);
  static std::string EncodeFsQuotaValue(const QuotaEntry& quota);
  static QuotaEntry DecodeFsQuotaValue(const std::string& value);

  // fs config log format: ${prefix} kTableMeta kMetaFsOpLog {fs_id} {time_ns}
  static bool IsFsOpLogKey(const std::string& key);
  static std::string EncodeFsOpLogKey(uint32_t fs_id, uint64_t time_ns);
  static void DecodeFsOpLogKey(const std::string& key, uint32_t& fs_id, uint64_t& time_ns);
  static std::string EncodeFsOpLogValue(const FsOpLog& entry);
  static FsOpLog DecodeFsOpLogValue(const std::string& value);

  // inode attr format: ${prefix} kTableFsMeta {fs_id} kMetaFsInode {ino} kFsInodeAttr
  static bool IsInodeKey(const std::string& key);
  static std::string EncodeInodeKey(uint32_t fs_id, Ino ino);
  static void DecodeInodeKey(const std::string& key, uint32_t& fs_id, uint64_t& ino);
  static std::string EncodeInodeValue(const AttrEntry& attr);
  static AttrEntry DecodeInodeValue(const std::string& value);

  // dentry format: ${prefix} kTableFsMeta {fs_id} kMetaFsInode {ino} kFsInodeDentry {name}
  static bool IsDentryKey(const std::string& key);
  static std::string EncodeDentryKey(uint32_t fs_id, Ino ino, const std::string& name);
  static void DecodeDentryKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, std::string& name);
  static std::string EncodeDentryValue(const DentryEntry& dentry);
  static DentryEntry DecodeDentryValue(const std::string& value);

  // inode chunk format: ${prefix} kTableFsMeta {fs_id} kMetaFsInode {ino} kFsInodeChunk {chunk_index}
  static bool IsChunkKey(const std::string& key);
  static std::string EncodeChunkKey(uint32_t fs_id, Ino ino, uint64_t chunk_index);
  static void DecodeChunkKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, uint64_t& chunk_index);
  static std::string EncodeChunkValue(const ChunkEntry& chunk);
  static ChunkEntry DecodeChunkValue(const std::string& value);

  // inode file session format: ${prefix} kTableFsMeta {fs_id} kMetaFsFileSession {ino} {session_id}
  static bool IsFileSessionKey(const std::string& key);
  static std::string EncodeFileSessionKey(uint32_t fs_id, Ino ino, const std::string& session_id);
  static void DecodeFileSessionKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, std::string& session_id);
  static std::string EncodeFileSessionValue(const FileSessionEntry& file_session);
  static FileSessionEntry DecodeFileSessionValue(const std::string& value);

  // dir quota format: ${prefix} kTableFsMeta {fs_id} kMetaFsDirQuota {ino}
  static bool IsDirQuotaKey(const std::string& key);
  static std::string EncodeDirQuotaKey(uint32_t fs_id, Ino ino);
  static void DecodeDirQuotaKey(const std::string& key, uint32_t& fs_id, Ino& ino);
  static std::string EncodeDirQuotaValue(const QuotaEntry& dir_quota);
  static QuotaEntry DecodeDirQuotaValue(const std::string& value);

  // inode delslice format: ${prefix} kTableFsMeta {fs_id} kMetaFsDelSlice {ino} {chunk_index} {time_ns}
  static bool IsDelSliceKey(const std::string& key);
  static std::string EncodeDelSliceKey(uint32_t fs_id, Ino ino, uint64_t chunk_index, uint64_t time_ns);
  static void DecodeDelSliceKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, uint64_t& chunk_index,
                                uint64_t& time_ns);
  static std::string EncodeDelSliceValue(const TrashSliceList& slice_list);
  static TrashSliceList DecodeDelSliceValue(const std::string& value);

  // inode delfile format: ${prefix} kTableFsMeta {fs_id} kMetaFsDelFile {ino}
  static bool IsDelFileKey(const std::string& key);
  static std::string EncodeDelFileKey(uint32_t fs_id, Ino ino);
  static void DecodeDelFileKey(const std::string& key, uint32_t& fs_id, Ino& ino);
  static std::string EncodeDelFileValue(const AttrEntry& attr);
  static AttrEntry DecodeDelFileValue(const std::string& value);

  // fs stats format: ${prefix} kTableFsStats kMetaFsStats {fs_id} {time_ns}
  static bool IsFsStatsKey(const std::string& key);
  static std::string EncodeFsStatsKey(uint32_t fs_id, uint64_t time_ns);
  static void DecodeFsStatsKey(const std::string& key, uint32_t& fs_id, uint64_t& time_ns);
  static std::string EncodeFsStatsValue(const FsStatsDataEntry& stats);
  static FsStatsDataEntry DecodeFsStatsValue(const std::string& value);

  // tiny file data format: ${prefix} kTableFsMeta {fs_id} kMetaFsTinyFileData {ino}
  static bool IsTinyFileDataKey(const std::string& key);
  static std::string EncodeTinyFileDataKey(uint32_t fs_id, Ino ino);
  static void DecodeTinyFileDataKey(const std::string& key, uint32_t& fs_id, Ino& ino);
  static std::string& EncodeTinyFileDataValue(std::string& data, uint64_t version);
  static void DecodeTinyFileDataValue(std::string& value, uint64_t& version);

  // check key belongs to a specific table
  static bool IsMetaTableKey(const std::string& key);
  static bool IsFsStatsTableKey(const std::string& key);
  static bool IsFsMetaTableKey(const std::string& key);

  static std::pair<std::string, std::string> ParseMetaTableKey(const std::string& key, const std::string& value);
  static std::pair<std::string, std::string> ParseFsStatsTableKey(const std::string& key, const std::string& value);
  static std::pair<std::string, std::string> ParseFsMetaTableKey(const std::string& key, const std::string& value);
  static std::pair<std::string, std::string> ParseKey(const std::string& key, const std::string& value);
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FILESYSTEM_CODEC_H_