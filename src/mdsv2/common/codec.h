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

#ifndef DINGOFS_MDV2_FILESYSTEM_CODEC_H_
#define DINGOFS_MDV2_FILESYSTEM_CODEC_H_

#include <cstdint>
#include <string>

#include "dingofs/mdsv2.pb.h"

namespace dingofs {
namespace mdsv2 {

class MetaDataCodec {
 public:
  static void GetLockTableRange(std::string& start_key, std::string& end_key);

  static void GetHeartbeatTableRange(std::string& start_key, std::string& end_key);
  static void GetHeartbeatMdsRange(std::string& start_key, std::string& end_key);
  static void GetHeartbeatClientRange(std::string& start_key, std::string& end_key);

  static void GetFsTableRange(std::string& start_key, std::string& end_key);
  static void GetDentryTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key);
  static void GetFileInodeTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key);

  static void GetQuotaTableRange(std::string& start_key, std::string& end_key);
  static void GetDirQuotaRange(uint32_t fs_id, std::string& start_key, std::string& end_key);

  static void GetFsStatsTableRange(std::string& start_key, std::string& end_key);
  static void GetFsStatsRange(uint32_t fs_id, std::string& start_key, std::string& end_key);

  static void GetFileSessionTableRange(std::string& start_key, std::string& end_key);
  static void GetFsFileSessionRange(uint32_t fs_id, std::string& start_key, std::string& end_key);
  static void GetFileSessionRange(uint32_t fs_id, uint64_t ino, std::string& start_key, std::string& end_key);

  static void GetChunkTableRange(std::string& start_key, std::string& end_key);
  static void GetChunkRange(uint32_t fs_id, uint64_t ino, std::string& start_key, std::string& end_key);
  static void GetChunkRange(uint32_t fs_id, uint64_t ino, uint64_t chunk_index, std::string& start_key,
                            std::string& end_key);
  static void GetTrashChunkTableRange(std::string& start_key, std::string& end_key);
  static void GetTrashChunkRange(uint32_t fs_id, uint64_t ino, std::string& start_key, std::string& end_key);
  static void GetTrashChunkRange(uint32_t fs_id, uint64_t ino, uint64_t chunk_index, std::string& start_key,
                                 std::string& end_key);

  static void GetDelFileTableRange(std::string& start_key, std::string& end_key);

  // lock
  // format: [$prefix, $type, $name]
  static std::string EncodeLockKey(const std::string& name);
  static void DecodeLockKey(const std::string& key, std::string& name);
  static std::string EncodeLockValue(int64_t mds_id, uint64_t expire_time_ms);
  static void DecodeLockValue(const std::string& value, int64_t& mds_id, uint64_t& expire_time_ms);

  // heartbeat
  // format: [$prefix, $type, $role, $mds_id]
  // or format: [$prefix, $type, $role, $client_mountpoint]
  static std::string EncodeHeartbeatKey(int64_t mds_id);
  static std::string EncodeHeartbeatKey(const std::string& client_mountpoint);
  static void DecodeHeartbeatKey(const std::string& key, int64_t& mds_id);
  static void DecodeHeartbeatKey(const std::string& key, std::string& client_mountpoint);
  static std::string EncodeHeartbeatValue(const pb::mdsv2::MDS& mds);
  static std::string EncodeHeartbeatValue(const pb::mdsv2::Client& client);
  static void DecodeHeartbeatValue(const std::string& value, pb::mdsv2::MDS& mds);
  static void DecodeHeartbeatValue(const std::string& value, pb::mdsv2::Client& client);

  // fs
  // format: [$prefix, $type, $name]
  static std::string EncodeFSKey(const std::string& name);
  static void DecodeFSKey(const std::string& key, std::string& name);
  static std::string EncodeFSValue(const pb::mdsv2::FsInfo& fs_info);
  static pb::mdsv2::FsInfo DecodeFSValue(const std::string& value);

  // format: [$prefix, $type, $fs_id, $ino, $name]
  static std::string EncodeDentryKey(uint32_t fs_id, uint64_t ino, const std::string& name);
  static void DecodeDentryKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, std::string& name);
  static std::string EncodeDentryValue(const pb::mdsv2::Dentry& dentry);
  static pb::mdsv2::Dentry DecodeDentryValue(const std::string& value);
  // format: [$prefix, $type, $fs_id, $ino]
  // format: [$prefix, $type, $fs_id, $ino+1]
  static void EncodeDentryRange(uint32_t fs_id, uint64_t ino, std::string& start_key, std::string& end_key);

  // inode
  // format: [$prefix, $type, $fs_id, $ino]
  static uint32_t InodeKeyLength();
  static std::string EncodeInodeKey(uint32_t fs_id, uint64_t ino);
  static void DecodeInodeKey(const std::string& key, uint32_t& fs_id, uint64_t& ino);
  static std::string EncodeInodeValue(const pb::mdsv2::Inode& inode);
  static pb::mdsv2::Inode DecodeInodeValue(const std::string& value);

  // quota encode/decode
  // fs format: [$prefix, $type, $fs_id]
  static std::string EncodeFsQuotaKey(uint32_t fs_id);
  static void DecodeFsQuotaKey(const std::string& key, uint32_t& fs_id);
  static std::string EncodeFsQuotaValue(const pb::mdsv2::Quota& quota);
  static pb::mdsv2::Quota DecodeFsQuotaValue(const std::string& value);

  // dir format: [$prefix, $type, $fs_id, $ino]
  static std::string EncodeDirQuotaKey(uint32_t fs_id, uint64_t ino);
  static void DecodeDirQuotaKey(const std::string& key, uint32_t& fs_id, uint64_t& ino);
  static std::string EncodeDirQuotaValue(const pb::mdsv2::Quota& quota);
  static pb::mdsv2::Quota DecodeDirQuotaValue(const std::string& value);

  // fs stats
  // format: [$prefix, $type, $fs_id, $time_ns]
  static std::string EncodeFsStatsKey(uint32_t fs_id, uint64_t time_ns);
  static void DecodeFsStatsKey(const std::string& key, uint32_t& fs_id, uint64_t& time_ns);
  static std::string EncodeFsStatsValue(const pb::mdsv2::FsStatsData& stats);
  static pb::mdsv2::FsStatsData DecodeFsStatsValue(const std::string& value);

  // file session
  // format: [$prefix, $type, $fs_id, $ino, $session_id]
  static std::string EncodeFileSessionKey(uint32_t fs_id, uint64_t ino, const std::string& session_id);
  static void DecodeFileSessionKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, std::string& session_id);
  static std::string EncodeFileSessionValue(const pb::mdsv2::FileSession& file_session);
  static pb::mdsv2::FileSession DecodeFileSessionValue(const std::string& value);

  // chunk
  // format: [$prefix, $type, $fs_id, $ino, $chunk_index]
  static std::string EncodeChunkKey(uint32_t fs_id, uint64_t ino, uint64_t chunk_index);
  static void DecodeChunkKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, uint64_t& chunk_index);
  static std::string EncodeChunkValue(const pb::mdsv2::Chunk& chunk);
  static pb::mdsv2::Chunk DecodeChunkValue(const std::string& value);

  // trash chunk
  // format: [$prefix, $type, $fs_id, $ino, $chunk_index, $time_ns]
  static std::string EncodeTrashChunkKey(uint32_t fs_id, uint64_t ino, uint64_t chunk_index, uint64_t time_ns);
  static void DecodeTrashChunkKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, uint64_t& chunk_index,
                                  uint64_t& time_ns);
  static std::string EncodeTrashChunkValue(const pb::mdsv2::TrashSliceList& slice_list);
  static pb::mdsv2::TrashSliceList DecodeTrashChunkValue(const std::string& value);

  // del file
  // format: [$prefix, $type, $fs_id, $ino]
  static std::string EncodeDelFileKey(uint32_t fs_id, uint64_t ino);
  static void DecodeDelFileKey(const std::string& key, uint32_t& fs_id, uint64_t& ino);
  static std::string EncodeDelFileValue(const pb::mdsv2::Inode& inode);
  static pb::mdsv2::Inode DecodeDelFileValue(const std::string& value);
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_CODEC_H_