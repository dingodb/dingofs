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
  static void GetMdsTableRange(std::string& start_key, std::string& end_key);
  static void GetFsTableRange(std::string& start_key, std::string& end_key);
  static void GetDentryTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key);
  static void GetFileInodeTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key);
  static void GetQuotaTableRange(std::string& start_key, std::string& end_key);
  static void GetDirQuotaRange(uint32_t fs_id, std::string& start_key, std::string& end_key);
  static void GetFsStatsTableRange(std::string& start_key, std::string& end_key);
  static void GetFsStatsRange(uint32_t fs_id, std::string& start_key, std::string& end_key);

  // lock
  // format: [$prefix, $type, $kDelimiter, $name]
  static std::string EncodeLockKey(const std::string& name);
  static void DecodeLockKey(const std::string& key, std::string& name);
  static std::string EncodeLockValue(int64_t mds_id, uint64_t expire_time_ns);
  static void DecodeLockValue(const std::string& value, int64_t& mds_id, uint64_t& expire_time_ns);

  // fs
  // format: [$prefix, $type, $kDelimiter, $name]
  static std::string EncodeFSKey(const std::string& name);
  static void DecodeFSKey(const std::string& key, std::string& name);
  static std::string EncodeFSValue(const pb::mdsv2::FsInfo& fs_info);
  static pb::mdsv2::FsInfo DecodeFSValue(const std::string& value);

  // format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino, $kDelimiter, $name]
  static std::string EncodeDentryKey(uint32_t fs_id, uint64_t ino, const std::string& name);
  static void DecodeDentryKey(const std::string& key, uint32_t& fs_id, uint64_t& ino, std::string& name);
  static std::string EncodeDentryValue(const pb::mdsv2::Dentry& dentry);
  static pb::mdsv2::Dentry DecodeDentryValue(const std::string& value);
  // format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino]
  // format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino+1]
  static void EncodeDentryRange(uint32_t fs_id, uint64_t ino, std::string& start_key, std::string& end_key);

  // inode
  // format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino]
  static uint32_t InodeKeyLength();
  static std::string EncodeInodeKey(uint32_t fs_id, uint64_t ino);
  static void DecodeInodeKey(const std::string& key, uint32_t& fs_id, uint64_t& ino);
  static std::string EncodeInodeValue(const pb::mdsv2::Inode& inode);
  static pb::mdsv2::Inode DecodeInodeValue(const std::string& value);

  // quota encode/decode
  // fs format: [$prefix, $type, $kDelimiter, $fs_id]
  static std::string EncodeFsQuotaKey(uint32_t fs_id);
  static void DecodeFsQuotaKey(const std::string& key, uint32_t& fs_id);
  static std::string EncodeFsQuotaValue(const pb::mdsv2::Quota& quota);
  static pb::mdsv2::Quota DecodeFsQuotaValue(const std::string& value);

  // dir format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino]
  static std::string EncodeDirQuotaKey(uint32_t fs_id, uint64_t ino);
  static void DecodeDirQuotaKey(const std::string& key, uint32_t& fs_id, uint64_t& ino);
  static std::string EncodeDirQuotaValue(const pb::mdsv2::Quota& quota);
  static pb::mdsv2::Quota DecodeDirQuotaValue(const std::string& value);

  // fs stats
  // format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $time]
  static std::string EncodeFsStatsKey(uint32_t fs_id, uint64_t time);
  static void DecodeFsStatsKey(const std::string& key, uint32_t& fs_id, uint64_t& time);
  static std::string EncodeFsStatsValue(const pb::mdsv2::FsStatsData& stats);
  static pb::mdsv2::FsStatsData DecodeFsStatsValue(const std::string& value);
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_CODEC_H_