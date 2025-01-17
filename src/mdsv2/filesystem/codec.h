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
#include "fs/local_filesystem.h"

namespace dingofs {
namespace mdsv2 {

class MetaDataCodec {
 public:
  static void GetFsTableRange(std::string& start_key, std::string& end_key);
  static void GetDentryTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key);
  static void GetFileInodeTableRange(uint32_t fs_id, std::string& start_key, std::string& end_key);

  // format: [$prefix, $type, $kDelimiter, $name]
  static std::string EncodeFSKey(const std::string& name);
  static void DecodeFSKey(const std::string& key, std::string& name);
  static std::string EncodeFSValue(const pb::mdsv2::FsInfo& fs_info);
  static pb::mdsv2::FsInfo DecodeFSValue(const std::string& value);

  // format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino, $kDelimiter, $name]
  static std::string EncodeDentryKey(int fs_id, uint64_t ino, const std::string& name);
  static void DecodeDentryKey(const std::string& key, int& fs_id, uint64_t& ino, std::string& name);
  static std::string EncodeDentryValue(const pb::mdsv2::Dentry& dentry);
  static pb::mdsv2::Dentry DecodeDentryValue(const std::string& value);
  // format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino]
  // format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino+1]
  static void EncodeDentryRange(int fs_id, uint64_t ino, std::string& start_key, std::string& end_key);

  // format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino]
  static std::string EncodeDirInodeKey(int fs_id, uint64_t ino);
  static void DecodeDirInodeKey(const std::string& key, int& fs_id, uint64_t& ino);
  static std::string EncodeDirInodeValue(const pb::mdsv2::Inode& inode);
  static pb::mdsv2::Inode DecodeDirInodeValue(const std::string& value);

  // format: [$prefix, $type, $kDelimiter, $fs_id, $kDelimiter, $ino]
  static std::string EncodeFileInodeKey(int fs_id, uint64_t ino);
  static void DecodeFileInodeKey(const std::string& key, int& fs_id, uint64_t& ino);
  static std::string EncodeFileInodeValue(const pb::mdsv2::Inode& inode);
  static pb::mdsv2::Inode DecodeFileInodeValue(const std::string& value);
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_CODEC_H_