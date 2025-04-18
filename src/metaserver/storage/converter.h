/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Dingofs
 * Created Date: 2022-03-16
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_METASERVER_STORAGE_CONVERTER_H_
#define DINGOFS_SRC_METASERVER_STORAGE_CONVERTER_H_

#include <google/protobuf/message.h>

#include <string>
#include <type_traits>

#include "dingofs/metaserver.pb.h"

namespace dingofs {
namespace metaserver {

class MetaStoreFStream;

namespace storage {

enum KEY_TYPE : unsigned char {
  kTypeInode = 1,
  kTypeS3ChunkInfo = 2,
  kTypeDentry = 3,
  kTypeVolumeExtent = 4,
  kTypeInodeAuxInfo = 5,
  kTypeFsQuota = 6,
  kTypeDirQuota = 7,
};

// NOTE: you must generate all table name by NameGenerator class for
// gurantee the fixed prefix for rocksdb storage.
// e.g: 1:0001
class NameGenerator {
 public:
  explicit NameGenerator(uint32_t partitionId);

  std::string GetInodeTableName() const;

  std::string GetS3ChunkInfoTableName() const;

  std::string GetDentryTableName() const;

  std::string GetVolumeExtentTableName() const;

  std::string GetInodeAuxInfoTableName() const;

  std::string GetFsQuotaTableName() const;

  std::string GetDirQuotaTableName() const;

  static size_t GetFixedLength();

 private:
  std::string Format(KEY_TYPE type, uint32_t partitionId);

 private:
  std::string tableName4Inode_;
  std::string tableName4S3ChunkInfo_;
  std::string tableName4Dentry_;
  std::string tableName4VolumeExtent_;
  std::string tableName4InodeAuxInfo_;
  std::string tableName4FsQuota_;
  std::string tableName4DirQuota_;
};

class StorageKey {
 public:
  virtual ~StorageKey() = default;

  virtual std::string SerializeToString() const = 0;
  virtual bool ParseFromString(const std::string& value) = 0;
};

/* rules for key serialization:
 *   Key4Inode                        : kTypeInode:fsId:InodeId
 *   Prefix4AllInode                  : kTypeInode:
 *   Key4S3ChunkInfoList              :
 * kTypeS3ChunkInfo:fsId:inodeId:chunkIndex:firstChunkId:lastChunkId  // NOLINT
 *   Prefix4ChunkIndexS3ChunkInfoList :
 * kTypeS3ChunkInfo:fsId:inodeId:chunkIndex:  // NOLINT
 *   Prefix4InodeS3ChunkInfoList      : kTypeS3ChunkInfo:fsId:inodeId:
 *   Prefix4AllS3ChunkInfoList        : kTypeS3ChunkInfo:
 *   Key4Dentry                       : kTypeDentry:parentInodeId:name
 *   Prefix4SameParentDentry          : kTypeDentry:parentInodeId:
 *   Prefix4AllDentry                 : kTypeDentry:
 *   Key4VolumeExtentSlice            : kTypeExtent:fsId:InodeId:SliceOffset
 *   Prefix4InodeVolumeExtent         : kTypeExtent:fsId:InodeId:
 *   Prefix4AllVolumeExtent           : kTypeExtent:
 *   Key4InodeAuxInfo                 : kTypeInodeAuxInfo:fsId:inodeId
 *   Key4FsQuota                      : kTypeFsQuota:fsId
 *   Key4DirQuota                     : kTypeDirQuota:fsId:inodeId
 *   Prefix4DirQuota                  : kTypeDirQuota:fsId:
 */

class Key4Inode : public StorageKey {
 public:
  Key4Inode();

  Key4Inode(uint32_t fsId, uint64_t inodeId);

  explicit Key4Inode(const pb::metaserver::Inode& inode);

  bool operator==(const Key4Inode& rhs);

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 public:
  static const KEY_TYPE keyType_ = kTypeInode;

  uint32_t fsId;
  uint64_t inodeId;
};

class Prefix4AllInode : public StorageKey {
 public:
  Prefix4AllInode() = default;

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 public:
  static const KEY_TYPE keyType_ = kTypeInode;
};

class Key4S3ChunkInfoList : public StorageKey {
 public:
  Key4S3ChunkInfoList();

  Key4S3ChunkInfoList(uint32_t fsId, uint64_t inodeId, uint64_t chunkIndex,
                      uint64_t firstChunkId, uint64_t lastChunkId,
                      uint64_t size);

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 public:
  static const size_t kMaxUint64Length_;
  static const KEY_TYPE keyType_ = kTypeS3ChunkInfo;

  uint32_t fsId;
  uint64_t inodeId;
  uint64_t chunkIndex;
  uint64_t firstChunkId;
  uint64_t lastChunkId;
  uint64_t size;
};

class Prefix4ChunkIndexS3ChunkInfoList : public StorageKey {
 public:
  Prefix4ChunkIndexS3ChunkInfoList();

  Prefix4ChunkIndexS3ChunkInfoList(uint32_t fsId, uint64_t inodeId,
                                   uint64_t chunkIndex);

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 public:
  static const KEY_TYPE keyType_ = kTypeS3ChunkInfo;

  uint32_t fsId;
  uint64_t inodeId;
  uint64_t chunkIndex;
};

class Prefix4InodeS3ChunkInfoList : public StorageKey {
 public:
  Prefix4InodeS3ChunkInfoList();

  Prefix4InodeS3ChunkInfoList(uint32_t fsId, uint64_t inodeId);

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 public:
  static const KEY_TYPE keyType_ = kTypeS3ChunkInfo;

  uint32_t fsId;
  uint64_t inodeId;
};

class Prefix4AllS3ChunkInfoList : public StorageKey {
 public:
  Prefix4AllS3ChunkInfoList() = default;

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 public:
  static const KEY_TYPE keyType_ = kTypeS3ChunkInfo;
};

class Key4Dentry : public StorageKey {
 public:
  Key4Dentry() = default;

  Key4Dentry(uint32_t fsId, uint64_t parentInodeId, const std::string& name);

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 public:
  uint32_t fsId;
  uint64_t parentInodeId;
  std::string name;

 private:
  static const KEY_TYPE keyType_ = kTypeDentry;
};

class Prefix4SameParentDentry : public StorageKey {
 public:
  Prefix4SameParentDentry() = default;

  Prefix4SameParentDentry(uint32_t fsId, uint64_t parentInodeId);

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 public:
  uint32_t fsId;
  uint64_t parentInodeId;

 private:
  static const KEY_TYPE keyType_ = kTypeDentry;
};

class Prefix4AllDentry : public StorageKey {
 public:
  Prefix4AllDentry() = default;

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 private:
  static const KEY_TYPE keyType_ = kTypeDentry;
};

class Key4VolumeExtentSlice : public StorageKey {
 public:
  Key4VolumeExtentSlice() = default;

  Key4VolumeExtentSlice(uint32_t fsId, uint64_t inodeId, uint64_t offset);

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 private:
  friend class dingofs::metaserver::MetaStoreFStream;

  uint32_t fsId_;
  uint64_t inodeId_;
  uint64_t offset_;

  static constexpr KEY_TYPE keyType_ = kTypeVolumeExtent;
};

class Prefix4InodeVolumeExtent : public StorageKey {
 public:
  Prefix4InodeVolumeExtent(uint32_t fsId, uint64_t inodeId);

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 private:
  uint32_t fsId_;
  uint64_t inodeId_;

  static constexpr KEY_TYPE keyType_ = kTypeVolumeExtent;
};

class Prefix4AllVolumeExtent : public StorageKey {
 public:
  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 private:
  static constexpr KEY_TYPE keyType_ = kTypeVolumeExtent;
};

class Key4InodeAuxInfo : public StorageKey {
 public:
  Key4InodeAuxInfo() = default;

  Key4InodeAuxInfo(uint32_t fsId, uint64_t inodeId);

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 public:
  uint32_t fsId;
  uint64_t inodeId;

 private:
  static constexpr KEY_TYPE keyType_ = kTypeInodeAuxInfo;
};

class Key4FsQuota : public StorageKey {
 public:
  Key4FsQuota() = default;

  Key4FsQuota(uint32_t fs_id);

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 public:
  uint32_t fs_id;

 private:
  static constexpr KEY_TYPE kKeyType = kTypeFsQuota;
};

class Key4DirQuota : public StorageKey {
 public:
  Key4DirQuota() = default;

  Key4DirQuota(uint32_t fs_id, uint64_t dir_inode_id);

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 public:
  uint32_t fs_id;
  uint64_t dir_inode_id;

 private:
  static constexpr KEY_TYPE kKeyType = kTypeDirQuota;
};

class Prefix4DirQuotas : public StorageKey {
 public:
  explicit Prefix4DirQuotas(uint32_t fs_id);

  std::string SerializeToString() const override;

  bool ParseFromString(const std::string& value) override;

 public:
  uint32_t fs_id;

 private:
  static constexpr KEY_TYPE kKeyType = kTypeDirQuota;
};

// converter
class Converter {
 public:
  Converter() = default;

  // for key
  std::string SerializeToString(const StorageKey& key);

  // for value
  bool SerializeToString(const google::protobuf::Message& entry,
                         std::string* value);

  // for key&value
  template <typename Entry,
            typename = typename std::enable_if<
                std::is_base_of<google::protobuf::Message, Entry>::value ||
                std::is_base_of<StorageKey, Entry>::value>::type>
  bool ParseFromString(const std::string& value, Entry* entry) {
    return entry->ParseFromString(value);
  }
};

}  // namespace storage
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_STORAGE_CONVERTER_H_
