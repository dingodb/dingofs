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

#include <cstdint>
#include <string>

#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mds/common/codec.h"
#include "mds/common/helper.h"

namespace dingofs {
namespace mds {
namespace unit_test {

class MetaDataCodecTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(MetaDataCodecTest, LockKey) {
  std::string expected_name = "test";
  std::string key = MetaCodec::EncodeLockKey(expected_name);
  EXPECT_TRUE(MetaCodec::IsLockKey(key));

  std::string actual_name;
  MetaCodec::DecodeLockKey(key, actual_name);
  EXPECT_EQ(expected_name, actual_name);
}

TEST_F(MetaDataCodecTest, AutoIncrementIDKey) {
  std::string expected_name = "test";
  std::string key = MetaCodec::EncodeAutoIncrementIDKey(expected_name);
  EXPECT_TRUE(MetaCodec::IsAutoIncrementIDKey(key));

  std::string actual_name;
  MetaCodec::DecodeAutoIncrementIDKey(key, actual_name);
  EXPECT_EQ(expected_name, actual_name);

  uint64_t expected_id = 11111;
  std::string value = MetaCodec::EncodeAutoIncrementIDValue(expected_id);
  uint64_t actual_id;
  MetaCodec::DecodeAutoIncrementIDValue(value, actual_id);
  EXPECT_EQ(expected_id, actual_id);
}

TEST_F(MetaDataCodecTest, MdsHeartbeatKey) {
  {
    int64_t expected_mds_id = 123456789;
    std::string key = MetaCodec::EncodeHeartbeatKey(expected_mds_id);

    EXPECT_TRUE(MetaCodec::IsMdsHeartbeatKey(key));

    int64_t actual_mds_id;
    MetaCodec::DecodeHeartbeatKey(key, actual_mds_id);
    EXPECT_EQ(expected_mds_id, actual_mds_id);

    MdsEntry mds;
    mds.set_id(expected_mds_id);
    std::string value = MetaCodec::EncodeHeartbeatValue(mds);
    MdsEntry actual_mds = MetaCodec::DecodeHeartbeatMdsValue(value);
    EXPECT_EQ(expected_mds_id, actual_mds.id());
  }
}

TEST_F(MetaDataCodecTest, ClientHeartbeatKey) {
  {
    std::string expected_client_id = "123e4567-e89b-12d3-a456-426614174001";
    std::string key = MetaCodec::EncodeHeartbeatKey(expected_client_id);

    EXPECT_TRUE(MetaCodec::IsClientHeartbeatKey(key));

    std::string actual_client_id;
    MetaCodec::DecodeHeartbeatKey(key, actual_client_id);
    EXPECT_EQ(expected_client_id, actual_client_id);

    ClientEntry client;
    client.set_id(expected_client_id);
    std::string value = MetaCodec::EncodeHeartbeatValue(client);
    ClientEntry actual_client = MetaCodec::DecodeHeartbeatClientValue(value);
    EXPECT_EQ(expected_client_id, actual_client.id());
  }
}

TEST_F(MetaDataCodecTest, FSKey) {
  {
    std::string expected_name = "test";
    std::string key = MetaCodec::EncodeFsKey(expected_name);

    EXPECT_TRUE(MetaCodec::IsFsKey(key));

    std::string actual_name;
    MetaCodec::DecodeFsKey(key, actual_name);
    EXPECT_EQ(expected_name, actual_name);

    FsInfoEntry fs_info;
    fs_info.set_fs_name(expected_name);
    std::string value = MetaCodec::EncodeFsValue(fs_info);
    FsInfoEntry actual_fs_info = MetaCodec::DecodeFsValue(value);
    EXPECT_EQ(expected_name, actual_fs_info.fs_name());
  }
}

TEST_F(MetaDataCodecTest, FsQuotaKey) {
  uint32_t expected_fs_id = 12345;
  std::string key = MetaCodec::EncodeFsQuotaKey(expected_fs_id);

  EXPECT_TRUE(MetaCodec::IsFsQuotaKey(key));

  uint32_t actual_fs_id;
  MetaCodec::DecodeFsQuotaKey(key, actual_fs_id);
  EXPECT_EQ(expected_fs_id, actual_fs_id);

  QuotaEntry quota;
  quota.set_max_bytes(1000);
  quota.set_max_inodes(2000);
  std::string value = MetaCodec::EncodeFsQuotaValue(quota);
  QuotaEntry actual_quota = MetaCodec::DecodeFsQuotaValue(value);
  EXPECT_EQ(quota.max_bytes(), actual_quota.max_bytes());
  EXPECT_EQ(quota.max_inodes(), actual_quota.max_inodes());
}

// static bool IsInodeKey(const std::string& key);
// static std::string EncodeInodeKey(uint32_t fs_id, Ino ino);
// static void DecodeInodeKey(const std::string& key, uint32_t& fs_id, uint64_t&
// ino); static std::string EncodeInodeValue(const AttrEntry& attr); static
// AttrEntry DecodeInodeValue(const std::string& value);

TEST_F(MetaDataCodecTest, InodeKey) {
  uint32_t expected_fs_id = 1;
  uint64_t expected_inode_id = 12345;
  std::string key =
      MetaCodec::EncodeInodeKey(expected_fs_id, expected_inode_id);

  EXPECT_TRUE(MetaCodec::IsInodeKey(key));

  uint32_t actual_fs_id;
  uint64_t actual_inode_id;
  MetaCodec::DecodeInodeKey(key, actual_fs_id, actual_inode_id);
  EXPECT_EQ(expected_fs_id, actual_fs_id);
  EXPECT_EQ(expected_inode_id, actual_inode_id);

  AttrEntry attr;
  attr.set_mode(0755);
  attr.set_uid(1000);
  attr.set_gid(1000);
  attr.set_length(1024);
  std::string value = MetaCodec::EncodeInodeValue(attr);
  AttrEntry actual_attr = MetaCodec::DecodeInodeValue(value);
  EXPECT_EQ(attr.mode(), actual_attr.mode());
  EXPECT_EQ(attr.uid(), actual_attr.uid());
  EXPECT_EQ(attr.gid(), actual_attr.gid());
  EXPECT_EQ(attr.length(), actual_attr.length());
}

TEST_F(MetaDataCodecTest, DentryKey) {
  {
    int expected_fs_id = 1;
    uint64_t expected_inode_id = 12345;
    std::string expected_name = "test121232";
    std::string key = MetaCodec::EncodeDentryKey(
        expected_fs_id, expected_inode_id, expected_name);

    EXPECT_TRUE(MetaCodec::IsDentryKey(key));

    uint32_t actual_fs_id;
    uint64_t actual_inode_id;
    std::string actual_name;
    MetaCodec::DecodeDentryKey(key, actual_fs_id, actual_inode_id, actual_name);
    EXPECT_EQ(expected_fs_id, actual_fs_id);
    EXPECT_EQ(expected_inode_id, actual_inode_id);
    EXPECT_EQ(expected_name, actual_name);

    DentryEntry dentry;
    dentry.set_name(expected_name);
    dentry.set_ino(expected_inode_id);
    std::string value = MetaCodec::EncodeDentryValue(dentry);
    DentryEntry actual_dentry = MetaCodec::DecodeDentryValue(value);
    EXPECT_EQ(dentry.name(), actual_dentry.name());
    EXPECT_EQ(dentry.ino(), actual_dentry.ino());
  }
}

// Sub-trash bucket dentries are encoded as plain Dentry, the same as tree
// dentries. ParseFsMetaTableKey relies on this — older builds wrapped these
// values in a TrashDentry message, which has been removed.
TEST_F(MetaDataCodecTest, ParseFsMetaTableKeyDecodesTrashBucketDentry) {
  constexpr uint32_t kFsId = 7;
  // Sub-trash hour bucket parent (range begins at kTrashSubInodeStart).
  const uint64_t kBucketIno = 0x7FFFFFFF00000005ULL;
  const uint64_t kFileIno = 1234567ULL;
  const std::string kEntryName = "1-1234567-foo.txt";

  DentryEntry dentry;
  dentry.set_fs_id(kFsId);
  dentry.set_ino(kFileIno);
  dentry.set_parent(kBucketIno);
  dentry.set_name(kEntryName);
  dentry.set_type(pb::mds::FileType::FILE);

  std::string key = MetaCodec::EncodeDentryKey(kFsId, kBucketIno, kEntryName);
  std::string value = MetaCodec::EncodeDentryValue(dentry);

  auto desc = MetaCodec::ParseFsMetaTableKey(key, value);
  EXPECT_NE(desc.second.find(fmt::format("ino: {}", kFileIno)), std::string::npos)
      << "value desc must surface bucket dentry ino, got: " << desc.second;
  EXPECT_NE(desc.second.find(kEntryName), std::string::npos)
      << "value desc must surface bucket dentry name, got: " << desc.second;
}

TEST_F(MetaDataCodecTest, ChunkKey) {
  uint32_t expected_fs_id = 1;
  Ino expected_inode_id = 12345;
  uint64_t expected_chunk_index = 67890;
  std::string key = MetaCodec::EncodeChunkKey(expected_fs_id, expected_inode_id,
                                              expected_chunk_index);

  EXPECT_TRUE(MetaCodec::IsChunkKey(key));

  uint32_t actual_fs_id;
  uint64_t actual_inode_id;
  uint64_t actual_chunk_index;
  MetaCodec::DecodeChunkKey(key, actual_fs_id, actual_inode_id,
                            actual_chunk_index);
  EXPECT_EQ(expected_fs_id, actual_fs_id);
  EXPECT_EQ(expected_inode_id, actual_inode_id);
  EXPECT_EQ(expected_chunk_index, actual_chunk_index);

  ChunkEntry chunk;
  chunk.set_index(expected_chunk_index);
  chunk.set_block_size(4096);
  chunk.set_version(12);
  std::string value = MetaCodec::EncodeChunkValue(chunk);
  ChunkEntry actual_chunk = MetaCodec::DecodeChunkValue(value);
  EXPECT_EQ(chunk.index(), actual_chunk.index());
  EXPECT_EQ(chunk.block_size(), actual_chunk.block_size());
  EXPECT_EQ(chunk.version(), actual_chunk.version());
}

TEST_F(MetaDataCodecTest, FileSessionKey) {
  uint32_t expected_fs_id = 1;
  Ino expected_inode_id = 12345;
  std::string expected_session_id = "123e4567-e89b-12d3-a456-426614174000";
  std::string key = MetaCodec::EncodeFileSessionKey(
      expected_fs_id, expected_inode_id, expected_session_id);
  EXPECT_TRUE(MetaCodec::IsFileSessionKey(key));
  uint32_t actual_fs_id;
  uint64_t actual_inode_id;
  std::string actual_session_id;
  MetaCodec::DecodeFileSessionKey(key, actual_fs_id, actual_inode_id,
                                  actual_session_id);
  EXPECT_EQ(expected_fs_id, actual_fs_id);
  EXPECT_EQ(expected_inode_id, actual_inode_id);
  EXPECT_EQ(expected_session_id, actual_session_id);

  FileSessionEntry file_session;
  file_session.set_session_id(expected_session_id);
  file_session.set_fs_id(expected_fs_id);
  file_session.set_ino(expected_inode_id);
  std::string value = MetaCodec::EncodeFileSessionValue(file_session);
  FileSessionEntry actual_file_session =
      MetaCodec::DecodeFileSessionValue(value);
  EXPECT_EQ(file_session.session_id(), actual_file_session.session_id());
  EXPECT_EQ(file_session.fs_id(), actual_file_session.fs_id());
  EXPECT_EQ(file_session.ino(), actual_file_session.ino());
}

TEST_F(MetaDataCodecTest, DirQuotaKey) {
  uint32_t expected_fs_id = 1;
  Ino expected_inode_id = 12345;
  std::string key =
      MetaCodec::EncodeDirQuotaKey(expected_fs_id, expected_inode_id);

  EXPECT_TRUE(MetaCodec::IsDirQuotaKey(key));

  uint32_t actual_fs_id;
  Ino actual_inode_id;
  MetaCodec::DecodeDirQuotaKey(key, actual_fs_id, actual_inode_id);
  EXPECT_EQ(expected_fs_id, actual_fs_id);
  EXPECT_EQ(expected_inode_id, actual_inode_id);

  QuotaEntry dir_quota;
  dir_quota.set_max_bytes(1000);
  dir_quota.set_max_inodes(2000);
  std::string value = MetaCodec::EncodeDirQuotaValue(dir_quota);
  QuotaEntry actual_dir_quota = MetaCodec::DecodeDirQuotaValue(value);
  EXPECT_EQ(dir_quota.max_bytes(), actual_dir_quota.max_bytes());
  EXPECT_EQ(dir_quota.max_inodes(), actual_dir_quota.max_inodes());
}

TEST_F(MetaDataCodecTest, DelSliceKey) {
  uint32_t expected_fs_id = 1;
  Ino expected_inode_id = 12345;
  uint64_t expected_chunk_index = 67890;
  uint64_t expected_time_ns = 1234567890;
  std::string key =
      MetaCodec::EncodeDelSliceKey(expected_fs_id, expected_inode_id,
                                   expected_chunk_index, expected_time_ns);

  EXPECT_TRUE(MetaCodec::IsDelSliceKey(key));

  uint32_t actual_fs_id;
  uint64_t actual_inode_id;
  uint64_t actual_chunk_index;
  uint64_t actual_time_ns;
  MetaCodec::DecodeDelSliceKey(key, actual_fs_id, actual_inode_id,
                               actual_chunk_index, actual_time_ns);
  EXPECT_EQ(expected_fs_id, actual_fs_id);
  EXPECT_EQ(expected_inode_id, actual_inode_id);
  EXPECT_EQ(expected_chunk_index, actual_chunk_index);
  EXPECT_EQ(expected_time_ns, actual_time_ns);

  TrashSliceList slice_list;
  auto* slice = slice_list.add_slices();
  slice->set_fs_id(expected_fs_id);
  slice->set_ino(expected_inode_id);
  slice->set_chunk_index(expected_chunk_index);
  slice->mutable_slice()->set_id(1210231231232);
  slice->set_chunk_size(123123213213);
  std::string value = MetaCodec::EncodeDelSliceValue(slice_list);
  TrashSliceList actual_slice_list = MetaCodec::DecodeDelSliceValue(value);
  EXPECT_EQ(slice_list.slices_size(), actual_slice_list.slices_size());
  for (int i = 0; i < slice_list.slices_size(); ++i) {
    EXPECT_EQ(slice_list.slices(i).fs_id(),
              actual_slice_list.slices(i).fs_id());
    EXPECT_EQ(slice_list.slices(i).ino(), actual_slice_list.slices(i).ino());
    EXPECT_EQ(slice_list.slices(i).chunk_index(),
              actual_slice_list.slices(i).chunk_index());
    EXPECT_EQ(slice_list.slices(i).slice().id(),
              actual_slice_list.slices(i).slice().id());
    EXPECT_EQ(slice_list.slices(i).chunk_size(),
              actual_slice_list.slices(i).chunk_size());
  }
}

TEST_F(MetaDataCodecTest, DelFileKey) {
  uint32_t expected_fs_id = 1;
  Ino expected_inode_id = 12345;
  std::string key =
      MetaCodec::EncodeDelFileKey(expected_fs_id, expected_inode_id);
  EXPECT_TRUE(MetaCodec::IsDelFileKey(key));
  uint32_t actual_fs_id;
  Ino actual_inode_id;
  MetaCodec::DecodeDelFileKey(key, actual_fs_id, actual_inode_id);
  EXPECT_EQ(expected_fs_id, actual_fs_id);
  EXPECT_EQ(expected_inode_id, actual_inode_id);

  AttrEntry attr;
  attr.set_mode(0755);
  attr.set_uid(1000);
  attr.set_gid(1000);
  attr.set_length(1024);
  std::string value = MetaCodec::EncodeDelFileValue(attr);
  AttrEntry actual_attr = MetaCodec::DecodeDelFileValue(value);
  EXPECT_EQ(attr.mode(), actual_attr.mode());
  EXPECT_EQ(attr.uid(), actual_attr.uid());
  EXPECT_EQ(attr.gid(), actual_attr.gid());
  EXPECT_EQ(attr.length(), actual_attr.length());
}

TEST_F(MetaDataCodecTest, FsStatsKey) {
  uint32_t expected_fs_id = 1;
  uint64_t expected_time_ns = 1234567890;
  std::string key =
      MetaCodec::EncodeFsStatsKey(expected_fs_id, expected_time_ns);

  EXPECT_TRUE(MetaCodec::IsFsStatsKey(key));

  uint32_t actual_fs_id;
  uint64_t actual_time_ns;
  MetaCodec::DecodeFsStatsKey(key, actual_fs_id, actual_time_ns);
  EXPECT_EQ(expected_fs_id, actual_fs_id);
  EXPECT_EQ(expected_time_ns, actual_time_ns);

  FsStatsDataEntry stats;
  stats.set_read_bytes(1000000);
  stats.set_read_qps(100);
  stats.set_write_bytes(2000000);
  stats.set_write_qps(200);
  stats.set_s3_read_bytes(3000000);
  stats.set_s3_read_qps(300);
  stats.set_s3_write_bytes(4000000);
  stats.set_s3_write_qps(400);

  std::string value = MetaCodec::EncodeFsStatsValue(stats);
  FsStatsDataEntry actual_stats = MetaCodec::DecodeFsStatsValue(value);
  EXPECT_EQ(stats.read_bytes(), actual_stats.read_bytes());
  EXPECT_EQ(stats.read_qps(), actual_stats.read_qps());
  EXPECT_EQ(stats.write_bytes(), actual_stats.write_bytes());
  EXPECT_EQ(stats.write_qps(), actual_stats.write_qps());
  EXPECT_EQ(stats.s3_read_bytes(), actual_stats.s3_read_bytes());
  EXPECT_EQ(stats.s3_read_qps(), actual_stats.s3_read_qps());
  EXPECT_EQ(stats.s3_write_bytes(), actual_stats.s3_write_bytes());
  EXPECT_EQ(stats.s3_write_qps(), actual_stats.s3_write_qps());
}

TEST_F(MetaDataCodecTest, ParseKeyFromHexInodeAttr) {
  // The key from the task description: old prefix, fs_id=10002, kMetaFsInode,
  // ino=20000249036, kFsInodeAttr.
  std::string desc = MetaCodec::ParseKeyFromHex(
      "7844494e474f46533a05000027120d00000004a81b94cc01");

  EXPECT_NE(desc.find("xDINGOFS:"), std::string::npos) << desc;
  EXPECT_NE(desc.find("kTableFsMeta"), std::string::npos) << desc;
  EXPECT_NE(desc.find("fs_id(10002)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("kMetaFsInode"), std::string::npos) << desc;
  EXPECT_NE(desc.find("ino(20000249036)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("kFsInodeAttr"), std::string::npos) << desc;
}

TEST_F(MetaDataCodecTest, ParseKeyFromHexDentry) {
  // old prefix, fs_id=7, ino=12345, kFsInodeDentry, name="hello.txt".
  std::string desc = MetaCodec::ParseKeyFromHex(
      "7844494e474f46533a05000000070d00000000000030390568656c6c6f2e747874");

  EXPECT_NE(desc.find("fs_id(7)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("kMetaFsInode"), std::string::npos) << desc;
  EXPECT_NE(desc.find("ino(12345)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("kFsInodeDentry"), std::string::npos) << desc;
  EXPECT_NE(desc.find("name(hello.txt)"), std::string::npos) << desc;
}

TEST_F(MetaDataCodecTest, ParseKeyFromHexChunk) {
  // old prefix, fs_id=3, ino=999, kFsInodeChunk, chunk_index=67890.
  std::string desc = MetaCodec::ParseKeyFromHex(
      "7844494e474f46533a05000000030d00000000000003e7070000000000010932");

  EXPECT_NE(desc.find("fs_id(3)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("ino(999)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("kFsInodeChunk"), std::string::npos) << desc;
  EXPECT_NE(desc.find("chunk_index(67890)"), std::string::npos) << desc;
}

TEST_F(MetaDataCodecTest, ParseKeyFromHexFsQuota) {
  // old prefix, kTableMeta, kMetaFsQuota, fs_id=12345.
  std::string desc =
      MetaCodec::ParseKeyFromHex("7844494e474f46533a010900003039");

  EXPECT_NE(desc.find("kTableMeta"), std::string::npos) << desc;
  EXPECT_NE(desc.find("kMetaFsQuota"), std::string::npos) << desc;
  EXPECT_NE(desc.find("fs_id(12345)"), std::string::npos) << desc;
}

TEST_F(MetaDataCodecTest, ParseKeyFromHexFsStats) {
  // old prefix, kTableFsStats, kMetaFsStats, fs_id=8, time_ns=1234567890.
  std::string desc = MetaCodec::ParseKeyFromHex(
      "7844494e474f46533a03150000000800000000499602d2");

  EXPECT_NE(desc.find("kTableFsStats"), std::string::npos) << desc;
  EXPECT_NE(desc.find("kMetaFsStats"), std::string::npos) << desc;
  EXPECT_NE(desc.find("fs_id(8)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("time_ns(1234567890)"), std::string::npos) << desc;
}

TEST_F(MetaDataCodecTest, ParseKeyFromHexDelSlice) {
  // old prefix, fs_id=2, ino=555, chunk_index=111, time_ns=222.
  std::string desc = MetaCodec::ParseKeyFromHex(
      "7844494e474f46533a050000000211000000000000022b000000000000006f"
      "00000000000000de");

  EXPECT_NE(desc.find("fs_id(2)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("kMetaFsDelSlice"), std::string::npos) << desc;
  EXPECT_NE(desc.find("ino(555)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("chunk_index(111)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("time_ns(222)"), std::string::npos) << desc;
}

TEST_F(MetaDataCodecTest, ParseKeyFromHexFileSession) {
  // old prefix, fs_id=4, ino=777, session_id="123e4567-...".
  std::string desc = MetaCodec::ParseKeyFromHex(
      "7844494e474f46533a05000000040f00000000000003093132336534353637"
      "2d653839622d313264332d613435362d343236363134313734303030");

  EXPECT_NE(desc.find("fs_id(4)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("kMetaFsFileSession"), std::string::npos) << desc;
  EXPECT_NE(desc.find("ino(777)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("session_id(123e4567-e89b-12d3-a456-426614174000)"),
            std::string::npos)
      << desc;
}

TEST_F(MetaDataCodecTest, ParseKeyFromHexSliceRef) {
  // old prefix, kTableMeta, kMetaFsSliceRef, slice_id=987654321.
  std::string desc =
      MetaCodec::ParseKeyFromHex("7844494e474f46533a011d000000003ade68b1");

  EXPECT_NE(desc.find("kMetaFsSliceRef"), std::string::npos) << desc;
  EXPECT_NE(desc.find("slice_id(987654321)"), std::string::npos) << desc;
}

TEST_F(MetaDataCodecTest, ParseKeyFromHexFs) {
  // old prefix, kTableMeta, kMetaFs, name="myfs".
  std::string desc =
      MetaCodec::ParseKeyFromHex("7844494e474f46533a01076d796673");

  EXPECT_NE(desc.find("kMetaFs"), std::string::npos) << desc;
  EXPECT_NE(desc.find("name(myfs)"), std::string::npos) << desc;
}

TEST_F(MetaDataCodecTest, ParseKeyFromHexNewPrefix) {
  // new prefix: "xdingofs" + cluster_id(16), kTableFsMeta, fs_id=100,
  // kMetaFsInode, ino=200, kFsInodeAttr.
  std::string desc = MetaCodec::ParseKeyFromHex(
      "7864696e676f66730000001005000000640d00000000000000c801");

  EXPECT_NE(desc.find("xdingofs(16)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("fs_id(100)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("ino(200)"), std::string::npos) << desc;
  EXPECT_NE(desc.find("kFsInodeAttr"), std::string::npos) << desc;
}

TEST_F(MetaDataCodecTest, ParseKeyFromHexUnknownPrefix) {
  std::string desc = MetaCodec::ParseKeyFromHex("00010203");
  EXPECT_NE(desc.find("unknown key prefix"), std::string::npos) << desc;
}

TEST_F(MetaDataCodecTest, ParseKeyFromHexTruncated) {
  // Only the prefix, no table id.
  std::string desc = MetaCodec::ParseKeyFromHex("7844494e474f46533a");
  EXPECT_NE(desc.find("truncated"), std::string::npos) << desc;
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs