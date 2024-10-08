/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: curve
 * Created Date: Thur Apr 24th 2019
 * Author: lixiaocui
 */

#include "src/mds/nameserver2/helper/namespace_helper.h"

#include <gtest/gtest.h>

#include <string>

#include "src/common/namespace_define.h"
#include "src/common/timeutility.h"

using ::curve::common::COMMON_PREFIX_LENGTH;
using ::curve::common::FILEINFOKEYPREFIX;
using ::curve::common::SEGMENTALLOCSIZEKEY;
using ::curve::common::SEGMENTINFOKEYPREFIX;
using ::curve::common::SNAPSHOTFILEINFOKEYPREFIX;

namespace curve {
namespace mds {
TEST(NameSpaceHelperTest, test_EncodeFileStoreKey) {
  std::string filename = "foo.txt";
  uint64_t parentID = 8;
  std::string str =
      NameSpaceStorageCodec::EncodeFileStoreKey(parentID, filename);

  ASSERT_EQ(str.size(), 17);
  ASSERT_EQ(str.substr(0, COMMON_PREFIX_LENGTH), FILEINFOKEYPREFIX);
  ASSERT_EQ(str.substr(10, filename.length()), filename);
  for (int i = 2; i != 9; i++) {
    ASSERT_EQ(static_cast<int>(str[i]), 0);
  }
  ASSERT_EQ(static_cast<int>(str[9]), 8);

  parentID = 8 << 8;
  str = NameSpaceStorageCodec::EncodeFileStoreKey(parentID, filename);

  ASSERT_EQ(str.size(), 17);
  ASSERT_EQ(str.substr(0, COMMON_PREFIX_LENGTH), FILEINFOKEYPREFIX);
  ASSERT_EQ(str.substr(10, filename.length()), filename);
  for (int i = 2; i != 8; i++) {
    ASSERT_EQ(static_cast<int>(str[i]), 0);
  }
  ASSERT_EQ(static_cast<int>(str[8]), 8);
  ASSERT_EQ(static_cast<int>(str[9]), 0);
}

TEST(NameSpaceHelperTest, test_EncodeSnapShotFileStoreKey) {
  std::string snapshotName = "hello-1";
  uint64_t parentID = 8;
  std::string str =
      NameSpaceStorageCodec::EncodeSnapShotFileStoreKey(parentID, snapshotName);

  ASSERT_EQ(str.size(), 17);
  ASSERT_EQ(str.substr(0, COMMON_PREFIX_LENGTH), SNAPSHOTFILEINFOKEYPREFIX);
  ASSERT_EQ(str.substr(10, snapshotName.length()), snapshotName);
  for (int i = 2; i != 8; i++) {
    ASSERT_EQ(static_cast<int>(str[i]), 0);
  }
  ASSERT_EQ(static_cast<int>(str[9]), 8);
}

TEST(NameSpaceHelperTest, test_EncodeSegmentStoreKey) {
  uint64_t inodeID = 8;
  offset_t offset = 3 << 16;
  std::string str =
      NameSpaceStorageCodec::EncodeSegmentStoreKey(inodeID, offset);

  ASSERT_EQ(str.substr(0, COMMON_PREFIX_LENGTH), SEGMENTINFOKEYPREFIX);

  ASSERT_EQ(str.size(), 18);
  for (int i = 2; i != 9; i++) {
    ASSERT_EQ(static_cast<int>(str[i]), 0);
  }
  ASSERT_EQ(static_cast<int>(str[9]), 8);

  for (int i = 10; i != 15; i++) {
    ASSERT_EQ(static_cast<int>(str[i]), 0);
  }
  ASSERT_EQ(static_cast<int>(str[15]), 3);
  ASSERT_EQ(static_cast<int>(str[16]), 0);
  ASSERT_EQ(static_cast<int>(str[17]), 0);
}

TEST(NameSpaceHelperTest, test_EncodeAndDecodeID) {
  // encode
  std::string str = "18446744073709551615";
  auto res = NameSpaceStorageCodec::EncodeID(ULLONG_MAX);
  ASSERT_EQ(str, res);

  // decode success
  uint64_t out;
  ASSERT_TRUE(NameSpaceStorageCodec::DecodeID(str, &out));
  ASSERT_EQ(ULLONG_MAX, out);

  // decode fail
  ASSERT_FALSE(NameSpaceStorageCodec::DecodeID("ffffff", &out));
}

TEST(NameSpaceHelperTest, test_EncodeAnDecode_FileInfo) {
  uint64_t DefaultChunkSize = 16 * kMB;

  FileInfo fileInfo;
  fileInfo.set_id(2 << 8);
  fileInfo.set_filename("helloword.log");
  fileInfo.set_parentid(1 << 8);
  fileInfo.set_filetype(FileType::INODE_DIRECTORY);
  fileInfo.set_chunksize(DefaultChunkSize);
  fileInfo.set_length(10 << 20);
  fileInfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
  fileInfo.set_seqnum(1);

  // encode fileInfo
  std::string out;
  ASSERT_TRUE(NameSpaceStorageCodec::EncodeFileInfo(fileInfo, &out));
  ASSERT_EQ(fileInfo.ByteSize(), out.size());

  // decode fileInfo
  FileInfo decodeRes;
  ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &decodeRes));
  ASSERT_EQ(fileInfo.id(), decodeRes.id());
  ASSERT_EQ(fileInfo.filename(), decodeRes.filename());
  ASSERT_EQ(fileInfo.parentid(), decodeRes.parentid());
  ASSERT_EQ(fileInfo.filetype(), decodeRes.filetype());
  ASSERT_EQ(fileInfo.chunksize(), decodeRes.chunksize());
  ASSERT_EQ(fileInfo.length(), decodeRes.length());
  ASSERT_EQ(fileInfo.ctime(), decodeRes.ctime());
  ASSERT_EQ(fileInfo.seqnum(), decodeRes.seqnum());

  // encode fileInfo ctime donnot set
  fileInfo.clear_ctime();
  ASSERT_FALSE(fileInfo.has_ctime());
  ASSERT_TRUE(NameSpaceStorageCodec::EncodeFileInfo(fileInfo, &out));
  ASSERT_EQ(fileInfo.ByteSize(), out.size());

  // decode
  ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &decodeRes));
  ASSERT_FALSE(decodeRes.has_ctime());
}

TEST(NameSpaceHelperTest, test_EncodeAndDecode_Segment) {
  PageFileSegment segment;
  segment.set_chunksize(16 << 20);
  segment.set_segmentsize(1 << 30);
  segment.set_startoffset(0);
  segment.set_logicalpoolid(16);
  int size = segment.segmentsize() / segment.chunksize();
  for (uint32_t i = 0; i < size; i++) {
    PageFileChunkInfo* chunkinfo = segment.add_chunks();
    chunkinfo->set_chunkid(i + 1);
    chunkinfo->set_copysetid(i + 1);
  }

  // encode segment
  std::string out;
  ASSERT_TRUE(NameSpaceStorageCodec::EncodeSegment(segment, &out));
  ASSERT_EQ(segment.ByteSize(), out.size());

  // decode segment
  PageFileSegment decodeRes;
  ASSERT_TRUE(NameSpaceStorageCodec::DecodeSegment(out, &decodeRes));
  ASSERT_EQ(segment.logicalpoolid(), decodeRes.logicalpoolid());
  ASSERT_EQ(segment.segmentsize(), decodeRes.segmentsize());
  ASSERT_EQ(segment.chunksize(), decodeRes.chunksize());
  ASSERT_EQ(segment.startoffset(), decodeRes.startoffset());
  ASSERT_EQ(segment.chunks_size(), decodeRes.chunks_size());
  for (int i = 0; i < size; i++) {
    ASSERT_EQ(i + 1, decodeRes.chunks(i).chunkid());
    ASSERT_EQ(i + 1, decodeRes.chunks(i).copysetid());
  }
}

TEST(NameSpaceHelperTest, test_EncodeSegmentAllocKey) {
  uint16_t lpid = 1;
  std::string res = SEGMENTALLOCSIZEKEY + std::to_string(lpid);
  ASSERT_EQ(res, NameSpaceStorageCodec::EncodeSegmentAllocKey(lpid));
}

TEST(NameSpaceHelperTest, test_Encode_Decode_SegmentAllocValue) {
  ASSERT_EQ("1_1024", NameSpaceStorageCodec::EncodeSegmentAllocValue(1, 1024));

  uint16_t lid;
  uint64_t alloc;
  ASSERT_TRUE(
      NameSpaceStorageCodec::DecodeSegmentAllocValue("1_1024", &lid, &alloc));
  ASSERT_EQ(1, lid);
  ASSERT_EQ(1024, alloc);

  ASSERT_FALSE(
      NameSpaceStorageCodec::DecodeSegmentAllocValue("world", &lid, &alloc));
}

}  // namespace mds
}  // namespace curve
