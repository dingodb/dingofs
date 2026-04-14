/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>

#include "client/vfs/data/file.h"
#include "client/vfs/data_buffer.h"
#include "common/trace/trace_manager.h"
#include "test/unit/client/vfs/test_base.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;
using dingofs::client::vfs::test::VFSTestBase;

// Helper: build a file Attr with a specific length.
static Attr MakeFileAttrWithLen(Ino ino, uint64_t length) {
  Attr a;
  a.ino = ino;
  a.type = dingofs::kFile;
  a.length = length;
  a.mode = 0644;
  a.nlink = 1;
  return a;
}

class FileTest : public VFSTestBase {
 protected:
  void SetUp() override {
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());

    // Default GetAttr: file of 4 MiB.
    file_length_ = 4 * 1024 * 1024;
    Attr attr = MakeFileAttrWithLen(kIno, file_length_);
    ON_CALL(*mock_meta_system_, GetAttr(_, kIno, _))
        .WillByDefault(DoAll(SetArgPointee<2>(attr), Return(Status::OK())));
    EXPECT_CALL(*mock_meta_system_, GetAttr(_, kIno, _)).Times(AnyNumber());
  }

  // Creates a File and opens it. Caller must call Close() and delete.
  File* MakeOpenFile(Ino ino = kIno, uint64_t fh = kFh) {
    auto* f = new File(mock_hub_, fh, ino);
    CHECK(f->Open().ok());
    return f;
  }

  static constexpr Ino kIno = 400;
  static constexpr uint64_t kFh = 4;
  uint64_t file_length_ = 0;
  std::unique_ptr<TraceManager> trace_manager_;
};

// 1. Open() succeeds and returns OK.
TEST_F(FileTest, Open_Succeeds) {
  auto* f = new File(mock_hub_, kFh, kIno);
  Status s = f->Open();
  EXPECT_TRUE(s.ok());
  f->Close();
  delete f;
}

// 2. Write() before Open() is not allowed by the design — Open() must
//    be called first. Here we verify that Open()+Write() succeeds.
TEST_F(FileTest, Write_AfterOpen_Succeeds) {
  auto* f = MakeOpenFile();

  const char buf[] = "test data";
  uint64_t wsize = 0;
  Status s = f->Write(ctx_, buf, sizeof(buf), /*offset=*/0, &wsize);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(wsize, sizeof(buf));

  f->Close();
  delete f;
}

// 3. Read() after Open() with a valid GetAttr succeeds.
TEST_F(FileTest, Read_AfterOpen_Succeeds) {
  auto* f = MakeOpenFile();

  DataBuffer buf;
  uint64_t rsize = 0;
  Status s = f->Read(ctx_, &buf, /*size=*/4096, /*offset=*/0, &rsize);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(rsize, 4096u);

  f->Close();
  delete f;
}

// 4. Flush() after a write calls WriteSlice on meta system.
TEST_F(FileTest, Flush_AfterWrite_CallsWriteSlice) {
  int ws_calls = 0;
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault([&](auto, auto, auto, auto, auto) {
        ++ws_calls;
        return Status::OK();
      });

  auto* f = MakeOpenFile();

  const char buf[] = "flush test";
  uint64_t wsize = 0;
  f->Write(ctx_, buf, sizeof(buf), 0, &wsize);

  Status s = f->Flush();
  EXPECT_TRUE(s.ok());
  EXPECT_GE(ws_calls, 1);

  f->Close();
  delete f;
}

// 5. Flush() failure sets file_status_; subsequent Write() returns that error.
TEST_F(FileTest, Flush_Error_SetsFileStatus_SubsequentWriteFails) {
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault(Return(Status::Internal("flush fail")));

  auto* f = MakeOpenFile();

  const char buf[] = "broken";
  uint64_t wsize = 0;
  f->Write(ctx_, buf, sizeof(buf), 0, &wsize);

  // First Flush fails and sets file_status_.
  Status flush_s = f->Flush();
  EXPECT_FALSE(flush_s.ok());

  // Subsequent Write should fail PreCheck because file_status_ is not OK.
  uint64_t wsize2 = 0;
  Status write_s = f->Write(ctx_, buf, sizeof(buf), 0, &wsize2);
  EXPECT_FALSE(write_s.ok());

  f->Close();
  delete f;
}

// 6. Flush() failure; subsequent Read() also fails via PreCheck.
TEST_F(FileTest, Flush_Error_SetsFileStatus_SubsequentReadFails) {
  ON_CALL(*mock_meta_system_, WriteSlice)
      .WillByDefault(Return(Status::Internal("flush fail")));

  auto* f = MakeOpenFile();

  const char wbuf[] = "data";
  uint64_t wsize = 0;
  f->Write(ctx_, wbuf, sizeof(wbuf), 0, &wsize);

  Status flush_s = f->Flush();
  EXPECT_FALSE(flush_s.ok());

  DataBuffer rbuf;
  uint64_t rsize = 0;
  Status read_s = f->Read(ctx_, &rbuf, 4096, 0, &rsize);
  EXPECT_FALSE(read_s.ok());

  f->Close();
  delete f;
}

// 7. Invalidate() delegates to the reader without crashing.
TEST_F(FileTest, Invalidate_NoCrash) {
  auto* f = MakeOpenFile();

  // No prior reads, so invalidate is a no-op on the reader.
  f->Invalidate(/*offset=*/0, /*size=*/4096);

  f->Close();
  delete f;
}

// 8. Close() can be called twice safely (idempotent for FileWriter).
TEST_F(FileTest, Close_Idempotent) {
  auto* f = MakeOpenFile();

  f->Close();
  f->Close();  // Second close must not crash.

  delete f;
}

// 9. Read() at offset beyond EOF returns 0 bytes.
TEST_F(FileTest, Read_OffsetBeyondEOF_ReturnsZero) {
  auto* f = MakeOpenFile();

  DataBuffer buf;
  uint64_t rsize = 0xDEAD;
  Status s = f->Read(ctx_, &buf, 1024, file_length_ + 1024, &rsize);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(rsize, 0u);

  f->Close();
  delete f;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
