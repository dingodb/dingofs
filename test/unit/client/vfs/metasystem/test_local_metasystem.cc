/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include <fcntl.h>
#include <sys/stat.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "client/vfs/metasystem/local/metasystem.h"
#include "common/meta.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace local {
namespace {

class LocalMetaSystemTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char path[] = "/tmp/dingofs-local-metasystem-XXXXXX";
    const char* dir = ::mkdtemp(path);
    ASSERT_NE(dir, nullptr);
    db_path_ = dir;

    meta_system_ = std::make_unique<LocalMetaSystem>(db_path_, "test-fs", "");
    auto status = meta_system_->Init(false);
    ASSERT_TRUE(status.ok()) << status.ToString();
    initialized_ = true;
  }

  void TearDown() override {
    if (initialized_) meta_system_->Stop(false);
    std::filesystem::remove_all(db_path_);
  }

  Attr CreateFile(const std::string& name) {
    Attr attr;
    auto status = meta_system_->MkNod(nullptr, 1, name, 0, 0,
                                      S_IFREG | S_IRUSR | S_IWUSR, 0, &attr);
    EXPECT_TRUE(status.ok()) << status.ToString();
    return attr;
  }

  void WriteSlice(Ino ino, uint64_t chunk_index, uint64_t id, int32_t pos,
                  int32_t len) {
    Slice slice;
    slice.id = id;
    slice.pos = pos;
    slice.size = len;
    slice.off = 0;
    slice.len = len;
    auto status =
        meta_system_->WriteSlice(nullptr, ino, chunk_index, 0, {slice});
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  std::string db_path_;
  std::unique_ptr<LocalMetaSystem> meta_system_;
  bool initialized_{false};
};

TEST_F(LocalMetaSystemTest, OpenTruncZerosOldSlicesBeforeSparseRewrite) {
  constexpr int32_t kPageSize = 4096;
  auto attr = CreateFile("truncate-rewrite");
  WriteSlice(attr.ino, 0, 100, 0, kPageSize);

  bool keep_cache = false;
  auto status =
      meta_system_->Open(nullptr, attr.ino, O_TRUNC | O_WRONLY, 0, &keep_cache);
  ASSERT_TRUE(status.ok()) << status.ToString();

  Attr truncated_attr;
  status = meta_system_->GetAttr(nullptr, attr.ino, &truncated_attr);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(truncated_attr.length, 0);

  std::vector<Slice> slices;
  uint64_t version = 0;
  status = meta_system_->ReadSlice(nullptr, attr.ino, 0, 0, &slices, version);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(slices.size(), 2);
  EXPECT_EQ(slices[0].id, 100);
  EXPECT_EQ(slices[1].id, 0);
  EXPECT_EQ(slices[1].pos, 0);
  EXPECT_EQ(slices[1].len, kPageSize);

  WriteSlice(attr.ino, 0, 101, kPageSize, kPageSize);
  slices.clear();
  status = meta_system_->ReadSlice(nullptr, attr.ino, 0, 0, &slices, version);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(slices.size(), 3);
  EXPECT_EQ(slices[1].id, 0);
  EXPECT_EQ(slices[1].pos, 0);
  EXPECT_EQ(slices[1].len, kPageSize);
  EXPECT_EQ(slices[2].id, 101);
  EXPECT_EQ(slices[2].pos, kPageSize);
}

TEST_F(LocalMetaSystemTest, OpenTruncDoesNotMaterializeSparseChunks) {
  auto attr = CreateFile("truncate-sparse");
  WriteSlice(attr.ino, 0, 200, 0, 4096);

  Attr grown_attr;
  grown_attr.length = 2 * meta_system_->GetFsInfo().chunk_size();
  Attr out_attr;
  auto status = meta_system_->SetAttr(nullptr, attr.ino, kSetAttrSize,
                                      grown_attr, &out_attr);
  ASSERT_TRUE(status.ok()) << status.ToString();

  bool keep_cache = false;
  status =
      meta_system_->Open(nullptr, attr.ino, O_TRUNC | O_WRONLY, 0, &keep_cache);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::vector<Slice> slices;
  uint64_t version = 0;
  status = meta_system_->ReadSlice(nullptr, attr.ino, 1, 0, &slices, version);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_TRUE(slices.empty());
}

TEST_F(LocalMetaSystemTest, ReadDirWithAttrReturnsChildAttributes) {
  auto child = CreateFile("child");

  constexpr uint64_t kFh = 1;
  bool need_cache = false;
  auto status = meta_system_->OpenDir(nullptr, 1, kFh, need_cache);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::vector<DirEntry> entries;
  uint32_t count = 0;
  status = meta_system_->ReadDir(
      nullptr, 1, kFh, 0, true,
      [&entries](const DirEntry& entry, uint64_t) {
        entries.push_back(entry);
        return true;
      },
      count);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(entries.size(), 1);
  EXPECT_EQ(entries[0].ino, child.ino);
  EXPECT_EQ(entries[0].attr.ino, child.ino);
  EXPECT_EQ(entries[0].attr.type, FileType::kFile);

  status = meta_system_->ReleaseDir(nullptr, 1, kFh);
  EXPECT_TRUE(status.ok()) << status.ToString();
}

}  // namespace
}  // namespace local
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
