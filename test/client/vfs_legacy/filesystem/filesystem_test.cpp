
/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-04-03
 * Author: Jingli Chen (Wine93)
 */

#include "client/vfs_legacy/filesystem/filesystem.h"

#include <gtest/gtest.h>
#include <cstdint>

#include "client/vfs_legacy/filesystem/helper/helper.h"

namespace dingofs {
namespace client {
namespace filesystem {

class FileSystemTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(FileSystemTest, Lookup_Basic) {
  auto builder = FileSystemBuilder();
  auto fs = builder.Build();

  EXPECT_CALL_RETURN_GetDentry(*builder.GetDentryManager(), DINGOFS_ERROR::OK);
  EXPECT_CALL_RETURN_GetInodeAttr(*builder.GetInodeManager(),
                                  DINGOFS_ERROR::OK);

  EntryOut entryOut;
  auto rc = fs->Lookup(1, "f1", &entryOut);
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);
}

TEST_F(FileSystemTest, Lookup_NameTooLong) {
  auto builder = FileSystemBuilder();
  auto fs = builder
                .SetOption([](FileSystemOption* option) {
                  option->maxNameLength = 255;
                })
                .Build();

  EntryOut entryOut;
  auto rc = fs->Lookup(1, std::string(256, 'x'), &entryOut);
  ASSERT_EQ(rc, DINGOFS_ERROR::NAMETOOLONG);
}

TEST_F(FileSystemTest, Lookup_NegativeCache) {
  auto builder = FileSystemBuilder();
  auto fs = builder
                .SetOption([](FileSystemOption* option) {
                  option->lookupCacheOption.negativeTimeoutSec = 1;
                  option->lookupCacheOption.lruSize = 100000;
                })
                .Build();

  EXPECT_CALL_RETURN_GetDentry(*builder.GetDentryManager(),
                               DINGOFS_ERROR::NOTEXIST);

  EntryOut entryOut;
  auto rc = fs->Lookup(1, "f1", &entryOut);
  ASSERT_EQ(rc, DINGOFS_ERROR::NOTEXIST);

  rc = fs->Lookup(1, "f1", &entryOut);
  ASSERT_EQ(rc, DINGOFS_ERROR::NOTEXIST);
}

TEST_F(FileSystemTest, GetAttr_Basic) {
  auto builder = FileSystemBuilder();
  auto fs = builder.Build();

  EXPECT_CALL_INVOKE_GetInodeAttr(
      *builder.GetInodeManager(),
      [&](uint64_t ino, InodeAttr* attr) -> DINGOFS_ERROR {
        attr->set_inodeid(ino);
        attr->set_length(4096);
        attr->set_mtime(123);
        attr->set_mtime_ns(456);
        return DINGOFS_ERROR::OK;
      });

  AttrOut attrOut;
  auto rc = fs->GetAttr(100, &attrOut);
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);
  ASSERT_EQ(attrOut.attr.inodeid(), 100);
  ASSERT_EQ(attrOut.attr.length(), 4096);
  ASSERT_EQ(attrOut.attr.mtime(), 123);
  ASSERT_EQ(attrOut.attr.mtime_ns(), 456);
}

TEST_F(FileSystemTest, OpenDir_Basic) {
  auto builder = FileSystemBuilder();
  auto fs = builder.Build();

  EXPECT_CALL_RETURN_GetInodeAttr(*builder.GetInodeManager(),
                                  DINGOFS_ERROR::OK);

  uint64_t fh = 0;
  auto rc = fs->OpenDir(1, &fh);
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);
}

TEST_F(FileSystemTest, ReadDir_Basic) {
  auto builder = FileSystemBuilder();
  auto fs = builder.Build();

  // mock what opendir() does:
  auto handler = fs->NewHandler();
  uint64_t fh = handler->fh;

  // CASE 1: readdir success
  EXPECT_CALL_INVOKE_ListDentry(
      *builder.GetDentryManager(),
      [&](uint64_t parent, std::list<Dentry>* dentries, uint32_t limit,
          bool only, uint32_t nlink) -> DINGOFS_ERROR {
        dentries->push_back(MkDentry(1, "test"));
        return DINGOFS_ERROR::OK;
      });
  EXPECT_CALL_INVOKE_BatchGetInodeAttrAsync(
      *builder.GetInodeManager(),
      [&](uint64_t parentId, std::set<uint64_t>* inos,
          std::map<uint64_t, InodeAttr>* attrs) -> DINGOFS_ERROR {
        for (const auto& ino : *inos) {
          auto attr = MkAttr(ino, AttrOption().mtime(123, ino));
          attrs->emplace(ino, attr);
        }
        return DINGOFS_ERROR::OK;
      });

  DirEntry dirEntry;
  auto entries = std::make_shared<DirEntryList>();
  auto rc = fs->ReadDir(1, fh, &entries);
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);
  ASSERT_EQ(entries->Size(), 1);
  ASSERT_TRUE(entries->Get(1, &dirEntry));
  ASSERT_EQ(dirEntry.ino, 1);
  ASSERT_EQ(dirEntry.name, "test");
}

TEST_F(FileSystemTest, ReadDir_CheckEntries) {
  auto builder = FileSystemBuilder();
  auto fs = builder.Build();
  Ino ino(1);

  // mock what opendir() does:
  auto handler = fs->NewHandler();
  uint64_t fh = handler->fh;

  auto CHECK_ENTRIES = [&](const std::shared_ptr<DirEntryList>& entries) {
    std::vector<DirEntry> out;
    entries->Iterate([&](DirEntry* dirEntry) { out.push_back(*dirEntry); });
    ASSERT_EQ(out.size(), 3);

    int idx = 0;
    for (auto ino = 100; ino <= 102; ino++) {
      ASSERT_EQ(out[idx].ino, ino);
      ASSERT_EQ(out[idx].name, StrFormat("f%d", ino));
      ASSERT_EQ(out[idx].attr.mtime(), 123);
      ASSERT_EQ(out[idx].attr.mtime_ns(), ino);
      idx++;
    }
  };

  // CASE 1: check entries
  {
    EXPECT_CALL_INVOKE_ListDentry(
        *builder.GetDentryManager(),
        [&](uint64_t parent, std::list<Dentry>* dentries, uint32_t limit,
            bool only, uint32_t nlink) -> DINGOFS_ERROR {
          for (auto ino = 100; ino <= 102; ino++) {
            dentries->push_back(MkDentry(ino, StrFormat("f%d", ino)));
          }
          return DINGOFS_ERROR::OK;
        });

    EXPECT_CALL_INVOKE_BatchGetInodeAttrAsync(
        *builder.GetInodeManager(),
        [&](uint64_t parentId, std::set<uint64_t>* inos,
            std::map<uint64_t, InodeAttr>* attrs) -> DINGOFS_ERROR {
          for (const auto& ino : *inos) {
            auto attr = MkAttr(ino, AttrOption().mtime(123, ino));
            attrs->emplace(ino, attr);
          }
          return DINGOFS_ERROR::OK;
        });

    auto entries = std::make_shared<DirEntryList>();
    auto rc = fs->ReadDir(ino, fh, &entries);
    ASSERT_EQ(rc, DINGOFS_ERROR::OK);
    CHECK_ENTRIES(entries);
  }

  // CASE 2: check dir cache
  {
    // readdir from cache
    auto entries = std::make_shared<DirEntryList>();
    auto rc = fs->ReadDir(ino, fh, &entries);
    ASSERT_EQ(rc, DINGOFS_ERROR::OK);
    CHECK_ENTRIES(entries);
  }
}

TEST_F(FileSystemTest, ReleaseDir_Basic) {
  auto builder = FileSystemBuilder();
  auto fs = builder.Build();

  uint64_t fh = 0;
  auto rc = fs->ReleaseDir(fh);
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);
}

TEST_F(FileSystemTest, ReleaseDir_CheckHandler) {
  auto builder = FileSystemBuilder();
  auto fs = builder.Build();

  // mock what opendir() does:
  auto handler = fs->NewHandler();
  auto fh = handler->fh;

  // CASE 1: find handler success
  ASSERT_TRUE(fs->FindHandler(fh) != nullptr);

  // CASE 2: releasedir will release handler
  auto rc = fs->ReleaseDir(fh);
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);
  ASSERT_TRUE(fs->FindHandler(fh) == nullptr);
}

TEST_F(FileSystemTest, Open_Basic) {
  auto builder = FileSystemBuilder();
  auto fs = builder.Build();

  // mock what lookup() does:
  Ino ino(100);
  auto attrWatcher = fs->BorrowMember().attrWatcher;
  attrWatcher->RemeberMtime(MkAttr(ino, AttrOption().mtime(123, 456)));

  // CASE 1: open success
  {
    EXPECT_CALL_INVOKE_GetInode(
        *builder.GetInodeManager(),
        [&](uint64_t ino,
            std::shared_ptr<InodeWrapper>& inode) -> DINGOFS_ERROR {
          inode = MkInode(ino, InodeOption().mtime(123, 456));
          return DINGOFS_ERROR::OK;
        });

    auto rc = fs->Open(ino);
    ASSERT_EQ(rc, DINGOFS_ERROR::OK);
  }

  // CASE 2: file already opened
  {
    auto rc = fs->Open(ino);
    ASSERT_EQ(rc, DINGOFS_ERROR::OK);
  }
}

TEST_F(FileSystemTest, Open_Stale) {
  auto builder = FileSystemBuilder();
  auto fs = builder.Build();

  // mock lookup() does:
  Ino ino(100);
  auto attrWatcher = fs->BorrowMember().attrWatcher;
  attrWatcher->RemeberMtime(MkAttr(ino, AttrOption().mtime(123, 456)));

  // CASE 1: mtime(123, 456) != mtime(123, 789)
  EXPECT_CALL_INVOKE_GetInode(
      *builder.GetInodeManager(),
      [&](uint64_t ino, std::shared_ptr<InodeWrapper>& inode) -> DINGOFS_ERROR {
        inode = MkInode(ino, InodeOption().mtime(123, 789));
        return DINGOFS_ERROR::OK;
      });

  auto rc = fs->Open(ino);
  ASSERT_EQ(rc, DINGOFS_ERROR::STALE);
}

TEST_F(FileSystemTest, Open_StaleForAttrNotFound) {
  auto builder = FileSystemBuilder();
  auto fs = builder.Build();

  EXPECT_CALL_INVOKE_GetInode(
      *builder.GetInodeManager(),
      [&](uint64_t ino, std::shared_ptr<InodeWrapper>& inode) -> DINGOFS_ERROR {
        inode = MkInode(ino, InodeOption().mtime(123, 456));
        return DINGOFS_ERROR::OK;
      });

  Ino ino(100);
  auto rc = fs->Open(ino);
  ASSERT_EQ(rc, DINGOFS_ERROR::STALE);
}

TEST_F(FileSystemTest, Release_Basic) {
  auto builder = FileSystemBuilder();
  auto fs = builder.Build();
  auto rc = fs->Release(100);
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);
}

TEST_F(FileSystemTest, Release_CheckOpenStatus) {
  auto builder = FileSystemBuilder();
  auto fs = builder.Build();

  // mock what open() does:
  Ino ino(100);
  auto inode = MkInode(100);
  auto openfiles = fs->BorrowMember().openFiles;
  openfiles->Open(ino, inode);

  // CASE 1: ino(100) is opened
  auto out = MkInode(0);
  bool yes = openfiles->IsOpened(ino, &out);
  ASSERT_TRUE(yes);
  ASSERT_EQ(inode->GetInodeId(), ino);

  // CASE 2: release will close open file
  auto rc = fs->Release(100);
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);
  yes = openfiles->IsOpened(ino, &out);
  ASSERT_FALSE(yes);
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs
