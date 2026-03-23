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

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "client/vfs/components/file_suffix_watcher.h"
#include "test/unit/client/vfs/test_common.h"

namespace dingofs {
namespace client {
namespace vfs {

TEST(FileSuffixWatcherTest, EmptySuffix_NeverWriteback) {
  FileSuffixWatcher watcher("");
  Attr attr = test::MakeFileAttr(100);
  watcher.Remeber(attr, "test.h");
  EXPECT_FALSE(watcher.ShouldWriteback(100));
}

TEST(FileSuffixWatcherTest, SingleSuffix_Match_True) {
  FileSuffixWatcher watcher(".txt");
  Attr attr = test::MakeFileAttr(200);
  watcher.Remeber(attr, "test.txt");
  EXPECT_TRUE(watcher.ShouldWriteback(200));
}

TEST(FileSuffixWatcherTest, SingleSuffix_NoMatch_False) {
  FileSuffixWatcher watcher(".txt");
  Attr attr = test::MakeFileAttr(300);
  watcher.Remeber(attr, "test.h");
  EXPECT_FALSE(watcher.ShouldWriteback(300));
}

TEST(FileSuffixWatcherTest, Forget_RemovesIno) {
  FileSuffixWatcher watcher(".txt");
  Attr attr = test::MakeFileAttr(400);
  watcher.Remeber(attr, "readme.txt");
  ASSERT_TRUE(watcher.ShouldWriteback(400));

  watcher.Forget(400);
  EXPECT_FALSE(watcher.ShouldWriteback(400));
}

TEST(FileSuffixWatcherTest, MultipleSuffixes_AnyMatch) {
  FileSuffixWatcher watcher(".txt:.h");
  Attr attr_txt = test::MakeFileAttr(500);
  Attr attr_h = test::MakeFileAttr(501);
  Attr attr_cc = test::MakeFileAttr(502);

  watcher.Remeber(attr_txt, "readme.txt");
  watcher.Remeber(attr_h, "header.h");
  watcher.Remeber(attr_cc, "source.cc");

  EXPECT_TRUE(watcher.ShouldWriteback(500));
  EXPECT_TRUE(watcher.ShouldWriteback(501));
  EXPECT_FALSE(watcher.ShouldWriteback(502));
}

TEST(FileSuffixWatcherTest, Concurrent_RememberAndCheck_NoCrash) {
  FileSuffixWatcher watcher(".dat");

  constexpr int kWriters = 4;
  constexpr int kReaders = 4;
  constexpr int kIters = 500;

  std::vector<std::thread> threads;
  threads.reserve(kWriters + kReaders);

  // Writer threads: Remember different inos
  for (int t = 0; t < kWriters; ++t) {
    threads.emplace_back([&watcher, t]() {
      for (int i = 0; i < kIters; ++i) {
        Ino ino = static_cast<Ino>(t * kIters + i + 1000);
        Attr attr = test::MakeFileAttr(ino);
        watcher.Remeber(attr, "file.dat");
      }
    });
  }

  // Reader threads: ShouldWriteback — just must not crash
  for (int t = 0; t < kReaders; ++t) {
    threads.emplace_back([&watcher, t]() {
      for (int i = 0; i < kIters; ++i) {
        Ino ino = static_cast<Ino>(t * kIters + i + 1000);
        // Result is irrelevant; we only check for absence of data races
        (void)watcher.ShouldWriteback(ino);
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }
  // Reaching here means no crash / data race detected
  SUCCEED();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
