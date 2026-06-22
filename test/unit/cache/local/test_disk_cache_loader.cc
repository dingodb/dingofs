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

/*
 * Project: DingoFS
 * Created Date: 2026-06-21
 * Author: AI
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cache/local/cache_store.h"
#include "cache/local/disk_cache_layout.h"
#include "cache/local/disk_cache_loader.h"
#include "cache/local/disk_cache_manager.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"

namespace dingofs {
namespace cache {

class DiskCacheLoaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static int seq = 0;
    cache_index_ = 200 + (seq++);
    root_dir_ = "/tmp/dingofs_test_disk_cache_loader_" +
                std::to_string(getpid()) + "_" + std::to_string(cache_index_);
    std::filesystem::create_directories(root_dir_);
    layout_ = std::make_shared<DiskCacheLayout>(cache_index_, root_dir_);
    manager_ =
        std::make_shared<DiskCacheManager>(100ULL * 1024 * 1024, layout_);
  }

  void TearDown() override { std::filesystem::remove_all(root_dir_); }

  void WriteFileAt(const std::string& path, const std::string& content) {
    std::filesystem::create_directories(
        std::filesystem::path(path).parent_path());
    std::ofstream ofs(path);
    ofs << content;
    ofs.close();
  }

  template <typename Pred>
  bool WaitUntil(Pred pred, int timeout_ms = 5000) {
    for (int waited = 0; waited < timeout_ms; waited += 10) {
      if (pred()) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
  }

  int cache_index_;
  std::string root_dir_;
  DiskCacheLayoutSPtr layout_;
  DiskCacheManagerSPtr manager_;
};

TEST_F(DiskCacheLoaderTest, LoadsStageAndCacheBlocks) {
  const std::string disk_id = "950c9813-ea26-4726-96fd-383b0cd22b20";

  BlockHandle stage_handle(1, BlockKey(4098, 1, 4194304));
  BlockHandle cache_handle(0, BlockKey(100, 0, 4096));

  std::string stage_dir = layout_->GetStageDir();
  std::string cache_dir = layout_->GetCacheDir();

  // Valid stage + cache blocks (placed under the layout the loader walks).
  WriteFileAt(stage_dir + "/1/blocks/0/4/4098_1_4194304", "stage-data");
  WriteFileAt(cache_dir + "/blocks/0/0/100_0_4096", "cache-data");
  // An unparseable name and a temp file: both must be removed.
  std::string invalid = stage_dir + "/1/blocks/0/4/not-a-valid-name";
  std::string temp = stage_dir + "/1/blocks/0/4/4098_2_4194304.999.tmp";
  WriteFileAt(invalid, "junk");
  WriteFileAt(temp, "partial");

  std::mutex mu;
  std::vector<std::tuple<std::string, size_t, BlockAttr>> uploads;
  auto uploader = [&](BlockHandle handle, size_t length, BlockAttr attr) {
    std::lock_guard<std::mutex> lk(mu);
    uploads.emplace_back(handle.Filename(), length, attr);
  };

  DiskCacheLoader loader(layout_, manager_);
  loader.Start(disk_id, uploader);

  bool loaded = WaitUntil([&]() {
    return manager_->Exist(stage_handle) && manager_->Exist(cache_handle) &&
           !std::filesystem::exists(invalid) && !std::filesystem::exists(temp);
  });

  loader.Shutdown();
  ASSERT_TRUE(loaded);

  // Stage block triggers exactly one upload, tagged as a reload from this disk.
  {
    std::lock_guard<std::mutex> lk(mu);
    ASSERT_EQ(uploads.size(), 1u);
    EXPECT_EQ(std::get<0>(uploads[0]), "4098_1_4194304");
    EXPECT_EQ(std::get<2>(uploads[0]).from, BlockAttr::kFromReload);
    EXPECT_EQ(std::get<2>(uploads[0]).store_id, disk_id);
  }

  // The valid block files are kept on disk.
  EXPECT_TRUE(
      std::filesystem::exists(stage_dir + "/1/blocks/0/4/4098_1_4194304"));
  EXPECT_TRUE(std::filesystem::exists(cache_dir + "/blocks/0/0/100_0_4096"));
}

TEST_F(DiskCacheLoaderTest, EmptyDirectoriesLoadNothing) {
  std::mutex mu;
  int upload_count = 0;
  auto uploader = [&](BlockHandle, size_t, BlockAttr) {
    std::lock_guard<std::mutex> lk(mu);
    upload_count++;
  };

  DiskCacheLoader loader(layout_, manager_);
  loader.Start("disk-empty", uploader);

  // Loading finishes (cache flag clears) even with no stage/cache dirs present.
  EXPECT_TRUE(WaitUntil([&]() { return !loader.StillLoading(); }));

  loader.Shutdown();

  std::lock_guard<std::mutex> lk(mu);
  EXPECT_EQ(upload_count, 0);
}

}  // namespace cache
}  // namespace dingofs
