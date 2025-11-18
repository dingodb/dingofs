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

#include <glog/logging.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "mds/common/helper.h"
#include "mds/filesystem/dentry.h"

namespace dingofs {
namespace mds {
namespace unit_test {

const int64_t kFsId = 1000;

class DentryTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(DentryTest, Put) {
  Dentry dentry(kFsId, "file1", 1, 1000, pb::mds::FileType::FILE, 1212,
                nullptr);

  ASSERT_EQ("file1", dentry.Name());
  ASSERT_EQ(1000, dentry.INo());
  ASSERT_EQ(pb::mds::FileType::FILE, dentry.Type());
  ASSERT_EQ(1212, dentry.Flag());
  ASSERT_EQ(nullptr, dentry.Inode());
}

class FileStore {
 public:
  FileStore() { files_.reserve(1000 * 1000); }
  ~FileStore() = default;

  void AddFile(const std::string& file_name) {
    std::lock_guard<std::mutex> lg(mutex_);

    ++version_;
    files_.push_back(file_name);
  }

  void GetFiles(uint64_t& version, std::vector<std::string_view>& result) {
    std::lock_guard<std::mutex> lg(mutex_);

    version = version_;

    for (auto& file : files_) {
      result.push_back(file);
    }
  }

 private:
  std::mutex mutex_;
  uint64_t version_ = 0;
  std::vector<std::string> files_;
};

using FileStoreSPtr = std::shared_ptr<FileStore>;

TEST_F(DentryTest, ReadDir) {
  FileStoreSPtr file_store = std::make_shared<FileStore>();

  const std::string sandbox =
      "/home/dengzihui/mount-test/dengzh_hash_01-1/test4";

  // create thread readdir
  int readdir_thread_num = 10;
  std::vector<std::thread> readdir_threads;
  for (int i = 0; i < readdir_thread_num; ++i) {
    std::thread t([sandbox, thread_no = i, file_store]() {
      while (true) {
        uint64_t version = 0;
        std::vector<std::string_view> filenames;
        file_store->GetFiles(version, filenames);

        std::set<std::string> read_filenames;
        std::vector<std::string> read_filename_vec;
        uint64_t dentry_count = 0;
        for (auto const& dir_entry :
             std::filesystem::directory_iterator{sandbox}) {
          // LOG(INFO) << fmt::format("[readdir.{}] path: {}.", thread_no,
          //                          dir_entry.path().string());
          read_filename_vec.push_back(dir_entry.path().string());
          auto result = read_filenames.insert(dir_entry.path().string());
          if (!result.second) {
            LOG(ERROR) << fmt::format("[readdir.{}] duplicate filename: {}.",
                                      thread_no, dir_entry.path().string());
          }
          ++dentry_count;
        }

        LOG(INFO) << fmt::format(
            "[readdir.{}] count: {} filenames_count({}) read_filename_vec({}).",
            thread_no, dentry_count, filenames.size(),
            read_filename_vec.size());

        // check filen exist
        for (auto& filename : filenames) {
          if (read_filenames.find(std::string(filename)) ==
              read_filenames.end()) {
            for (const auto& filename : read_filenames) {
              LOG(INFO) << fmt::format("[readdir.{}] read_filename: {}.",
                                       thread_no, filename);
            }

            for (const auto& filename : read_filename_vec) {
              LOG(INFO) << fmt::format("[readdir.{}] read_filename_vec: {}.",
                                       thread_no, filename);
            }

            LOG(FATAL) << fmt::format("[readdir.{}] filename not exist: {}.",
                                      thread_no, filename);
          }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    });

    readdir_threads.push_back(std::move(t));
  }

  // create thread for create file
  int createfile_thread_num = 3;
  std::vector<std::thread> createfile_threads;
  for (int i = 0; i < createfile_thread_num; ++i) {
    std::thread t([sandbox, thread_no = i, file_store]() {
      for (size_t j = 0; j < 10000; ++j) {
        const std::string file_path = fmt::format(
            "{}/file_{}_{}", sandbox, Helper::GenerateRandomString(32), j);

        std::ofstream ofs(file_path);
        // ofs << "this is a test file." << '\n';
        ofs.close();

        file_store->AddFile(file_path);

        // sleep for a while
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      }
    });

    createfile_threads.push_back(std::move(t));
  }

  for (auto& t : createfile_threads) {
    t.join();
  }

  for (auto& t : readdir_threads) {
    t.join();
  }
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
