
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

#include <fcntl.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>

#include "common/blockaccess/accesser_common.h"
#include "common/blockaccess/files/file_accesser.h"
#include "common/status.h"

namespace dingofs {
namespace blockaccess {
namespace unit_test {

class FileAccesserTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a temporary test directory
    test_root_ = "/tmp/file_accesser_test_" + std::to_string(getpid());
    std::filesystem::create_directories(test_root_);

    accesser_ = std::make_unique<FileAccesser>(test_root_);
    ASSERT_TRUE(accesser_->Init()) << "Failed to initialize FileAccesser";
  }

  void TearDown() override {
    if (accesser_) {
      accesser_->Destroy();
      accesser_.reset();
    }

    // Clean up test directory
    if (std::filesystem::exists(test_root_)) {
      std::filesystem::remove_all(test_root_);
    }
  }

  std::string GenerateRandomData(size_t length) {
    std::string data;
    data.reserve(length);
    for (size_t i = 0; i < length; ++i) {
      data.push_back(static_cast<char>('a' + (rand() % 26)));
    }
    return data;
  }

  std::string test_root_;
  std::unique_ptr<FileAccesser> accesser_;
};

// Test basic Put and Get operations
TEST_F(FileAccesserTest, PutAndGet) {
  const std::string key = "test_key_1";
  const std::string data = "Hello, DingoFS!";

  // Put data
  Status s = accesser_->Put(key, data.data(), data.size());
  ASSERT_TRUE(s.ok()) << "Put failed: " << s.ToString();

  // Get data back
  std::string retrieved_data;
  s = accesser_->Get(key, &retrieved_data);
  ASSERT_TRUE(s.ok()) << "Get failed: " << s.ToString();
  ASSERT_EQ(data, retrieved_data) << "Retrieved data doesn't match";
}

TEST_F(FileAccesserTest, PutLargeData) {
  const std::string key = "large_key";
  const size_t data_size = 4 * 1024 * 1024;
  std::string data = GenerateRandomData(data_size);

  Status s = accesser_->Put(key, data.data(), data.size());
  ASSERT_TRUE(s.ok()) << "Put large data failed: " << s.ToString();

  std::string retrieved_data;
  s = accesser_->Get(key, &retrieved_data);
  ASSERT_TRUE(s.ok()) << "Get large data failed: " << s.ToString();
  ASSERT_EQ(data.size(), retrieved_data.size()) << "Size mismatch";
  ASSERT_EQ(data, retrieved_data) << "Large data content mismatch";
}

// Test Get non-existent key
TEST_F(FileAccesserTest, GetNonExistent) {
  const std::string key = "non_existent_key";
  std::string data;

  Status s = accesser_->Get(key, &data);
  ASSERT_FALSE(s.ok()) << "Get non-existent key should fail";
  ASSERT_TRUE(s.IsNotFound()) << "Should return NotFound status";
}

// Test Range operation
TEST_F(FileAccesserTest, Range) {
  const std::string key = "range_key";
  const std::string data = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  // Put data first
  Status s = accesser_->Put(key, data.data(), data.size());
  ASSERT_TRUE(s.ok()) << "Put failed: " << s.ToString();

  // Test range read
  char buffer[10];
  s = accesser_->Range(key, 10, 10, buffer);
  ASSERT_TRUE(s.ok()) << "Range failed: " << s.ToString();
  ASSERT_EQ(std::string(buffer, 10), "ABCDEFGHIJ") << "Range data mismatch";

  // Test range at beginning
  s = accesser_->Range(key, 0, 5, buffer);
  ASSERT_TRUE(s.ok()) << "Range at beginning failed: " << s.ToString();
  ASSERT_EQ(std::string(buffer, 5), "01234") << "Range beginning mismatch";

  // Test range at end
  size_t end_offset = data.size() - 5;
  s = accesser_->Range(key, end_offset, 5, buffer);
  ASSERT_TRUE(s.ok()) << "Range at end failed: " << s.ToString();
  ASSERT_EQ(std::string(buffer, 5), "VWXYZ") << "Range end mismatch";
}

// Test Range with invalid offset
TEST_F(FileAccesserTest, RangeInvalidOffset) {
  const std::string key = "range_key_invalid";
  const std::string data = "Short data";

  Status s = accesser_->Put(key, data.data(), data.size());
  ASSERT_TRUE(s.ok()) << "Put failed: " << s.ToString();

  char buffer[100];
  // Offset beyond file size
  s = accesser_->Range(key, 100, 10, buffer);
  ASSERT_FALSE(s.ok()) << "Range with invalid offset should fail";
}

// Test BlockExist && Delete
TEST_F(FileAccesserTest, BlockExistAndDelete) {
  const std::string key = "delete_key";
  const std::string data = "To be deleted";

  // Put and verify
  Status s = accesser_->Put(key, data.data(), data.size());
  ASSERT_TRUE(s.ok()) << "Put failed: " << s.ToString();
  ASSERT_TRUE(accesser_->BlockExist(key)) << "Key should exist";

  // Delete
  s = accesser_->Delete(key);
  ASSERT_TRUE(s.ok()) << "Delete failed: " << s.ToString();

  // Verify deletion
  ASSERT_FALSE(accesser_->BlockExist(key))
      << "Key should not exist after delete";

  std::string retrieved_data;
  s = accesser_->Get(key, &retrieved_data);
  ASSERT_FALSE(s.ok()) << "Get deleted key should fail";
}

// Test Delete non-existent key (should succeed silently)
TEST_F(FileAccesserTest, DeleteNonExistent) {
  const std::string key = "non_existent_delete_key";

  Status s = accesser_->Delete(key);
  ASSERT_TRUE(s.ok()) << "Delete non-existent key should succeed";
}

// Test BatchDelete
TEST_F(FileAccesserTest, BatchDelete) {
  std::list<std::string> keys = {"batch_key_1", "batch_key_2", "batch_key_3"};
  const std::string data = "Batch data";

  // Put all keys
  for (const auto& key : keys) {
    Status s = accesser_->Put(key, data.data(), data.size());
    ASSERT_TRUE(s.ok()) << "Put failed for key: " << key;
  }

  // Verify all exist
  for (const auto& key : keys) {
    ASSERT_TRUE(accesser_->BlockExist(key)) << "Key should exist: " << key;
  }

  // Batch delete
  Status s = accesser_->BatchDelete(keys);
  ASSERT_TRUE(s.ok()) << "BatchDelete failed: " << s.ToString();

  // Verify all deleted
  for (const auto& key : keys) {
    ASSERT_FALSE(accesser_->BlockExist(key))
        << "Key should be deleted: " << key;
  }
}

// Test AsyncPut
TEST_F(FileAccesserTest, AsyncPut) {
  const std::string key = "async_put_key";
  const std::string data = "Async put data";

  std::mutex mtx;
  std::condition_variable cv;
  bool callback_called = false;
  Status async_status;

  auto context = std::make_shared<PutObjectAsyncContext>();
  context->key = key;
  context->buffer = data.data();
  context->buffer_size = data.size();
  context->cb = [&](const PutObjectAsyncContextSPtr& ctx) {
    std::lock_guard<std::mutex> lock(mtx);
    async_status = ctx->status;
    callback_called = true;
    cv.notify_one();
  };

  // Execute async put
  accesser_->AsyncPut(context);

  // Wait for callback
  {
    std::unique_lock<std::mutex> lock(mtx);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&] {
      return callback_called;
    })) << "AsyncPut callback timeout";
  }

  ASSERT_TRUE(async_status.ok())
      << "AsyncPut failed: " << async_status.ToString();

  // Verify data was written
  std::string retrieved_data;
  Status s = accesser_->Get(key, &retrieved_data);
  ASSERT_TRUE(s.ok()) << "Get after AsyncPut failed";
  ASSERT_EQ(data, retrieved_data) << "AsyncPut data mismatch";
}

// Test AsyncGet
TEST_F(FileAccesserTest, AsyncGet) {
  const std::string key = "async_get_key";
  const std::string data = "Async get data";

  // Put data first
  Status s = accesser_->Put(key, data.data(), data.size());
  ASSERT_TRUE(s.ok()) << "Put failed: " << s.ToString();

  std::mutex mtx;
  std::condition_variable cv;
  bool callback_called = false;
  Status async_status;
  std::vector<char> buffer(data.size());

  auto context = std::make_shared<GetObjectAsyncContext>();
  context->key = key;
  context->buf = buffer.data();
  context->offset = 0;
  context->len = data.size();
  context->cb = [&](const GetObjectAsyncContextSPtr& ctx) {
    std::lock_guard<std::mutex> lock(mtx);
    async_status = ctx->status;
    callback_called = true;
    cv.notify_one();
  };

  // Execute async get
  accesser_->AsyncGet(context);

  // Wait for callback
  {
    std::unique_lock<std::mutex> lock(mtx);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&] {
      return callback_called;
    })) << "AsyncGet callback timeout";
  }

  ASSERT_TRUE(async_status.ok())
      << "AsyncGet failed: " << async_status.ToString();
  ASSERT_EQ(context->actual_len, data.size()) << "AsyncGet size mismatch";
  ASSERT_EQ(std::string(buffer.data(), context->actual_len), data)
      << "AsyncGet data mismatch";
}

// Test AsyncPut with multiple concurrent requests
TEST_F(FileAccesserTest, AsyncPutConcurrent) {
  const int num_requests = 10;
  std::mutex mtx;
  std::condition_variable cv;
  int completed_count = 0;
  std::vector<Status> statuses(num_requests);

  for (int i = 0; i < num_requests; ++i) {
    std::string key = "concurrent_key_" + std::to_string(i);
    std::string data = "Concurrent data " + std::to_string(i);

    auto context = std::make_shared<PutObjectAsyncContext>();
    context->key = key;
    context->buffer = data.data();
    context->buffer_size = data.size();
    context->cb = [&, i, key, data](const PutObjectAsyncContextSPtr& ctx) {
      std::lock_guard<std::mutex> lock(mtx);
      statuses[i] = ctx->status;
      ++completed_count;
      cv.notify_one();
    };

    accesser_->AsyncPut(context);
  }

  // Wait for all callbacks
  {
    std::unique_lock<std::mutex> lock(mtx);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10),
                            [&] { return completed_count == num_requests; }))
        << "Timeout waiting for concurrent AsyncPut, completed: "
        << completed_count;
  }

  // Verify all succeeded
  for (int i = 0; i < num_requests; ++i) {
    ASSERT_TRUE(statuses[i].ok())
        << "AsyncPut " << i << " failed: " << statuses[i].ToString();
  }
}

// Test ContainerExist
TEST_F(FileAccesserTest, ContainerExist) {
  ASSERT_TRUE(accesser_->ContainerExist()) << "Container should exist";

  // Test with non-existent container
  FileAccesser non_existent_accesser("/tmp/non_existent_container_test");
  ASSERT_FALSE(non_existent_accesser.ContainerExist())
      << "Non-existent container should return false";
}

// Test Init creates directory
TEST_F(FileAccesserTest, InitCreatesDirectory) {
  std::string new_root =
      "/tmp/file_accesser_init_test_" + std::to_string(getpid());

  // Ensure directory doesn't exist
  std::filesystem::remove_all(new_root);
  ASSERT_FALSE(std::filesystem::exists(new_root));

  // Init should create it
  FileAccesser new_accesser(new_root);
  ASSERT_TRUE(new_accesser.Init()) << "Init should create directory";
  ASSERT_TRUE(std::filesystem::exists(new_root))
      << "Directory should be created";

  // Cleanup
  new_accesser.Destroy();
  std::filesystem::remove_all(new_root);
}

// Test multiple Put/Get cycles
TEST_F(FileAccesserTest, MultiplePutGetCycles) {
  const std::string key = "cycle_key";
  const int num_cycles = 100;

  for (int i = 0; i < num_cycles; ++i) {
    std::string data = "Cycle " + std::to_string(i) + " data";

    Status s = accesser_->Put(key, data.data(), data.size());
    ASSERT_TRUE(s.ok()) << "Put failed at cycle " << i;

    std::string retrieved_data;
    s = accesser_->Get(key, &retrieved_data);
    ASSERT_TRUE(s.ok()) << "Get failed at cycle " << i;
    ASSERT_EQ(data, retrieved_data) << "Data mismatch at cycle " << i;
  }
}

}  // namespace unit_test
}  // namespace blockaccess
}  // namespace dingofs
