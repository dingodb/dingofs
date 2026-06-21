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

#include "cache/common/storage_client_pool.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/io_buffer.h"
#include "cache/common/storage_client.h"
#include "test/unit/cache/common/mock/mock_mds_client.h"
#include "test/unit/common/blockaccess/mock/mock_accesser.h"

namespace dingofs {
namespace cache {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrictMock;
using blockaccess::MockBlockAccesser;

namespace {

BlockHandle Handle(uint64_t id) {
  return BlockHandle(1, BlockKey(id, 0, 4096));
}

IOBuffer Buffer(const std::string& data) {
  return IOBuffer(data.data(), data.size());
}

pb::mds::FsInfo MakeLocalFileFsInfo(uint64_t fs_id, const std::string& name,
                                    const std::string& root) {
  pb::mds::FsInfo fs_info;
  fs_info.set_fs_id(fs_id);
  fs_info.set_fs_name(name);
  fs_info.set_fs_type(pb::mds::FsType::LOCALFILE);
  fs_info.mutable_extra()->mutable_file_info()->set_path(root);
  return fs_info;
}

}  // namespace

class StorageClientPoolImplTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = (std::filesystem::temp_directory_path() /
             ("dingofs_test_storage_client_pool_" +
              std::to_string(::getpid())))
                .string();
    std::filesystem::remove_all(root_);
  }

  void TearDown() override { std::filesystem::remove_all(root_); }

  std::string root_;
};

TEST(SingletonStorageClientTest, AlwaysReturnsSameClient) {
  MockBlockAccesser accesser;
  StorageClient storage_client(&accesser);
  SingletonStorageClient pool(&storage_client);

  // fs_id is ignored: every call yields the single wrapped client.
  StorageClient* out = nullptr;
  ASSERT_TRUE(pool.GetStorageClient(1, &out).ok());
  EXPECT_EQ(out, &storage_client);

  StorageClient* out2 = nullptr;
  ASSERT_TRUE(pool.GetStorageClient(99999, &out2).ok());
  EXPECT_EQ(out2, &storage_client);
}

TEST_F(StorageClientPoolImplTest, GetStorageClientCachesByFsId) {
  auto mds = std::make_shared<StrictMock<MockMDSClient>>();
  auto fs_info = MakeLocalFileFsInfo(42, "cache-pool-fs", root_);

  EXPECT_CALL(*mds, GetFSInfo(42, _))
      .WillOnce(Invoke([&fs_info](uint64_t, pb::mds::FsInfo* out) {
        *out = fs_info;
        return Status::OK();
      }));

  StorageClientPoolImpl pool(mds);
  StorageClient* first = nullptr;
  ASSERT_TRUE(pool.GetStorageClient(42, &first).ok());
  ASSERT_NE(first, nullptr);

  StorageClient* second = nullptr;
  ASSERT_TRUE(pool.GetStorageClient(42, &second).ok());
  EXPECT_EQ(second, first);

  ASSERT_TRUE(first->Shutdown().ok());
}

TEST_F(StorageClientPoolImplTest, CreatedClientCanAccessLocalFileBackend) {
  auto mds = std::make_shared<StrictMock<MockMDSClient>>();
  auto fs_info = MakeLocalFileFsInfo(7, "cache-local-fs", root_);

  EXPECT_CALL(*mds, GetFSInfo(7, _))
      .WillOnce(Invoke([&fs_info](uint64_t, pb::mds::FsInfo* out) {
        *out = fs_info;
        return Status::OK();
      }));

  StorageClientPoolImpl pool(mds);
  StorageClient* client = nullptr;
  ASSERT_TRUE(pool.GetStorageClient(7, &client).ok());
  ASSERT_NE(client, nullptr);

  const std::string data = "storage-client-pool";
  ASSERT_TRUE(client->Put(Handle(700), Buffer(data)).ok());

  std::string out(data.size(), '\0');
  IOBuffer out_buffer(out.data(), out.size());
  ASSERT_TRUE(client->Range(Handle(700), 0, out.size(), &out_buffer).ok());
  out_buffer.CopyTo(out.data(), out.size());
  EXPECT_EQ(out, data);

  ASSERT_TRUE(client->Shutdown().ok());
}

TEST_F(StorageClientPoolImplTest, GetStorageClientReturnsMdsError) {
  auto mds = std::make_shared<StrictMock<MockMDSClient>>();
  EXPECT_CALL(*mds, GetFSInfo(404, _))
      .WillOnce(Return(Status::NotFound("fs not found")));

  StorageClientPoolImpl pool(mds);
  StorageClient* client = nullptr;
  EXPECT_TRUE(pool.GetStorageClient(404, &client).IsNotFound());
  EXPECT_EQ(client, nullptr);
}

TEST_F(StorageClientPoolImplTest, GetStorageClientReturnsInitError) {
  auto mds = std::make_shared<StrictMock<MockMDSClient>>();
  auto fs_info = MakeLocalFileFsInfo(11, "cache-invalid-fs", "");

  EXPECT_CALL(*mds, GetFSInfo(11, _))
      .WillOnce(Invoke([&fs_info](uint64_t, pb::mds::FsInfo* out) {
        *out = fs_info;
        return Status::OK();
      }));

  StorageClientPoolImpl pool(mds);
  StorageClient* client = nullptr;
  EXPECT_TRUE(pool.GetStorageClient(11, &client).IsInternal());
  EXPECT_EQ(client, nullptr);
}

TEST_F(StorageClientPoolImplTest, ConcurrentGetCreatesClientOnce) {
  auto mds = std::make_shared<StrictMock<MockMDSClient>>();
  auto fs_info = MakeLocalFileFsInfo(9, "cache-concurrent-fs", root_);

  EXPECT_CALL(*mds, GetFSInfo(9, _))
      .Times(1)
      .WillOnce(Invoke([&fs_info](uint64_t, pb::mds::FsInfo* out) {
        *out = fs_info;
        return Status::OK();
      }));

  StorageClientPoolImpl pool(mds);
  std::vector<StorageClient*> clients(8, nullptr);
  std::vector<std::thread> threads;
  threads.reserve(clients.size());

  for (size_t i = 0; i < clients.size(); i++) {
    threads.emplace_back([&pool, &clients, i]() {
      EXPECT_TRUE(pool.GetStorageClient(9, &clients[i]).ok());
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  ASSERT_NE(clients[0], nullptr);
  for (auto* client : clients) {
    EXPECT_EQ(client, clients[0]);
  }

  ASSERT_TRUE(clients[0]->Shutdown().ok());
}

}  // namespace cache
}  // namespace dingofs
