/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Project: dingo
 * Date: Friday Oct 22 15:09:30 CST 2021
 * Author: huyao
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "client/vfs_legacy/mock_client_s3_cache_manager.h"
#include "client/vfs_legacy/s3/client_s3_adaptor.h"
#include "client/vfs_legacy/s3/client_s3_cache_manager.h"
#include "options/client/vfs_legacy/vfs_legacy_option.h"
#include "utils/concurrent/count_down_event.h"

namespace dingofs {
namespace client {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

class FsCacheManagerTest : public testing::Test {
 public:
  FsCacheManagerTest() = default;

  ~FsCacheManagerTest() override = default;

  void SetUp() override {
    maxReadCacheByte_ = 16ull * 1024 * 1024;  // 16MiB
    uint64_t maxWriteCacheByte = maxReadCacheByte_;
    S3ClientAdaptorOption option;
    option.blockSize = 1 * 1024 * 1024;
    option.chunkSize = 4 * 1024 * 1024;
    option.baseSleepUs = 500;
    option.pageSize = 64 * 1024;
    option.intervalMs = 5000 * 1000;
    option.flushIntervalSec = 5000;
    option.readCacheMaxByte = 104857600;
    option.readCacheThreads = 5;
    s3ClientAdaptor_ = new S3ClientAdaptorImpl();
    fsCacheManager_ = std::make_shared<FsCacheManager>(
        s3ClientAdaptor_, maxReadCacheByte_, maxWriteCacheByte,
        option.readCacheThreads, nullptr);
    s3ClientAdaptor_->Init(option, nullptr, nullptr, nullptr, fsCacheManager_,
                           nullptr, nullptr, nullptr);
    s3ClientAdaptor_->SetFsId(2);

    mockChunkCacheManager_ = std::make_shared<MockChunkCacheManager>();
  }

  void TearDown() override { delete s3ClientAdaptor_; }

 protected:
  S3ClientAdaptorImpl* s3ClientAdaptor_;
  std::shared_ptr<FsCacheManager> fsCacheManager_;
  std::shared_ptr<MockChunkCacheManager> mockChunkCacheManager_;
  uint64_t maxReadCacheByte_;
};

TEST_F(FsCacheManagerTest, test_FindFileCacheManager) {
  uint64_t inodeId = 1;
  uint64_t fsId = 1;

  ASSERT_EQ(nullptr, fsCacheManager_->FindFileCacheManager(inodeId));
  auto fileCacheManager =
      fsCacheManager_->FindOrCreateFileCacheManager(fsId, inodeId);
  ASSERT_EQ(fileCacheManager, fsCacheManager_->FindFileCacheManager(inodeId));
}

TEST_F(FsCacheManagerTest, test_FindOrCreateFileCacheManager) {
  uint64_t inodeId = 1;
  uint64_t fsId = 1;

  auto fileCacheManager =
      fsCacheManager_->FindOrCreateFileCacheManager(fsId, inodeId);
  ASSERT_EQ(fileCacheManager,
            fsCacheManager_->FindOrCreateFileCacheManager(fsId, inodeId));
}

TEST_F(FsCacheManagerTest, test_ReleaseFileCacheManager) {
  uint64_t inodeId = 1;
  uint64_t fsId = 1;

  fsCacheManager_->ReleaseFileCacheManager(inodeId);
  ASSERT_EQ(nullptr, fsCacheManager_->FindFileCacheManager(inodeId));
  auto fileCacheManager =
      fsCacheManager_->FindOrCreateFileCacheManager(fsId, inodeId);
  ASSERT_EQ(fileCacheManager, fsCacheManager_->FindFileCacheManager(inodeId));
  fsCacheManager_->ReleaseFileCacheManager(inodeId);
  ASSERT_EQ(nullptr, fsCacheManager_->FindFileCacheManager(inodeId));
}

TEST_F(FsCacheManagerTest, test_lru_set_and_delete) {
  uint64_t smallDataCacheByte = 128ull * 1024;  // 128KiB
  uint64_t dataCacheByte = 4ull * 1024 * 1024;  // 4MiB
  char* buf = new char[dataCacheByte];
  std::list<DataCachePtr>::iterator outIter;

  {
    for (size_t i = 0; i < maxReadCacheByte_ / smallDataCacheByte; ++i) {
      fsCacheManager_->Set(
          std::make_shared<DataCache>(s3ClientAdaptor_, mockChunkCacheManager_,
                                      0, smallDataCacheByte, buf, nullptr),
          &outIter);
    }
  }

  {
    const uint32_t expectCallTimes = 1;
    dingofs::utils::CountDownEvent counter(expectCallTimes);

    EXPECT_CALL(*mockChunkCacheManager_, ReleaseReadDataCache(_))
        .Times(expectCallTimes)
        .WillRepeatedly(Invoke([&counter](uint64_t) { counter.Signal(); }));
    fsCacheManager_->Set(
        std::make_shared<DataCache>(s3ClientAdaptor_, mockChunkCacheManager_, 0,
                                    dataCacheByte, buf, nullptr),
        &outIter);

    counter.Wait();
  }

  {
    const uint32_t expectCallTimes = 32;
    dingofs::utils::CountDownEvent counter(expectCallTimes);

    EXPECT_CALL(*mockChunkCacheManager_, ReleaseReadDataCache(_))
        .Times(expectCallTimes)
        .WillRepeatedly(Invoke([&counter](uint64_t) { counter.Signal(); }));

    fsCacheManager_->Set(
        std::make_shared<DataCache>(s3ClientAdaptor_, mockChunkCacheManager_, 0,
                                    dataCacheByte, buf, nullptr),
        &outIter);
    counter.Wait();
  }
}

TEST_F(FsCacheManagerTest, test_fsSync_ok) {
  uint64_t inodeId = 1;
  auto fileCache = std::make_shared<MockFileCacheManager>();

  EXPECT_CALL(*fileCache, Flush(_, _)).WillOnce(Return(DINGOFS_ERROR::OK));
  fsCacheManager_->SetFileCacheManagerForTest(inodeId, fileCache);
  ASSERT_EQ(DINGOFS_ERROR::OK, fsCacheManager_->FsSync(true));
}

TEST_F(FsCacheManagerTest, test_fsSync_not_exist) {
  uint64_t inodeId = 1;

  auto fileCache = std::make_shared<MockFileCacheManager>();
  EXPECT_CALL(*fileCache, Flush(_, _))
      .WillOnce(Return(DINGOFS_ERROR::NOTEXIST));
  fsCacheManager_->SetFileCacheManagerForTest(inodeId, fileCache);
  ASSERT_EQ(fileCache, fsCacheManager_->FindFileCacheManager(inodeId));
  ASSERT_EQ(DINGOFS_ERROR::OK, fsCacheManager_->FsSync(true));
  ASSERT_EQ(nullptr, fsCacheManager_->FindFileCacheManager(inodeId));
}

TEST_F(FsCacheManagerTest, test_fsSync_fail) {
  uint64_t inodeId = 1;
  auto fileCache = std::make_shared<MockFileCacheManager>();

  EXPECT_CALL(*fileCache, Flush(_, _))
      .WillOnce(Return(DINGOFS_ERROR::INTERNAL));
  fsCacheManager_->SetFileCacheManagerForTest(inodeId, fileCache);
  ASSERT_EQ(DINGOFS_ERROR::INTERNAL, fsCacheManager_->FsSync(true));
}

}  // namespace client
}  // namespace dingofs
