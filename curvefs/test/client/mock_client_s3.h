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
 * Created Date: Thur Jun 22 2021
 * Author: huyao
 */

#ifndef CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_H_
#define CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "curvefs/src/client/blockcache/block_cache.h"
#include "curvefs/src/client/blockcache/s3_client.h"

using ::testing::_;
using ::testing::Return;

namespace curvefs {
namespace client {

using ::curve::common::S3AdapterOption;
using ::curvefs::client::blockcache::BCACHE_ERROR;
using ::curvefs::client::blockcache::GetObjectAsyncContext;
using ::curvefs::client::blockcache::PutObjectAsyncContext;
using ::curvefs::client::blockcache::S3Client;

class MockS3Client : public S3Client {
 public:
  MockS3Client() {}

  ~MockS3Client() {}

  MOCK_METHOD1(Init, void(const S3AdapterOption& options));

  MOCK_METHOD0(Destroy, void());

  MOCK_METHOD3(Put, BCACHE_ERROR(const std::string& key, const char* buffer,
                                 size_t length));

  MOCK_METHOD4(Range, BCACHE_ERROR(const std::string& key, off_t offset,
                                   size_t length, char* buffer));

  MOCK_METHOD4(AsyncPut, void(const std::string& key, const char* buffer,
                              size_t length, RetryCallback callback));

  MOCK_METHOD1(AsyncPut, void(std::shared_ptr<PutObjectAsyncContext> context));

  MOCK_METHOD1(AsyncGet, void(std::shared_ptr<GetObjectAsyncContext> context));
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_H_
