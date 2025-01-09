/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Project: Curve
 * Created Date: 2024-09-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_TEST_CLIENT_BLOCKCACHE_MOCK_MOCK_CLIENT_S3_H_
#define DINGOFS_TEST_CLIENT_BLOCKCACHE_MOCK_MOCK_CLIENT_S3_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "client/blockcache/block_cache.h"
#include "client/blockcache/s3_client.h"

using ::testing::_;
using ::testing::Return;

namespace dingofs {
namespace client {
namespace blockcache {

using ::dingofs::aws::GetObjectAsyncContext;
using ::dingofs::aws::PutObjectAsyncContext;
using ::dingofs::aws::S3AdapterOption;
using ::dingofs::client::blockcache::BCACHE_ERROR;
using ::dingofs::client::blockcache::S3Client;

class MockS3Client : public S3Client {
 public:
  MockS3Client() = default;

  ~MockS3Client() override = default;

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

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_BLOCKCACHE_MOCK_MOCK_CLIENT_S3_H_
