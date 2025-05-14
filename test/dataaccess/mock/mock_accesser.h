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
 * Project: Dingofs
 * Created Date: 2024-09-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_TEST_CLIENT_DATAACCESS_MOCK_ACCESSER_H_
#define DINGOFS_TEST_CLIENT_DATAACCESS_MOCK_ACCESSER_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "dataaccess/accesser.h"

using ::testing::_;
using ::testing::Return;

namespace dingofs {
namespace dataaccess {

class MockDataAccesser : public DataAccesser {
 public:
  MockDataAccesser() = default;

  ~MockDataAccesser() override = default;

  MOCK_METHOD0(Init, bool());

  MOCK_METHOD0(Destroy, bool());

  MOCK_METHOD3(Put, Status(const std::string& key, const char* buffer,
                           size_t length));
  MOCK_METHOD4(AsyncPut, void(const std::string& key, const char* buffer,
                              size_t length, RetryCallback callback));

  MOCK_METHOD1(AsyncPut, void(std::shared_ptr<PutObjectAsyncContext> context));

  MOCK_METHOD4(Get, Status(const std::string& key, off_t offset, size_t length,
                           char* buffer));

  MOCK_METHOD1(AsyncGet, void(std::shared_ptr<GetObjectAsyncContext> context));

  MOCK_METHOD5(AsyncGet,
               void(const std::string& key, off_t offset, size_t length,
                    char* buffer, RetryCallback retry_cb));

  MOCK_METHOD1(Delete, Status(const std::string& key));
};

}  // namespace dataaccess
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_DATAACCESS_MOCK_ACCESSER_H_