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

#ifndef DINGOFS_TEST_CLIENT_BLOCKACCESS_MOCK_ACCESSER_H_
#define DINGOFS_TEST_CLIENT_BLOCKACCESS_MOCK_ACCESSER_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/blockaccess/block_accesser.h"
#include "gmock/gmock.h"

using ::testing::_;
using ::testing::Return;

namespace dingofs {
namespace blockaccess {

class MockBlockAccesser : public BlockAccesser {
 public:
  MockBlockAccesser() = default;

  ~MockBlockAccesser() override = default;

  MOCK_METHOD(Status, Init, (), (override));

  MOCK_METHOD(Status, Destroy, (), (override));

  MOCK_METHOD(bool, ContainerExist, (), (override));

  MOCK_METHOD(Status, Put, (const std::string& key, const std::string& data),
              (override));

  MOCK_METHOD(Status, Put,
              (const std::string& key, const char* buffer, size_t length),
              (override));

  MOCK_METHOD(void, AsyncPut, (std::shared_ptr<PutObjectAsyncContext> context),
              (override));

  MOCK_METHOD(Status, Get, (const std::string& key, std::string* data),
              (override));

  MOCK_METHOD(void, AsyncGet, (std::shared_ptr<GetObjectAsyncContext> context),
              (override));

  MOCK_METHOD(Status, Range,
              (const std::string& key, off_t offset, size_t length,
               char* buffer),
              (override));

  MOCK_METHOD(bool, BlockExist, (const std::string& key), (override));

  MOCK_METHOD(Status, Delete, (const std::string& key), (override));

  MOCK_METHOD(Status, BatchDelete, (const std::list<std::string>& keys),
              (override));
};

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_BLOCKACCESS_MOCK_ACCESSER_H_