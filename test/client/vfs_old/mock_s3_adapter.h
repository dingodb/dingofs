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
 * Created Date: Thur Sep 8 2021
 * Author: huyao
 */

#ifndef DINGOFS_TEST_CLIENT_MOCK_S3_ADAPTER_H_
#define DINGOFS_TEST_CLIENT_MOCK_S3_ADAPTER_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "dataaccess/s3/aws/s3_adapter.h"

using ::testing::Return;

using dingofs::dataaccess::aws::GetObjectAsyncContext;
using dingofs::dataaccess::aws::PutObjectAsyncContext;

namespace dingofs {
namespace client {

class MockS3Adapter : public dataaccess::aws::S3Adapter {
 public:
  MockS3Adapter() = default;
  ~MockS3Adapter() override = default;

  MOCK_METHOD1(Init, void(const std::string&));
  MOCK_METHOD3(PutObject, int(const Aws::String&, const char* buffer,
                              const size_t bufferSize));
  MOCK_METHOD1(PutObjectAsync, void(std::shared_ptr<PutObjectAsyncContext>));
  MOCK_METHOD4(GetObject, int(const std::string&, char*, off_t, size_t));
  MOCK_METHOD1(GetObjectAsync, void(std::shared_ptr<GetObjectAsyncContext>));
  MOCK_METHOD1(ObjectExist, bool(const Aws::String& key));
};

}  // namespace client
}  // namespace dingofs
#endif  // DINGOFS_TEST_CLIENT_MOCK_S3_ADAPTER_H_
