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

#ifndef DINGOFS_DATA_ACCESS_S3_ACCESSER_H_
#define DINGOFS_DATA_ACCESS_S3_ACCESSER_H_

#include "dataaccess/accesser.h"
#include "dataaccess/aws/s3_adapter.h"
#include "options/client/s3.h"

namespace dingofs {
namespace dataaccess {

using ::dingofs::client::Status;
using options::client::S3Option;

class S3Accesser;
using S3AccesserPtr = std::shared_ptr<S3Accesser>;

// S3Accesser is a class that provides a way to access data from a S3 data
// source. It is a derived class of DataAccesser.
// use aws-sdk-cpp implement
class S3Accesser : public DataAccesser {
 public:
  S3Accesser(const S3Option& option) : option_(option) {}
  ~S3Accesser() override = default;

  static S3AccesserPtr New(const S3Option& option) {
    return std::make_shared<S3Accesser>(option);
  }

  bool Init() override;

  bool Destroy() override;

  Status Put(const std::string& key, const char* buffer,
             size_t length) override;
  void AsyncPut(const std::string& key, const char* buffer, size_t length,
                RetryCallback retry_cb) override;
  void AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) override;

  Status Get(const std::string& key, off_t offset, size_t length,
             char* buffer) override;
  void AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) override;

  Status Delete(const std::string& key) override;

 private:
  static Aws::String S3Key(const std::string& key);

  const S3Option option_;

  std::unique_ptr<dataaccess::aws::S3Adapter> client_;
};

}  // namespace dataaccess
}  // namespace dingofs

#endif  // DINGOFS_DATA_ACCESS_S3_ACCESSER_H_