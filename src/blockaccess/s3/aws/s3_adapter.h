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

#ifndef DINGOFS_DATAACCESS_AWS_S3_ADAPTER_H_
#define DINGOFS_DATAACCESS_AWS_S3_ADAPTER_H_

#include <memory>

#include "blockaccess/accesser_common.h"
#include "blockaccess/s3/aws/client/aws_s3_client.h"
#include "blockaccess/s3/s3_common.h"

namespace dingofs {
namespace blockaccess {
namespace aws {

class S3Adapter {
 public:
  S3Adapter() = default;
  ~S3Adapter() = default;

  void Init(const S3Options& options);
  static void Shutdown();

  bool BucketExist();

  int PutObject(const std::string& key, const char* buffer, size_t buffer_size);
  int PutObject(const std::string& key, const std::string& data);
  void AsyncPutObject(std::shared_ptr<PutObjectAsyncContext> context);

  int GetObject(const std::string& key, std::string* data);
  int RangeObject(const std::string& key, char* buf, off_t offset, size_t len);
  void AsyncGetObject(std::shared_ptr<GetObjectAsyncContext> context);

  int DeleteObject(const std::string& key);
  int BatchDeleteObject(const std::list<std::string>& key_list);

  bool ObjectExist(const std::string& key);

 private:
  AwsSdkConfig aws_sdk_config_;
  S3Options s3_options_;

  std::string bucket_;

  AwsS3ClientUPtr s3_client_{nullptr};
};

using S3AdapterUPtr = std::unique_ptr<S3Adapter>;

}  // namespace aws
}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_DATAACCESS_AWS_S3_ADAPTER_H_
