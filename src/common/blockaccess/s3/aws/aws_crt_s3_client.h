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

#ifndef SRC_AWS_S3_CLIENT_AWS_CRT_S3_CLIENT_H_
#define SRC_AWS_S3_CLIENT_AWS_CRT_S3_CLIENT_H_

#include <aws/core/utils/logging/CRTLogSystem.h>

#include <memory>

#include "aws/s3-crt/S3CrtClient.h"
#include "aws/s3-crt/S3CrtClientConfiguration.h"
#include "common/blockaccess/s3/aws/aws_s3_client.h"

namespace dingofs {
namespace blockaccess {
namespace aws {

class AwsCrtS3Client : public AwsS3Client {
 public:
  explicit AwsCrtS3Client() = default;
  ~AwsCrtS3Client() override = default;

  using CRTLogSystemInterfaceSPtr =
      std::shared_ptr<Aws::Utils::Logging::CRTLogSystemInterface>;
  static CRTLogSystemInterfaceSPtr CreateLogger(int log_level);

  void Init(const S3Options& options) override;

  std::string GetAk() override { return s3_options_.s3_info.ak; }

  std::string GetSk() override { return s3_options_.s3_info.sk; }

  std::string GetEndpoint() override { return s3_options_.s3_info.endpoint; }

  bool BucketExist(const std::string& bucket) override;

  int PutObject(const std::string& bucket, const std::string& key,
                const char* buffer, size_t buffer_size) override;

  void AsyncPutObject(const std::string& bucket,
                      PutObjectAsyncContextSPtr user_ctx) override;

  int GetObject(const std::string& bucket, const std::string& key,
                std::string* data) override;

  int RangeObject(const std::string& bucket, const std::string& key, char* buf,
                  off_t offset, size_t len) override;

  void AsyncGetObject(const std::string& bucket,
                      GetObjectAsyncContextSPtr user_ctx) override;

  int DeleteObject(const std::string& bucket, const std::string& key) override;

  int DeleteObjects(const std::string& bucket,
                    const std::list<std::string>& key_list) override;

  bool ObjectExist(const std::string& bucket, const std::string& key) override;

 private:
  S3Options s3_options_;

  std::unique_ptr<Aws::S3Crt::S3CrtClientConfiguration> cfg_{nullptr};
  std::unique_ptr<Aws::S3Crt::S3CrtClient> client_{nullptr};
};

}  // namespace aws
}  // namespace blockaccess
}  // namespace dingofs

#endif  // SRC_AWS_S3_CLIENT_AWS_CRT_S3_CLIENT_H_