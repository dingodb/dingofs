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

#ifndef DINGOFS_SRC_BLOCKACCESS_S3_AWS_AWS_S3_CLIENT_H_
#define DINGOFS_SRC_BLOCKACCESS_S3_AWS_AWS_S3_CLIENT_H_

#include "blockaccess/s3/aws/aws_s3_common.h"
#include "blockaccess/s3/s3_common.h"

namespace dingofs {
namespace blockaccess {
namespace aws {

class AwsS3Client {
 public:
  AwsS3Client() = default;
  virtual ~AwsS3Client() = default;

  virtual void Init(const S3Options& options) = 0;

  virtual std::string GetAk() = 0;
  virtual std::string GetSk() = 0;
  virtual std::string GetEndpoint() = 0;

  virtual bool BucketExist(const std::string& bucket) = 0;

  virtual int PutObject(const std::string& bucket, const std::string& key,
                        const char* buffer, size_t buffer_size) = 0;

  virtual void AsyncPutObject(const std::string& bucket,
                              PutObjectAsyncContextSPtr user_ctx) = 0;

  virtual int GetObject(const std::string& bucket, const std::string& key,
                        std::string* data) = 0;

  virtual int RangeObject(const std::string& bucket, const std::string& key,
                          char* buf, off_t offset, size_t len) = 0;

  virtual void AsyncGetObject(const std::string& bucket,
                              GetObjectAsyncContextSPtr user_ctx) = 0;

  virtual int DeleteObject(const std::string& bucket,
                           const std::string& key) = 0;

  virtual int DeleteObjects(const std::string& bucket,
                            const std::list<std::string>& key_list) = 0;

  virtual bool ObjectExist(const std::string& bucket,
                           const std::string& key) = 0;
};

using AwsS3ClientUPtr = std::unique_ptr<AwsS3Client>;

}  // namespace aws
}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_SRC_BLOCKACCESS_S3_AWS_AWS_S3_CLIENT_H_
