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

#ifndef DINGOFS_BLOCK_ACCESS_S3_ACCESSER_H_
#define DINGOFS_BLOCK_ACCESS_S3_ACCESSER_H_

#include "common/blockaccess/accesser.h"
#include "common/blockaccess/s3/aws/aws_s3_client.h"
#include "common/blockaccess/s3/s3_common.h"
#include "common/status.h"

namespace dingofs {
namespace blockaccess {

// S3Accesser is a class that provides a way to access data from a S3 data
// source. It is a derived class of DataAccesser.
// use aws-sdk-cpp implement
class S3Accesser : public Accesser {
 public:
  S3Accesser(const S3Options& options) : options_(options) {}
  ~S3Accesser() override = default;

  bool Init() override;
  bool Destroy() override;

  bool ContainerExist() override;

  Status Put(const std::string& key, const char* buffer,
             size_t length) override;
  void AsyncPut(PutObjectAsyncContextSPtr context) override;

  Status Get(const std::string& key, std::string* data) override;
  Status Range(const std::string& key, off_t offset, size_t length,
               char* buffer) override;
  void AsyncGet(GetObjectAsyncContextSPtr context) override;

  bool BlockExist(const std::string& key) override;

  Status Delete(const std::string& key) override;
  Status BatchDelete(const std::list<std::string>& keys) override;

 private:
  static Aws::String S3Key(const std::string& key);

  std::string bucket_;

  const S3Options options_;

  aws::AwsS3ClientUPtr client_{nullptr};
};

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_BLOCK_ACCESS_S3_ACCESSER_H_