
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

#ifndef DATA_ACCESS_AWS_FAKE_S3_ADAPTER_H
#define DATA_ACCESS_AWS_FAKE_S3_ADAPTER_H

#include "dataaccess/aws/s3_adapter.h"

namespace dingofs {
namespace dataaccess {
namespace aws {

class FakeS3Adapter final : public S3Adapter {
 public:
  FakeS3Adapter() = default;

  ~FakeS3Adapter() override = default;

  bool BucketExist() override { return true; }

  int PutObject(const Aws::String& key, const char* buffer,
                const size_t buffer_size) override {
    (void)key;
    (void)buffer;
    (void)buffer_size;
    return 0;
  }

  int PutObject(const Aws::String& key, const std::string& data) override {
    (void)key;
    (void)data;
    return 0;
  }

  void PutObjectAsync(std::shared_ptr<PutObjectAsyncContext> context) override {
    context->ret_code = 0;
    context->cb(context);
  }

  int GetObject(const Aws::String& key, std::string* data) override {
    (void)key;
    (void)data;
    // just return 4M data
    data->resize(4 * 1024 * 1024, '1');
    return 0;
  }

  int GetObject(const std::string& key, char* buf, off_t offset,
                size_t len) override {
    (void)key;
    (void)offset;
    // juset return len data
    memset(buf, '1', len);
    return 0;
  }

  void GetObjectAsync(std::shared_ptr<GetObjectAsyncContext> context) override {
    memset(context->buf, '1', context->len);
    context->ret_code = 0;
    context->cb(context);
  }

  int DeleteObject(const Aws::String& key) override {
    (void)key;
    return 0;
  }

  int DeleteObjects(const std::list<Aws::String>& key_list) override {
    (void)key_list;
    return 0;
  }

  bool ObjectExist(const Aws::String& key) override {
    (void)key;
    return true;
  }
};

}  // namespace aws
}  // namespace dataaccess
}  // namespace dingofs

#endif  // DATA_ACCESS_AWS_FAKE_S3_ADAPTER_H
