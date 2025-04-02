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
 * Created Date: 21-5-31
 * Author: huyao
 */
#ifndef DINGOFS_SRC_CLIENT_S3_CLIENT_S3_H_
#define DINGOFS_SRC_CLIENT_S3_CLIENT_S3_H_

#include <memory>
#include <string>

#include "aws/s3_adapter.h"

namespace dingofs {
namespace client {

using dingofs::aws::FakeS3Adapter;
using dingofs::aws::GetObjectAsyncContext;
using dingofs::aws::PutObjectAsyncContext;

namespace common {
DECLARE_bool(useFakeS3);
}  // namespace common

class S3Client {
 public:
  S3Client() = default;
  virtual ~S3Client() = default;
  virtual void Init(const dingofs::aws::S3AdapterOption& option) = 0;
  virtual void Deinit() = 0;
  virtual int Upload(const std::string& name, const char* buf,
                     uint64_t length) = 0;
  virtual void UploadAsync(std::shared_ptr<PutObjectAsyncContext> context) = 0;
  virtual int Download(const std::string& name, char* buf, uint64_t offset,
                       uint64_t length) = 0;
  virtual void DownloadAsync(
      std::shared_ptr<GetObjectAsyncContext> context) = 0;
};

class S3ClientImpl : public S3Client {
 public:
  S3ClientImpl() {
    if (dingofs::client::common::FLAGS_useFakeS3) {
      s3Adapter_ = std::make_shared<FakeS3Adapter>();
      LOG(INFO) << "use fake S3";
    } else {
      s3Adapter_ = std::make_shared<dingofs::aws::S3Adapter>();
      LOG(INFO) << "use S3";
    }
  }
  ~S3ClientImpl() override = default;

  void Init(const dingofs::aws::S3AdapterOption& option) override;
  void Deinit() override;

  int Upload(const std::string& name, const char* buf,
             uint64_t length) override;
  void UploadAsync(std::shared_ptr<PutObjectAsyncContext> context) override;

  int Download(const std::string& name, char* buf, uint64_t offset,
               uint64_t length) override;
  void DownloadAsync(std::shared_ptr<GetObjectAsyncContext> context) override;

  void SetAdapter(std::shared_ptr<dingofs::aws::S3Adapter> adapter) {
    s3Adapter_ = adapter;
  }

 private:
  std::shared_ptr<dingofs::aws::S3Adapter> s3Adapter_;
};

}  // namespace client
}  // namespace dingofs
#endif  // DINGOFS_SRC_CLIENT_S3_CLIENT_S3_H_
