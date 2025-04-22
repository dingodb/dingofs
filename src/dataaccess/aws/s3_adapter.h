/*
 *  Copyright (c) 2020 NetEase Inc.
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

/*************************************************************************
    > File Name: s3_adapter.h
    > Author:
    > Created Time: Mon Dec 10 14:30:12 2018
 ************************************************************************/

#ifndef DINGOFS_DATAACCESS_AWS_S3_ADAPTER_H_
#define DINGOFS_DATAACCESS_AWS_S3_ADAPTER_H_

#include <condition_variable>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>

#include "dataaccess/accesser_common.h"
#include "dataaccess/aws/aws_s3_common.h"
#include "dataaccess/aws/client/aws_s3_client.h"
#include "options/client/s3.h"
#include "utils/configuration.h"
#include "utils/throttle.h"

namespace dingofs {
namespace dataaccess {
namespace aws {

class S3Adapter {
 public:
  S3Adapter() = default;

  virtual ~S3Adapter() = default;

  // 初始化S3Adapter
  virtual void Init(const S3Option& option);

  static void Shutdown();

  virtual void Reinit(const S3Option& option);

  virtual std::string GetS3Ak();
  virtual std::string GetS3Sk();
  virtual std::string GetS3Endpoint();

  virtual bool BucketExist();

  virtual int PutObject(const std::string& key, const char* buffer,
                        size_t buffer_size);

  virtual int PutObject(const std::string& key, const std::string& data);

  virtual void PutObjectAsync(std::shared_ptr<PutObjectAsyncContext> context);

  virtual int GetObject(const std::string& key, std::string* data);

  virtual int GetObject(const std::string& key, char* buf, off_t offset,
                        size_t len);

  virtual void GetObjectAsync(std::shared_ptr<GetObjectAsyncContext> context);

  virtual int DeleteObject(const std::string& key);

  virtual int DeleteObjects(const std::list<std::string>& key_list);

  virtual bool ObjectExist(const std::string& key);

  void SetBucketName(const std::string& name) { bucket_ = name; }

  std::string GetBucketName() { return bucket_; }

 private:
  class AsyncRequestInflightBytesThrottle {
   public:
    explicit AsyncRequestInflightBytesThrottle(uint64_t max_inflight_bytes)
        : maxInflightBytes_(max_inflight_bytes), inflightBytes_(0) {}

    void OnStart(uint64_t len);
    void OnComplete(uint64_t len);

   private:
    const uint64_t maxInflightBytes_;
    uint64_t inflightBytes_;

    std::mutex mtx_;
    std::condition_variable cond_;
  };

  std::string bucket_;
  std::unique_ptr<AwsS3Client> s3_client_{nullptr};
  std::unique_ptr<utils::Throttle> throttle_{nullptr};

  std::unique_ptr<AsyncRequestInflightBytesThrottle> inflightBytesThrottle_;
};

}  // namespace aws
}  // namespace dataaccess
}  // namespace dingofs

#endif  // DINGOFS_DATAACCESS_AWS_S3_ADAPTER_H_
