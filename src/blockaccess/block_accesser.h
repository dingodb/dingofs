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

#ifndef DINGOFS_BLOCK_ACCESS_BLOCK_ACCESSER_H_
#define DINGOFS_BLOCK_ACCESS_BLOCK_ACCESSER_H_

#include <glog/logging.h>
#include <sys/types.h>

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "blockaccess/accesser.h"
#include "blockaccess/accesser_common.h"
#include "common/status.h"
#include "utils/throttle.h"

namespace dingofs {
namespace blockaccess {

// BlockAccesser is a class that provides a way to access block from a data
// source. It is a base class for all data access classes.
class BlockAccesser {
 public:
  virtual ~BlockAccesser() = default;

  virtual Status Init() = 0;

  virtual Status Destroy() = 0;

  virtual bool ContainerExist() = 0;

  virtual Status Put(const std::string& key, const std::string& data) = 0;

  virtual Status Put(const std::string& key, const char* buffer,
                     size_t length) = 0;

  virtual void AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) = 0;

  virtual Status Get(const std::string& key, std::string* data) = 0;

  virtual void AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) = 0;

  virtual Status Range(const std::string& key, off_t offset, size_t length,
                       char* buffer) = 0;

  virtual bool BlockExist(const std::string& key) = 0;

  virtual Status Delete(const std::string& key) = 0;

  virtual Status BatchDelete(const std::list<std::string>& keys) = 0;
};

class BlockAccesserImpl : public BlockAccesser {
 public:
  BlockAccesserImpl(const BlockAccessOptions& options) : options_(options) {}

  ~BlockAccesserImpl() override { Destroy(); }

  Status Init() override;

  Status Destroy() override;

  bool ContainerExist() override;

  Status Put(const std::string& key, const std::string& data) override;

  Status Put(const std::string& key, const char* buffer,
             size_t length) override;

  void AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) override;

  Status Get(const std::string& key, std::string* data) override;

  void AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) override;

  Status Range(const std::string& key, off_t offset, size_t length,
               char* buffer) override;

  bool BlockExist(const std::string& key) override;

  Status Delete(const std::string& key) override;

  Status BatchDelete(const std::list<std::string>& keys) override;

 private:
  class AsyncRequestInflightBytesThrottle {
   public:
    explicit AsyncRequestInflightBytesThrottle(uint64_t max_inflight_bytes)
        : max_inflight_bytes_(max_inflight_bytes) {}

    void OnStart(uint64_t len) {
      std::unique_lock<std::mutex> lock(mtx_);
      while (inflight_bytes_ + len > max_inflight_bytes_) {
        cond_.wait(lock);
      }

      inflight_bytes_ += len;
    }

    void OnComplete(uint64_t len) {
      std::unique_lock<std::mutex> lock(mtx_);
      inflight_bytes_ -= len;
      CHECK(inflight_bytes_ >= 0)
          << "inflight_bytes_=" << inflight_bytes_
          << ", max_inflight_bytes_=" << max_inflight_bytes_;
      cond_.notify_all();
    }

   private:
    const uint64_t max_inflight_bytes_;
    int64_t inflight_bytes_{0};

    std::mutex mtx_;
    std::condition_variable cond_;
  };

  const BlockAccessOptions options_;
  std::unique_ptr<Accesser> data_accesser_;
  std::string container_name_;

  std::unique_ptr<utils::Throttle> throttle_{nullptr};
  std::unique_ptr<AsyncRequestInflightBytesThrottle> inflight_bytes_throttle_;
};

using BlockAccesserSPtr = std::shared_ptr<BlockAccesser>;
using BlockAccesserUPtr = std::unique_ptr<BlockAccesser>;

inline BlockAccesserUPtr NewBlockAccesser(const BlockAccessOptions& options) {
  return std::make_unique<blockaccess::BlockAccesserImpl>(options);
}

inline BlockAccesserSPtr NewShareBlockAccesser(
    const BlockAccessOptions& options) {
  return std::make_shared<blockaccess::BlockAccesserImpl>(options);
}

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_BLOCK_ACCESS_BLOCK_ACCESSER_H_
