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

#ifndef DINGOFS_DATA_ACCESS_BLOCK_ACCESSER_H_
#define DINGOFS_DATA_ACCESS_BLOCK_ACCESSER_H_

#include <functional>
#include <memory>
#include <string>

#include "common/status.h"
#include "dataaccess/accesser_common.h"
#include "dataaccess/accesser.h"

namespace dingofs {
namespace dataaccess {

using RetryCallback = std::function<bool(int code)>;

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

  virtual void AsyncPut(const std::string& key, const char* buffer,
                        size_t length, RetryCallback retry_cb) = 0;

  virtual void AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) = 0;

  virtual Status Get(const std::string& key, std::string* data) = 0;

  virtual Status Get(const std::string& key, off_t offset, size_t length,
                     char* buffer) = 0;

  virtual void AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) = 0;

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

  void AsyncPut(const std::string& key, const char* buffer, size_t length,
                RetryCallback retry_cb) override;

  void AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) override;

  Status Get(const std::string& key, std::string* data) override;

  Status Get(const std::string& key, off_t offset, size_t length,
             char* buffer) override;

  void AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) override;

  bool BlockExist(const std::string& key) override;

  Status Delete(const std::string& key) override;

  Status BatchDelete(const std::list<std::string>& keys) override;

 private:
  const BlockAccessOptions options_;
  std::unique_ptr<Accesser> data_accesser_;
};

using BlockAccesserPtr = std::shared_ptr<BlockAccesser>;

}  // namespace dataaccess
}  // namespace dingofs

#endif  // DINGOFS_DATA_ACCESS_BLOCK_ACCESSER_H_
