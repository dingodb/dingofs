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

#ifndef DINGOFS_COMMON_BLOCK_ACCESS_DATA_ACCESSER_H_
#define DINGOFS_COMMON_BLOCK_ACCESS_DATA_ACCESSER_H_

#include <functional>
#include <list>
#include <memory>
#include <string>

#include "common/blockaccess/accesser_common.h"
#include "common/status.h"

namespace dingofs {
namespace blockaccess {

// Accesser is a class that provides a way to access data from a data
// source. It is a base class for all data access classes.
class Accesser {
 public:
  Accesser() = default;
  virtual ~Accesser() = default;

  using RetryCallback = std::function<bool(int code)>;

  virtual bool Init() = 0;

  virtual bool Destroy() = 0;

  virtual bool ContainerExist() = 0;

  virtual Status Put(const std::string& key, const char* buffer,
                     size_t length) = 0;

  virtual void AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) = 0;

  virtual Status Get(const std::string& key, std::string* data) = 0;

  virtual Status Range(const std::string& key, off_t offset, size_t length,
                       char* buffer) = 0;

  virtual void AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) = 0;

  virtual bool BlockExist(const std::string& key) = 0;

  virtual Status Delete(const std::string& key) = 0;

  virtual Status BatchDelete(const std::list<std::string>& keys) = 0;
};

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_ACCESS_DATA_ACCESSER_H_
