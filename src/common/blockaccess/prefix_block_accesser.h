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

#ifndef DINGOFS_COMMON_BLOCK_ACCESS_PREFIX_BLOCK_ACCESSER_H_
#define DINGOFS_COMMON_BLOCK_ACCESS_PREFIX_BLOCK_ACCESSER_H_

#include <memory>
#include <string>
#include <utility>

#include "common/blockaccess/block_accesser.h"

namespace dingofs {
namespace blockaccess {

// PrefixBlockAccesser wraps a BlockAccesserImpl and transparently prepends
// a path prefix to all object keys. This enables multiple filesystems to
// share a single S3 bucket with isolated namespaces.
//
// Example:
//   key = "blocks/0/1/1001_0_4194304"
//   prefix = "myfs"
//   actual key = "myfs/blocks/0/1/1001_0_4194304"
class PrefixBlockAccesser : public BlockAccesser {
 public:
  PrefixBlockAccesser(std::string prefix, const BlockAccessOptions& options)
      : inner_(std::make_unique<BlockAccesserImpl>(options)),
        prefix_(std::move(prefix)) {
    CHECK(!prefix_.empty()) << "PrefixBlockAccesser: prefix must not be empty";
  }

  ~PrefixBlockAccesser() override = default;

  Status Init() override { return inner_->Init(); }

  Status Destroy() override { return inner_->Destroy(); }

  bool ContainerExist() override { return inner_->ContainerExist(); }

  Status Put(const std::string& key, const std::string& data) override {
    return inner_->Put(PrefixKey(key), data);
  }

  Status Put(const std::string& key, const char* buffer,
             size_t length) override {
    return inner_->Put(PrefixKey(key), buffer, length);
  }

  void AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) override {
    context->key = PrefixKey(context->key);
    inner_->AsyncPut(std::move(context));
  }

  Status Get(const std::string& key, std::string* data) override {
    return inner_->Get(PrefixKey(key), data);
  }

  void AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) override {
    context->key = PrefixKey(context->key);
    inner_->AsyncGet(std::move(context));
  }

  Status Range(const std::string& key, off_t offset, size_t length,
               char* buffer) override {
    return inner_->Range(PrefixKey(key), offset, length, buffer);
  }

  bool BlockExist(const std::string& key) override {
    return inner_->BlockExist(PrefixKey(key));
  }

  Status Delete(const std::string& key) override {
    return inner_->Delete(PrefixKey(key));
  }

  Status BatchDelete(const std::list<std::string>& keys) override {
    std::list<std::string> prefixed_keys;
    for (const auto& key : keys) {
      prefixed_keys.push_back(PrefixKey(key));
    }
    return inner_->BatchDelete(prefixed_keys);
  }

 private:
  std::string PrefixKey(const std::string& key) const {
    return prefix_ + "/" + key;
  }

  std::unique_ptr<BlockAccesserImpl> inner_;
  const std::string prefix_;
};

inline BlockAccesserUPtr NewPrefixBlockAccesser(
    const std::string& prefix, const BlockAccessOptions& options) {
  return std::make_unique<PrefixBlockAccesser>(prefix, options);
}

inline BlockAccesserSPtr NewSharePrefixBlockAccesser(
    const std::string& prefix, const BlockAccessOptions& options) {
  return std::make_shared<PrefixBlockAccesser>(prefix, options);
}

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_ACCESS_PREFIX_BLOCK_ACCESSER_H_
