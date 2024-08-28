/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2024-08-25
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/blockcache/s3_client.h"

#include <ostream>

namespace curvefs {
namespace client {
namespace blockcache {

void S3ClientImpl::Init(const S3AdapterOption& option) {
  client_ = std::make_unique<S3Adapter>();
  client_->Init(option);
}

void S3ClientImpl::Destroy() { client_->Deinit(); }

BCACHE_ERROR S3ClientImpl::Put(const std::string& key, const char* buffer,
                               size_t length) {
  int rc = client_->PutObject(S3Key(key), buffer, length);
  if (rc < 0) {
    LOG(ERROR) << "Put object(" << key << ") failed, retCode=" << rc;
    return BCACHE_ERROR::IO_ERROR;
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR S3ClientImpl::Range(const std::string& key, off_t offset,
                                 size_t length, char* buffer) {
  int rc = client_->GetObject(S3Key(key), buffer, offset, length);
  if (rc < 0) {
    if (!client_->ObjectExist(S3Key(key))) {  // TODO: more efficient
      LOG(WARNING) << "Object(" << key << ") not found.";
      return BCACHE_ERROR::NOT_FOUND;
    }
    LOG(ERROR) << "Get object(" << key << ") failed, retCode=" << rc;
    return BCACHE_ERROR::IO_ERROR;
  }
  return BCACHE_ERROR::OK;
}

void S3ClientImpl::AsyncPut(const std::string& key, const char* buffer,
                            size_t length, RetryCallback retry) {
  auto context = std::make_shared<PutObjectAsyncContext>();
  context->key = key;
  context->buffer = buffer;
  context->bufferSize = length;
  context->startTime = butil::cpuwide_time_us();
  context->cb = [&,
                 retry](const std::shared_ptr<PutObjectAsyncContext>& context) {
    if (retry(context->retCode)) {  // retry
      client_->PutObjectAsync(context);
    }
  };
  client_->PutObjectAsync(context);
}

void S3ClientImpl::AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) {
  client_->PutObjectAsync(context);
}

void S3ClientImpl::AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) {
  client_->GetObjectAsync(context);
}

Aws::String S3ClientImpl::S3Key(const std::string& key) {
  return Aws::String(key.c_str(), key.size());
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
