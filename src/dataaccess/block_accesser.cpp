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

#include "dataaccess/block_accesser.h"

#include <memory>

#include "common/status.h"
#include "dataaccess/rados/rados_accesser.h"
#include "dataaccess/s3/s3_accesser.h"

namespace dingofs {
namespace dataaccess {

Status BlockAccesserImpl::Init() {
  if (options_.type == AccesserType::kS3) {
    data_accesser_ = std::make_unique<S3Accesser>(options_.s3_options);
    data_accesser_->Init();
  } else if (options_.type == AccesserType::kRados) {
    data_accesser_ = std::make_unique<RadosAccesser>(options_.rados_options);
  } else {
    LOG(ERROR) << "Unsupported accesser type: " << options_.type;
    return Status::InvalidParam("Unsupported accesser type");
  }

  if (!data_accesser_->Init()) {
    return Status::Internal("Failed to initialize data accesser");
  }

  return Status::OK();
}

Status BlockAccesserImpl::Destroy() {
  if (data_accesser_ != nullptr) {
    data_accesser_->Destroy();
    data_accesser_.reset(nullptr);
  }
  return Status::OK();
}

bool BlockAccesserImpl::ContainerExist() {
  return data_accesser_->ContainerExist();
}

Status BlockAccesserImpl::Put(const std::string& key, const std::string& data) {
  return data_accesser_->Put(key, data.data(), data.size());
}

Status BlockAccesserImpl::Put(const std::string& key, const char* buffer,
                              size_t length) {
  return data_accesser_->Put(key, buffer, length);
}

void BlockAccesserImpl::AsyncPut(
    std::shared_ptr<PutObjectAsyncContext> context) {
  data_accesser_->AsyncPut(context);
}

void BlockAccesserImpl::AsyncPut(const std::string& key, const char* buffer,
                                 size_t length, RetryCallback retry_cb) {
  auto put_obj_ctx = std::make_shared<PutObjectAsyncContext>();
  put_obj_ctx->key = key;
  put_obj_ctx->buffer = buffer;
  put_obj_ctx->buffer_size = length;
  put_obj_ctx->start_time = butil::cpuwide_time_us();
  put_obj_ctx->cb =
      [&, retry_cb](const std::shared_ptr<PutObjectAsyncContext>& ctx) {
        if (retry_cb(ctx->ret_code)) {
          AsyncPut(ctx);
        }
      };

  AsyncPut(put_obj_ctx);
}

Status BlockAccesserImpl::Get(const std::string& key, std::string* data) {
  return data_accesser_->Get(key, data);
}

Status BlockAccesserImpl::Get(const std::string& key, off_t offset,
                              size_t length, char* buffer) {
  return data_accesser_->Get(key, offset, length, buffer);
}

void BlockAccesserImpl::AsyncGet(
    std::shared_ptr<GetObjectAsyncContext> context) {
  data_accesser_->AsyncGet(context);
}

bool BlockAccesserImpl::BlockExist(const std::string& key) {
  return data_accesser_->BlockExist(key);
}

Status BlockAccesserImpl::Delete(const std::string& key) {
  Status s = data_accesser_->Delete(key);
  if (s.ok() || !BlockExist(key)) {
    return s;
  }
  return s;
}

Status BlockAccesserImpl::BatchDelete(const std::list<std::string>& keys) {
  return data_accesser_->BatchDelete(keys);
}

}  // namespace dataaccess
}  // namespace dingofs
