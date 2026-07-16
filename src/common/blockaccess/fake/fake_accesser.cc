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

#include "common/blockaccess/fake/fake_accesser.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <thread>

namespace dingofs {
namespace blockaccess {

static constexpr int64_t kFakeBlockSize = 4 * 1024 * 1024;  // 4MB

bool FakeAccesser::Init() {
  if (started_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "FakeAccesser already started";
    return false;
  }

  started_.store(true, std::memory_order_relaxed);
  return true;
}

bool FakeAccesser::Destroy() {
  if (!started_.load(std::memory_order_relaxed)) {
    return true;
  }

  started_.store(false, std::memory_order_relaxed);
  return true;
}

bool FakeAccesser::ContainerExist() { return true; }

Status FakeAccesser::Put(const std::string& key, const PutPayload& payload) {
  (void)key;
  (void)payload;
  return Status::OK();
}

void FakeAccesser::DoAsyncPut(const std::string& key,
                              PutObjectAsyncContextSPtr context) {
  context->status = Put(key, context->payload);
  context->cb(context);
}

void FakeAccesser::AsyncPut(const std::string& key,
                            PutObjectAsyncContextSPtr context) {
  std::thread([this, key, context]() { DoAsyncPut(key, context); }).detach();
}

Status FakeAccesser::Get(const std::string& key, std::string* data) {
  (void)key;
  data->resize(kFakeBlockSize);
  return Status::OK();
}

Status FakeAccesser::RangeRead(const std::string& key, off_t offset,
                               size_t length, char* buffer,
                               size_t* readed_size) {
  (void)key;
  (void)offset;
  (void)buffer;
  // Skip memset to isolate client overhead from memset page-fault cost.
  *readed_size = length;
  return Status::OK();
}

Status FakeAccesser::Range(const std::string& key, off_t offset, size_t length,
                           char* buffer) {
  size_t total_read = 0;
  RangeRead(key, offset, length, buffer, &total_read);
  return Status::OK();
}

void FakeAccesser::DoAsyncGet(const std::string& key,
                              GetObjectAsyncContextSPtr context) {
  size_t total_read = 0;
  context->status =
      RangeRead(key, context->offset, context->len, context->buf, &total_read);
  context->actual_len = total_read;
  context->cb(context);
}

void FakeAccesser::AsyncGet(const std::string& key,
                            GetObjectAsyncContextSPtr context) {
  std::thread([this, key, context]() { DoAsyncGet(key, context); }).detach();
}

bool FakeAccesser::BlockExist(const std::string& key) {
  (void)key;
  return true;
}

Status FakeAccesser::Delete(const std::string& key) {
  (void)key;
  return Status::OK();
}

void FakeAccesser::AsyncDelete(const std::string& key,
                               DeleteObjectAsyncContextSPtr context) {
  std::thread([this, key, context]() {
    context->status = Delete(key);
    context->cb(context);
  }).detach();
}

Status FakeAccesser::BatchDelete(const std::list<std::string>& keys) {
  (void)keys;
  return Status::OK();
}

void FakeAccesser::AsyncBatchDelete(const std::list<std::string>& keys,
                                    BatchDeleteObjectAsyncContextSPtr context) {
  std::thread([this, keys, context]() {
    context->status = BatchDelete(keys);
    context->cb(context);
  }).detach();
}

}  // namespace blockaccess
}  // namespace dingofs
