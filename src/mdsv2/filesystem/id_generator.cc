// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mdsv2/filesystem/id_generator.h"

#include <cstdint>

#include "bthread/mutex.h"
#include "dingofs/error.pb.h"
#include "fmt/core.h"
#include "mdsv2/common/logging.h"

namespace dingofs {

namespace mdsv2 {

AutoIncrementIdGenerator::AutoIncrementIdGenerator(CoordinatorClientPtr client, int64_t table_id, int64_t start_id,
                                                   int batch_size)
    : client_(client), table_id_(table_id), start_id_(start_id), batch_size_(batch_size) {
  next_id_ = start_id;
  bundle_ = start_id;
  bundle_end_ = start_id - 1;

  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

AutoIncrementIdGenerator::~AutoIncrementIdGenerator() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destory mutex fail.";
}

bool AutoIncrementIdGenerator::Init() {
  auto status = IsExistAutoIncrement();
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      return false;
    }

    DINGO_LOG(INFO) << fmt::format("auto increment table({}) not exist.", table_id_);
  } else {
    DINGO_LOG(INFO) << fmt::format("auto increment table({}) exist.", table_id_);
    return true;
  }

  status = CreateAutoIncrement();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Create auto increment table fail, error: " << status.error_str();
    return false;
  }

  return true;
}

bool AutoIncrementIdGenerator::GenID(int64_t& id) {
  BAIDU_SCOPED_LOCK(mutex_);

  DINGO_LOG(INFO) << fmt::format("GenID, next_id({}) bundle[{}, {}).", next_id_, bundle_, bundle_end_);

  if (next_id_ >= bundle_end_) {
    auto status = TakeBundleIdFromCoordinator();
    if (!status.ok()) {
      return false;
    }
  }

  id = next_id_++;
  return true;
}

Status AutoIncrementIdGenerator::IsExistAutoIncrement() {
  int64_t start_id = -1;
  return client_->GetAutoIncrement(table_id_, start_id);
}

Status AutoIncrementIdGenerator::CreateAutoIncrement() {
  DINGO_LOG(INFO) << fmt::format("Create auto increment table({}), start_id({}).", table_id_, start_id_);
  return client_->CreateAutoIncrement(table_id_, start_id_);
}

Status AutoIncrementIdGenerator::TakeBundleIdFromCoordinator() {
  int64_t bundle = 0;
  int64_t bundle_end = 0;
  auto status = client_->GenerateAutoIncrement(table_id_, batch_size_, bundle, bundle_end);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Take bundle id fail, error: " << status.error_str();
    return status;
  }

  bundle_ = bundle;
  next_id_ = bundle;
  bundle_end_ = bundle_end;

  DINGO_LOG(INFO) << fmt::format("Take bundle id, bundle[{}, {}).", bundle_, bundle_end_);

  return Status::OK();
}

}  // namespace mdsv2
}  // namespace dingofs