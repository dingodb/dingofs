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

#ifndef DINGOFS_DATA_ACCESS_RADOS_ACCESSER_H_
#define DINGOFS_DATA_ACCESS_RADOS_ACCESSER_H_

#include <glog/logging.h>
#include <rados/librados.h>

#include <memory>
#include <string>
#include <variant>

#include "dataaccess/accesser.h"
#include "dataaccess/ceph-rados/rados_common.h"

namespace dingofs {
namespace dataaccess {

using AsyncContext = std::variant<std::shared_ptr<GetObjectAsyncContext>,
                                  std::shared_ptr<PutObjectAsyncContext>>;

struct RadosAsyncIOUnit {
  std::string key;
  AsyncContext async_context;
  rados_ioctx_t ioctx{};
  rados_completion_t completion{};

  explicit RadosAsyncIOUnit(std::shared_ptr<GetObjectAsyncContext> get_context)
      : async_context(std::move(CHECK_NOTNULL(get_context))) {}

  explicit RadosAsyncIOUnit(std::shared_ptr<PutObjectAsyncContext> put_context)
      : async_context(std::move(CHECK_NOTNULL(put_context))) {}

  ~RadosAsyncIOUnit();

  RadosAsyncIOUnit(const RadosAsyncIOUnit&) = delete;
  RadosAsyncIOUnit& operator=(const RadosAsyncIOUnit&) = delete;
};

class RadosAccesser : public DataAccesser {
 public:
  RadosAccesser(const RadosOptions& options) : options_(options) {};

  ~RadosAccesser() override { Destroy(); }

  bool Init() override;

  bool Destroy() override;

  Status Put(const std::string& key, const char* buffer,
             size_t length) override;
  void AsyncPut(const std::string& key, const char* buffer, size_t length,
                RetryCallback retry_cb) override;
  void AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) override;

  Status Get(const std::string& key, off_t offset, size_t length,
             char* buffer) override;
  void AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) override;

  Status Delete(const std::string& key) override;

 private:
  Status ExecuteSyncOp(const std::string& key,
                       std::function<Status(rados_ioctx_t ioctx)> sync_op);

  template <typename Context>
  void ExecuteAsyncOperation(
      std::shared_ptr<Context> context,
      std::function<int(const RadosAsyncIOUnit&)> async_op);

  const RadosOptions options_;

  rados_t cluster_{nullptr};
};

}  // namespace dataaccess

}  // namespace dingofs

#endif  // DINGOFS_DATA_ACCESS_RADOS_ACCESSER_H_