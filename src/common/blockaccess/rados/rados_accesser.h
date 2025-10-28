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

#ifndef DINGOFS_COMMON_BLOCK_ACCESS_RADOS_ACCESSER_H_
#define DINGOFS_COMMON_BLOCK_ACCESS_RADOS_ACCESSER_H_

#include <glog/logging.h>
#include <rados/librados.h>

#include <memory>
#include <string>
#include <variant>

#include "common/blockaccess/accesser.h"
#include "common/blockaccess/rados/rados_common.h"

namespace dingofs {
namespace blockaccess {

using AsyncContext = std::variant<std::shared_ptr<GetObjectAsyncContext>,
                                  std::shared_ptr<PutObjectAsyncContext>>;

struct RadosAsyncIOUnit {
  const std::string key;
  AsyncContext async_context;
  rados_ioctx_t ioctx{nullptr};
  rados_completion_t completion{nullptr};
  std::function<void(RadosAsyncIOUnit*, int ret_code)> callback;

  explicit RadosAsyncIOUnit(std::shared_ptr<GetObjectAsyncContext> get_context)
      : async_context(std::move(CHECK_NOTNULL(get_context))),
        key(get_context->key) {}

  explicit RadosAsyncIOUnit(std::shared_ptr<PutObjectAsyncContext> put_context)
      : async_context(std::move(CHECK_NOTNULL(put_context))),
        key(put_context->key) {}

  ~RadosAsyncIOUnit();

  RadosAsyncIOUnit(const RadosAsyncIOUnit&) = delete;
  RadosAsyncIOUnit& operator=(const RadosAsyncIOUnit&) = delete;
};

class RadosAccesser : public Accesser {
 public:
  RadosAccesser(const RadosOptions& options) : options_(options){};

  ~RadosAccesser() override { Destroy(); }

  bool Init() override;

  bool Destroy() override;

  bool ContainerExist() override;

  Status Put(const std::string& key, const char* buffer,
             size_t length) override;
  void AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) override;

  Status Get(const std::string& key, std::string* data) override;
  Status Range(const std::string& key, off_t offset, size_t length,
               char* buffer) override;
  void AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) override;

  bool BlockExist(const std::string& key) override;

  Status Delete(const std::string& key) override;

  Status BatchDelete(const std::list<std::string>& keys) override;

 private:
  struct BlockStat {
    uint64_t size;
    time_t mtime;
  };

  Status Stat(const std::string& key, BlockStat* stat);

  Status ExecuteSyncOp(const std::string& key,
                       std::function<Status(rados_ioctx_t ioctx)> sync_op);

  void ExecuteAsyncOperation(RadosAsyncIOUnit* io_unit,
                             std::function<int(RadosAsyncIOUnit*)> async_op);

  const RadosOptions options_;

  rados_t cluster_{nullptr};
};

}  // namespace blockaccess

}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_ACCESS_RADOS_ACCESSER_H_