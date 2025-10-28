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

#include "common/blockaccess/rados/rados_accesser.h"

#include <absl/cleanup/cleanup.h>
#include <butil/time.h>
#include <glog/logging.h>
#include <rados/librados.h>

#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <variant>

#include "common/options/blockaccess.h"
#include "common/status.h"

namespace dingofs {
namespace blockaccess {

namespace {

const std::string kDestoryKey = "DESTROY_KEY";

int CreateIoContext(rados_t cluster, const std::string& pool_name,
                    rados_ioctx_t* ioctx) {
  return rados_ioctx_create(cluster, pool_name.c_str(), ioctx);
}

void DestroyIoctx(rados_ioctx_t ioctx) {
  if (ioctx != nullptr) {
    rados_ioctx_destroy(ioctx);
  }
}
}  // namespace

bool RadosAccesser::Init() {
  if (options_.cluster_name.empty() || options_.user_name.empty() ||
      options_.mon_host.empty() || options_.key.empty() ||
      options_.pool_name.empty()) {
    LOG(ERROR)
        << "param cluster_name/user_name/mon_host/key/pool_name is empty.";
    return false;
  }

  uint64_t flags = 0;
  int err = rados_create2(&cluster_, options_.cluster_name.c_str(),
                          options_.user_name.c_str(), flags);
  if (err < 0) {
    LOG(ERROR) << "Failed to create rados cluster, name: "
               << options_.cluster_name << ", user: " << options_.user_name
               << ", err: " << strerror(-err);
    return false;
  }

  err = rados_conf_set(cluster_, "mon_host", options_.mon_host.c_str());
  if (err < 0) {
    LOG(ERROR) << "Failed to set mon_host: " << options_.mon_host
               << ", err: " << strerror(-err);
    return false;
  }

  err = rados_conf_set(cluster_, "key", options_.key.c_str());
  if (err < 0) {
    LOG(ERROR) << "Failed to set key: " << options_.key
               << ", err: " << strerror(-err);
    return false;
  }

  err = rados_conf_set(cluster_, "rados_osd_op_timeout",
                       std::to_string(FLAGS_rados_op_timeout).c_str());
  if (err < 0) {
    LOG(ERROR) << "Failed to set rados_osd_op_timeout, value: "
               << FLAGS_rados_op_timeout << ", err: " << strerror(-err);
    return false;
  }

  err = rados_connect(cluster_);
  if (err < 0) {
    LOG(ERROR) << "Failed to connect to rados cluster, name: "
               << options_.cluster_name << ", user: " << options_.user_name
               << ", err: " << strerror(-err);
    return false;
  }

  LOG(INFO) << "Succss init RadosAccesser cluster: " << cluster_
            << ", mon_host: " << options_.mon_host << options_.cluster_name
            << ", user: " << options_.user_name << ", key: " << options_.key
            << ", pool: " << options_.pool_name;

  return true;
}

bool RadosAccesser::Destroy() {
  if (cluster_ != nullptr) {
    VLOG(1) << "Waiting all aio to finish before destroy rados cluster: "
            << cluster_;

    Status s = ExecuteSyncOp(kDestoryKey, [&](rados_ioctx_t ioctx) {
      int err = rados_aio_flush(ioctx);
      if (err < 0) {
        LOG(ERROR) << "Failed to flush aio in destroy, err: " << strerror(-err);
        return Status::IoError("Failed to write object");
      }
      return Status::OK();
    });

    VLOG(1) << "Finish flush all aio,cluster: " << cluster_
            << ", status: " << s.ToString();

    rados_shutdown(cluster_);
    cluster_ = nullptr;
  }
  return true;
}

Status RadosAccesser::ExecuteSyncOp(
    const std::string& key,
    std::function<Status(rados_ioctx_t ioctx)> sync_op) {
  rados_ioctx_t ioctx;
  int err = CreateIoContext(cluster_, options_.pool_name, &ioctx);
  if (err < 0) {
    LOG(ERROR) << "Failed to create ioctx, pool: " << options_.pool_name
               << ", key: " << key << ", err: " << strerror(-err);
    return Status::IoError(err, "Failed to create ioctx");
  }

  auto defer = ::absl::MakeCleanup([&]() { DestroyIoctx(ioctx); });

  return sync_op(ioctx);
}

bool RadosAccesser::ContainerExist() {
  int pool_id = rados_pool_lookup(cluster_, options_.pool_name.c_str());
  if (pool_id == -ENOENT) {
    LOG(WARNING) << "Not found pool: " << options_.pool_name;
    return false;
  } else {
    VLOG(1) << "Found pool: " << options_.pool_name << ", pool_id: " << pool_id;
    return true;
  }
}

Status RadosAccesser::Put(const std::string& key, const char* buffer,
                          size_t length) {
  return ExecuteSyncOp(key, [&](rados_ioctx_t ioctx) {
    int err = rados_write(ioctx, key.c_str(), buffer, length, 0);
    if (err < 0) {
      LOG(ERROR) << "Failed to write object, key: " << key
                 << ", length: " << length << ", err: " << strerror(-err);
      return Status::IoError("Failed to write object");
    }
    return Status::OK();
  });
}

Status RadosAccesser::Get(const std::string& key, std::string* data) {
  BlockStat stat;
  DINGOFS_RETURN_NOT_OK(Stat(key, &stat));
  VLOG(1) << "Get object, key: " << key << ", size: " << stat.size
          << ", mtime: " << stat.mtime;

  auto buffer = std::make_unique<char[]>(stat.size);

  Status s = Range(key, 0, stat.size, buffer.get());
  if (s.ok()) {
    *data = std::string(buffer.get(), stat.size);
  }
  return s;
}

// TODO: transfer rc to Status
Status RadosAccesser::Range(const std::string& key, off_t offset, size_t length,
                            char* buffer) {
  return ExecuteSyncOp(key, [&](rados_ioctx_t ioctx) {
    int err = rados_read(ioctx, key.c_str(), buffer, length, offset);
    if (err < 0) {
      LOG(ERROR) << "Failed to read object, key: " << key
                 << ", length: " << length << ", offset: " << offset
                 << ", err: " << strerror(-err);
      if (err == -ENOENT) {
        return Status::NotFound("Not found object");
      } else {
        return Status::IoError("Failed to read object");
      }
    }
    return Status::OK();
  });
}

Status RadosAccesser::Stat(const std::string& key, BlockStat* stat) {
  return ExecuteSyncOp(key, [key, stat](rados_ioctx_t ioctx) {
    int err = rados_stat(ioctx, key.c_str(), &stat->size, &stat->mtime);
    if (err == 0) {
      VLOG(1) << "Found object, key: " << key << ", size: " << stat->size
              << ", mtime: " << stat->mtime;
      return Status::OK();
    } else if (err == -ENOENT) {
      LOG(INFO) << "Not found object, key: " << key;
      return Status::NotFound("Not found object");
    } else {
      LOG(ERROR) << "Failed to stat object, key: " << key
                 << ", err: " << strerror(-err);
      return Status::IoError(err, "Failed to stat object");
    }
  });
}

bool RadosAccesser::BlockExist(const std::string& key) {
  BlockStat stat;
  return Stat(key, &stat).ok();
}

Status RadosAccesser::Delete(const std::string& key) {
  return ExecuteSyncOp(key, [&](rados_ioctx_t ioctx) {
    int err = rados_remove(ioctx, key.c_str());
    if (err < 0) {
      LOG(ERROR) << "Failed to remove object, key: " << key
                 << ", err: " << strerror(-err);
      if (err != -ENOENT) {
        return Status::IoError("Failed to remove object");
      }
    }

    return Status::OK();
  });
}

Status RadosAccesser::BatchDelete(const std::list<std::string>& keys) {
  Status s;
  for (const auto& key : keys) {
    Status tmp = Delete(key);
    if (!tmp.ok()) {
      s = tmp;
    }
  }
  return s;
}

RadosAsyncIOUnit::~RadosAsyncIOUnit() {
  if (completion) {
    rados_aio_release(completion);
  }
  if (ioctx) {
    rados_ioctx_destroy(ioctx);
  }
}

static void CompleteCallback(rados_completion_t cb, void* arg) {
  RadosAsyncIOUnit* io_unit = static_cast<RadosAsyncIOUnit*>(arg);

  VLOG(9) << "CompleteCallback is called, key: " << io_unit->key;
  CHECK_NOTNULL(io_unit);
  CHECK(rados_aio_is_complete(io_unit->completion))
      << "Completion is not "
      << "complete, key: " << io_unit->key;

  int err = rados_aio_get_return_value(cb);
  if (err < 0) {
    LOG(WARNING) << "Async operation failed, key: " << io_unit->key
                 << ", error: " << strerror(-err);
  }

  io_unit->callback(io_unit, err);

  delete io_unit;
}

// take ownership of io_unit and pass it to the callback
void RadosAccesser::ExecuteAsyncOperation(
    RadosAsyncIOUnit* io_unit, std::function<int(RadosAsyncIOUnit*)> async_op) {
  VLOG(9) << "ExecuteAsyncOperation is called, key: " << io_unit->key;
  auto defer = ::absl::MakeCleanup([&]() { delete io_unit; });

  int err = CreateIoContext(cluster_, options_.pool_name, &io_unit->ioctx);
  if (err < 0) {
    LOG(ERROR) << "Failed to create ioctx, pool: " << options_.pool_name
               << ", key: " << io_unit->key << ", err: " << strerror(-err);

    io_unit->callback(io_unit, err);
    return;
  }

  err = rados_aio_create_completion(io_unit, &CompleteCallback, nullptr,
                                    &io_unit->completion);
  if (err < 0) {
    LOG(ERROR) << "Failed to create completion, key: " << io_unit->key
               << ", err: " << strerror(-err);
    io_unit->callback(io_unit, err);
    return;
  }

  err = async_op(io_unit);
  if (err < 0) {
    LOG(ERROR) << "Failed to async_op, key: " << io_unit->key
               << ", err: " << strerror(-err);
    io_unit->callback(io_unit, err);
    return;
  }

  std::move(defer).Cancel();
}

//  * The return value of the completion will be number of bytes read on
//  * success, negative error code on failure.
static void AsyncGetCallback(RadosAsyncIOUnit* io_unit, int ret_code) {
  CHECK(std::holds_alternative<std::shared_ptr<GetObjectAsyncContext>>(
      io_unit->async_context))
      << "AsyncGetCallback expects GetObjectAsyncContext";

  auto get_context =
      std::get<std::shared_ptr<GetObjectAsyncContext>>(io_unit->async_context);

  if (ret_code < 0) {
    if (ret_code == -ENOENT) {
      get_context->status = Status::NotFound(strerror(-ret_code));
    } else {
      get_context->status = Status::IoError(strerror(-ret_code));
    }
  } else {
    get_context->status = Status::OK();
    get_context->actual_len = ret_code;
  }

  get_context->cb(get_context);
}

void RadosAccesser::AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) {
  auto* io_unit = new RadosAsyncIOUnit(context);
  io_unit->callback = &AsyncGetCallback;

  // transfer ownership of io_unit to the callback
  ExecuteAsyncOperation(io_unit, [this, context](RadosAsyncIOUnit* unit) {
    int err =
        rados_aio_read(unit->ioctx, context->key.c_str(), unit->completion,
                       context->buf, context->len, context->offset);
    if (err < 0) {
      LOG(ERROR) << "Fail AsyncGet key: " << context->key
                 << ", length: " << context->len
                 << ", offset: " << context->offset
                 << ", err: " << strerror(-err);
    }
    return err;
  });
}

static void AsyncPutCallback(RadosAsyncIOUnit* io_unit, int ret_code) {
  CHECK(std::holds_alternative<std::shared_ptr<PutObjectAsyncContext>>(
      io_unit->async_context))
      << "AsyncPutCallback expects PutObjectAsyncContext";

  auto put_context =
      std::get<std::shared_ptr<PutObjectAsyncContext>>(io_unit->async_context);
  put_context->status =
      (ret_code < 0) ? Status::IoError(strerror(-ret_code)) : Status::OK();
  put_context->cb(put_context);
}

void RadosAccesser::AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) {
  auto* io_unit = new RadosAsyncIOUnit(context);
  io_unit->callback = &AsyncPutCallback;

  // transfer ownership of io_unit to the callback
  ExecuteAsyncOperation(io_unit, [this, context](RadosAsyncIOUnit* unit) {
    int err =
        rados_aio_write(unit->ioctx, context->key.c_str(), unit->completion,
                        context->buffer, context->buffer_size, 0);
    if (err < 0) {
      LOG(ERROR) << "Fail AsyncPut key: " << context->key
                 << ", length: " << context->buffer_size
                 << ", err: " << strerror(-err);
    }
    return err;
  });
}

}  // namespace blockaccess
}  // namespace dingofs