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

#include <butil/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rados/librados.h>

#include <atomic>
#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/directory.h"
#include "common/options/blockaccess.h"
#include "common/status.h"
#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace blockaccess {

DEFINE_int64(rados_objecter_inflight_ops, 8192,
             "number of rados objecter inflight ops");
DEFINE_int64(rados_objecter_inflight_op_bytes, 1048576000,
             "number of rados objecter inflight op bytes");

DEFINE_int64(rados_ms_async_op_threads, 16, "number of rados async op threads");
DEFINE_int64(rados_ms_dispatch_throttle_bytes, 1048576000,
             "number of rados ms dispatch throttle bytes");

DEFINE_bool(rados_enable_admin_socket, true,
            "enable librados admin socket for objecter_requests/perf dump "
            "introspection; socket goes to the dingofs run dir alongside "
            "fd_comm_socket: GetDefaultDir(run)/rados.<pid>.asok "
            "(root -> /var/dingofs/run, non-root -> $HOME/.dingofs/run)");

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

void AppendPayload(rados_write_op_t op, const PutPayload& payload) {
  uint64_t offset = 0;
  for (const auto& segment : payload.Segments()) {
    rados_write_op_write(op, segment.data, segment.size, offset);
    offset += segment.size;
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

  err =
      rados_conf_set(cluster_, "objecter_inflight_ops",
                     std::to_string(FLAGS_rados_objecter_inflight_ops).c_str());
  if (err < 0) {
    LOG(ERROR) << "Failed to set objecter_inflight_ops, value: "
               << FLAGS_rados_objecter_inflight_ops
               << ", err: " << strerror(-err);
    return false;
  }

  err = rados_conf_set(
      cluster_, "objecter_inflight_op_bytes",
      std::to_string(FLAGS_rados_objecter_inflight_op_bytes).c_str());
  if (err < 0) {
    LOG(ERROR) << "Failed to set objecter_inflight_op_bytes, value: "
               << FLAGS_rados_objecter_inflight_op_bytes
               << ", err: " << strerror(-err);
    return false;
  }

  err = rados_conf_set(cluster_, "ms_async_op_threads",
                       std::to_string(FLAGS_rados_ms_async_op_threads).c_str());
  if (err < 0) {
    LOG(ERROR) << "Failed to set ms_async_op_threads, value: "
               << FLAGS_rados_ms_async_op_threads
               << ", err: " << strerror(-err);
    return false;
  }

  err = rados_conf_set(
      cluster_, "ms_dispatch_throttle_bytes",
      std::to_string(FLAGS_rados_ms_dispatch_throttle_bytes).c_str());
  if (err < 0) {
    LOG(ERROR) << "Failed to set ms_dispatch_throttle_bytes, value: "
               << FLAGS_rados_ms_dispatch_throttle_bytes
               << ", err: " << strerror(-err);
    return false;
  }

  // librados admin socket (default on): same dir as dingofs fd_comm_socket via
  // GetDefaultDir(kSocketDir) -- root -> /var/dingofs/run, non-root ->
  // $HOME/.dingofs/run -- so it follows whoever runs the process. Keyed by pid.
  // Non-fatal: a failure here only loses introspection, never blocks mount.
  if (FLAGS_rados_enable_admin_socket) {
    const std::string socket_dir = GetDefaultDir(kSocketDir);
    if (!Helper::CreateDirectory(socket_dir)) {
      LOG(WARNING)
          << "Create admin socket dir failed, skip rados admin socket: "
          << socket_dir;
    } else {
      const std::string asok =
          fmt::format("{}/rados.{}.asok", socket_dir, getpid());
      int aerr = rados_conf_set(cluster_, "admin_socket", asok.c_str());
      if (aerr < 0) {
        LOG(WARNING) << "Set rados admin_socket failed, value: " << asok
                     << ", err: " << strerror(-aerr);
      } else {
        LOG(INFO) << "Rados admin socket enabled: " << asok;
      }
    }
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

  auto defer = MakeScopedCleanup([&]() { DestroyIoctx(ioctx); });

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

Status RadosAccesser::Put(const std::string& key, const PutPayload& payload) {
  return ExecuteSyncOp(key, [&](rados_ioctx_t ioctx) {
    rados_write_op_t op = rados_create_write_op();
    if (op == nullptr) {
      LOG(ERROR) << "Failed to allocate rados write operation, key: " << key;
      return Status::OutOfMemory("failed to allocate rados write operation");
    }
    auto release = MakeScopedCleanup([&]() { rados_release_write_op(op); });
    AppendPayload(op, payload);
    int err = rados_write_op_operate(op, ioctx, key.c_str(), nullptr, 0);
    if (err < 0) {
      LOG(ERROR) << "Failed to write object, key: " << key
                 << ", length: " << payload.Size()
                 << ", err: " << strerror(-err);
      if (err == -EOPNOTSUPP) {
        // e.g. overwrite on an EC pool without allow_ec_overwrites: resending
        // the identical request can never succeed, must not be retried.
        return Status::NotSupport(strerror(-err));
      }
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
  if (keys.empty()) {
    return Status::OK();
  }

  // librados has no multi-object delete; fan out one aio remove per key so the
  // N deletes run concurrently instead of N sequential round-trips.
  // Caller must pre-batch (GC uses 1000); a single batch larger than
  // objecter_inflight_ops will block mid-submit (librados completes ops on its
  // own threads and frees budget, so no deadlock).
  rados_ioctx_t ioctx = nullptr;
  int rc = CreateIoContext(cluster_, options_.pool_name, &ioctx);
  if (rc < 0) {
    LOG(ERROR) << "Failed to create ioctx for batch delete, pool: "
               << options_.pool_name << ", err: " << strerror(-rc);
    return Status::IoError("Failed to create ioctx");
  }
  auto ioctx_guard = MakeScopedCleanup([&]() { DestroyIoctx(ioctx); });

  Status s;
  std::vector<std::pair<const std::string*, rados_completion_t>> inflight;
  inflight.reserve(keys.size());

  for (const auto& key : keys) {
    rados_completion_t c = nullptr;
    rc = rados_aio_create_completion(nullptr, nullptr, nullptr, &c);
    if (rc < 0) {
      LOG(ERROR) << "Failed to create completion, err: " << strerror(-rc);
      s = Status::IoError("Failed to create completion");
      break;  // stop submitting; already-submitted are still drained below
    }

    rc = rados_aio_remove(ioctx, key.c_str(), c);
    if (rc < 0) {
      rados_aio_release(c);
      LOG(ERROR) << "Failed to submit aio remove, key: " << key
                 << ", err: " << strerror(-rc);
      s = Status::IoError("Failed to submit aio remove");
      continue;  // don't return; keep submitting/draining the rest
    }

    inflight.push_back({&key, c});
  }

  // Drain every submitted completion regardless of submit-phase errors.
  for (auto& [key, c] : inflight) {
    rados_aio_wait_for_complete(c);
    int ret = rados_aio_get_return_value(c);
    if (ret < 0 && ret != -ENOENT) {
      LOG(ERROR) << "Failed to remove object, key: " << *key
                 << ", err: " << strerror(-ret);
      s = Status::IoError("Failed to remove object");
    }
    rados_aio_release(c);
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
  auto defer = MakeScopedCleanup([&]() { delete io_unit; });

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

  defer.cancel();
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

void RadosAccesser::AsyncGet(const std::string& key,
                             std::shared_ptr<GetObjectAsyncContext> context) {
  auto* io_unit = new RadosAsyncIOUnit(key, context);
  io_unit->callback = &AsyncGetCallback;

  // transfer ownership of io_unit to the callback
  ExecuteAsyncOperation(io_unit, [this, key, context](RadosAsyncIOUnit* unit) {
    int err = rados_aio_read(unit->ioctx, key.c_str(), unit->completion,
                             context->buf, context->len, context->offset);
    if (err < 0) {
      LOG(ERROR) << "Fail AsyncGet key: " << key << ", length: " << context->len
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
  if (ret_code == -ENOMEM) {
    put_context->status = Status::OutOfMemory("rados put ran out of memory");
  } else if (ret_code == -EOPNOTSUPP) {
    // e.g. overwrite on an EC pool without allow_ec_overwrites: resending the
    // identical request can never succeed, must not be retried.
    put_context->status = Status::NotSupport(strerror(-ret_code));
  } else if (ret_code < 0) {
    put_context->status = Status::IoError(strerror(-ret_code));
  } else {
    put_context->status = Status::OK();
  }
  put_context->cb(put_context);
}

void RadosAccesser::AsyncPut(const std::string& key,
                             std::shared_ptr<PutObjectAsyncContext> context) {
  auto* io_unit = new RadosAsyncIOUnit(key, context);
  io_unit->callback = &AsyncPutCallback;

  // transfer ownership of io_unit to the callback
  ExecuteAsyncOperation(io_unit, [this, key, context](RadosAsyncIOUnit* unit) {
    rados_write_op_t op = rados_create_write_op();
    if (op == nullptr) {
      LOG(ERROR) << "Failed to allocate rados write operation, key: " << key;
      return -ENOMEM;
    }
    auto release = MakeScopedCleanup([&]() { rados_release_write_op(op); });
    AppendPayload(op, context->payload);
    int err = rados_aio_write_op_operate(op, unit->ioctx, unit->completion,
                                         key.c_str(), nullptr, 0);
    if (err < 0) {
      LOG(ERROR) << "Fail AsyncPut key: " << key
                 << ", length: " << context->payload.Size()
                 << ", err: " << strerror(-err);
    }
    return err;
  });
}

static void AsyncDeleteCallback(RadosAsyncIOUnit* io_unit, int ret_code) {
  CHECK(std::holds_alternative<std::shared_ptr<DeleteObjectAsyncContext>>(
      io_unit->async_context))
      << "AsyncDeleteCallback expects DeleteObjectAsyncContext";

  auto delete_context = std::get<std::shared_ptr<DeleteObjectAsyncContext>>(
      io_unit->async_context);
  // ENOENT means the object is already gone -> treat as success (same as
  // the synchronous Delete).
  delete_context->status = (ret_code < 0 && ret_code != -ENOENT)
                               ? Status::IoError(strerror(-ret_code))
                               : Status::OK();
  delete_context->cb(delete_context);
}

void RadosAccesser::AsyncDelete(
    const std::string& key, std::shared_ptr<DeleteObjectAsyncContext> context) {
  auto* io_unit = new RadosAsyncIOUnit(key, context);
  io_unit->callback = &AsyncDeleteCallback;

  // transfer ownership of io_unit to the callback
  ExecuteAsyncOperation(io_unit, [key](RadosAsyncIOUnit* unit) {
    int err = rados_aio_remove(unit->ioctx, key.c_str(), unit->completion);
    if (err < 0) {
      LOG(ERROR) << "Fail AsyncDelete key: " << key
                 << ", err: " << strerror(-err);
    }
    return err;
  });
}

// Aggregator for batch async delete: N completions share one ioctx, so it
// cannot reuse RadosAsyncIOUnit's per-unit destructor (which destroys ioctx).
namespace {

struct RadosBatchDeleteCtx {
  std::shared_ptr<BatchDeleteObjectAsyncContext> user_ctx;
  rados_ioctx_t ioctx{nullptr};
  std::atomic<size_t> remaining{1};  // sentinel: submit loop not finished
  std::atomic<int> first_err{0};
};

struct RadosBatchDeleteNode {
  RadosBatchDeleteCtx* agg{nullptr};
  rados_completion_t completion{nullptr};
  std::string key;  // own a copy; caller's list must not be referenced
};

void FinalizeBatchDelete(RadosBatchDeleteCtx* agg) {
  int err = agg->first_err.load(std::memory_order_relaxed);
  agg->user_ctx->status =
      (err == 0) ? Status::OK() : Status::IoError(strerror(-err));
  agg->user_ctx->cb(agg->user_ctx);
  DestroyIoctx(agg->ioctx);
  delete agg;
}

// remaining's acq_rel ordering carries the happens-before for first_err so the
// finalizing thread sees every per-key CAS. Do NOT weaken to relaxed.
void FinishOne(RadosBatchDeleteCtx* agg) {
  if (agg->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    FinalizeBatchDelete(agg);
  }
}

void PerKeyComplete(rados_completion_t /*c*/, void* arg) {
  auto* node = static_cast<RadosBatchDeleteNode*>(arg);
  auto* agg = node->agg;

  int ret = rados_aio_get_return_value(node->completion);
  if (ret < 0 && ret != -ENOENT) {
    int expected = 0;
    agg->first_err.compare_exchange_strong(expected, ret);
  }

  rados_aio_release(node->completion);
  delete node;
  FinishOne(agg);
}

}  // namespace

void RadosAccesser::AsyncBatchDelete(
    const std::list<std::string>& keys,
    std::shared_ptr<BatchDeleteObjectAsyncContext> context) {
  CHECK(context->cb) << "AsyncBatchDelete context callback is null";

  if (keys.empty()) {
    context->status = Status::OK();
    context->cb(context);
    return;
  }

  auto* agg = new RadosBatchDeleteCtx;
  agg->user_ctx = context;

  int rc = CreateIoContext(cluster_, options_.pool_name, &agg->ioctx);
  if (rc < 0) {
    LOG(ERROR) << "Failed to create ioctx for async batch delete, pool: "
               << options_.pool_name << ", err: " << strerror(-rc);
    context->status = Status::IoError("Failed to create ioctx");
    context->cb(context);
    delete agg;
    return;
  }

  for (const auto& key : keys) {
    auto* node = new RadosBatchDeleteNode;
    node->agg = agg;
    node->key = key;

    rc = rados_aio_create_completion(node, &PerKeyComplete, nullptr,
                                     &node->completion);
    if (rc < 0) {
      LOG(ERROR) << "Failed to create completion, key: " << key
                 << ", err: " << strerror(-rc);
      int expected = 0;
      agg->first_err.compare_exchange_strong(expected, rc);
      delete node;
      continue;  // no remaining++ -> no completion will fire for this key
    }

    agg->remaining.fetch_add(1, std::memory_order_relaxed);

    rc = rados_aio_remove(agg->ioctx, node->key.c_str(), node->completion);
    if (rc < 0) {
      LOG(ERROR) << "Failed to submit aio remove, key: " << key
                 << ", err: " << strerror(-rc);
      int expected = 0;
      agg->first_err.compare_exchange_strong(expected, rc);
      rados_aio_release(node->completion);
      delete node;
      FinishOne(agg);  // roll back the remaining++ above
      continue;
    }
  }

  FinishOne(agg);  // remove sentinel; may trigger finalize here
}

}  // namespace blockaccess
}  // namespace dingofs
