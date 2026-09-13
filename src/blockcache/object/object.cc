/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "blockcache/object/object.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "blockcache/core/runtime/smp.h"
#include "blockcache/core/runtime/worker_pool.h"

namespace dingofs {
namespace blockcache {

DEFINE_uint32(storage_upload_max_tries, 10, "upload tries per block");
DEFINE_validator(storage_upload_max_tries, brpc::PassValidate);

DEFINE_uint32(storage_download_max_tries, 10, "retrieval tries per block");
DEFINE_validator(storage_download_max_tries, brpc::PassValidate);

DEFINE_uint32(storage_download_notfound_max_tries, 8,
              "retrieval tries when storage says not found");
DEFINE_validator(storage_download_notfound_max_tries, brpc::PassValidate);

DEFINE_uint32(storage_upload_retry_backoff_base_ms, 1000,
              "first upload retry delay, doubling up to the cap (ms)");
DEFINE_uint32(storage_download_retry_backoff_base_ms, 300,
              "first retrieve retry delay, doubling up to the cap (ms)");
DEFINE_uint32(storage_download_notfound_retry_backoff_base_ms, 500,
              "first retry delay after a not-found retrieve (ms)");

struct ObjectCompletion : InboxWork {
  ObjectCompletion() : shard(ThisShardId()) {
    run = [](InboxWork* work) {
      auto* self = static_cast<ObjectCompletion*>(work);
      self->promise.SetValue(std::move(self->status));
    };
  }

  void Complete(Status result) {
    status = std::move(result);
    CHECK(PostTo(shard, this)) << "Object completion outlived its shard";
  }

  const unsigned shard;
  Status status;
  Promise<Status> promise;
};

ObjectStorage::ObjectStorage(StorageClient* client)
    : client_(client),
      num_upload_retry_("dingofs_storage_upload_total_retry"),
      num_download_retry_("dingofs_storage_download_total_retry"),
      num_download_notfound_retry_(
          "dingofs_storage_download_notfound_total_retry") {}

Future<Status> ObjectStorage::Put(BlockHandle handle, BufferViews block,
                                  ObjectPutOption option) {
  const uint32_t max_tries = std::max(
      option.max_tries > 0 ? option.max_tries : FLAGS_storage_upload_max_tries,
      1U);
  const std::string key = handle.StoreKey();
  const blockaccess::PutPayload payload = PayloadOf(block);

  for (uint32_t tried = 1;; tried++) {
    Status status = co_await PutOnce(handle.fs_id, key, payload);
    if (status.ok()) {
      co_return status;
    } else if (!IsRetriable(status)) {
      LOG(ERROR) << "Fail to upload " << key
                 << ", unretriable: " << status.ToString();
      co_return status;
    } else if (tried >= max_tries) {
      LOG(ERROR) << "Fail to upload " << key << ", out of tries " << tried
                 << "/" << max_tries << ": " << status.ToString();
      co_return status;
    }

    const uint64_t backoff_ms = PutBackoffMs(tried);
    num_upload_retry_ << 1;
    LOG(WARNING) << "Fail to upload " << key << ", will retry " << tried << "/"
                 << max_tries << " in " << backoff_ms
                 << "ms: " << status.ToString();

    if (!co_await WaitBackoff(backoff_ms)) {
      co_return Status::Abort("object storage is shutting down");
    }
  }
}

Future<Status> ObjectStorage::Get(BlockHandle handle, uint64_t offset,
                                  uint32_t length, char* buffer,
                                  ObjectGetOption option) {
  const uint32_t max_tries =
      std::max(option.max_tries > 0 ? option.max_tries
                                    : FLAGS_storage_download_max_tries,
               1U);
  const uint32_t notfound_max_tries =
      option.retry_notfound
          ? std::max(FLAGS_storage_download_notfound_max_tries, 1U)
          : 1;
  const std::string key = handle.StoreKey();

  uint32_t tried = 0;
  uint32_t notfound_tried = 0;
  while (true) {
    Status status = co_await GetOnce(handle.fs_id, key, offset, length, buffer);
    if (status.ok()) {
      co_return status;
    }

    uint64_t backoff_ms;
    if (status.IsNotFound()) {
      if (++notfound_tried >= notfound_max_tries) {
        LOG(WARNING) << "Fail to retrieve " << key << ", object not found "
                     << notfound_tried << "/" << notfound_max_tries << ": "
                     << status.ToString();
        co_return status;
      }

      backoff_ms = NotFoundBackoffMs(notfound_tried);
      num_download_notfound_retry_ << 1;
      LOG(WARNING) << "Fail to retrieve " << key
                   << ", object not found, will retry " << notfound_tried << "/"
                   << notfound_max_tries << " in " << backoff_ms << "ms";
    } else if (!IsRetriable(status)) {
      LOG(ERROR) << "Fail to retrieve " << key
                 << ", unretriable: " << status.ToString();
      co_return status;
    } else if (++tried >= max_tries) {
      LOG(ERROR) << "Fail to retrieve " << key << ", out of tries " << tried
                 << "/" << max_tries << ": " << status.ToString();
      co_return status;
    } else {
      backoff_ms = GetBackoffMs(tried);
      num_download_retry_ << 1;
      LOG(WARNING) << "Fail to retrieve " << key << ", will retry " << tried
                   << "/" << max_tries << " in " << backoff_ms
                   << "ms: " << status.ToString();
    }

    if (!co_await WaitBackoff(backoff_ms)) {
      co_return Status::Abort("object storage is shutting down");
    }
  }
}

Future<Status> ObjectStorage::PutOnce(uint64_t fs_id, const std::string& key,
                                      const blockaccess::PutPayload& payload) {
  auto context =
      std::make_shared<blockaccess::PutObjectAsyncContext>(key, payload);
  return SubmitAsync(fs_id, std::move(context),
                     [](auto* accesser, const auto& ctx) {
                       accesser->AsyncPut(ctx->origin_key, ctx);
                     });
}

Future<Status> ObjectStorage::GetOnce(uint64_t fs_id, const std::string& key,
                                      uint64_t offset, uint32_t length,
                                      char* buffer) {
  auto context = std::make_shared<blockaccess::GetObjectAsyncContext>(key);
  context->offset = static_cast<off_t>(offset);
  context->len = length;
  context->buf = buffer;
  return SubmitAsync(fs_id, std::move(context),
                     [](auto* accesser, const auto& ctx) {
                       accesser->AsyncGet(ctx->origin_key, ctx);
                     });
}

template <typename Context, typename Submit>
Future<Status> ObjectStorage::SubmitAsync(uint64_t fs_id,
                                          std::shared_ptr<Context> context,
                                          Submit submit) {
  ObjectCompletion completion;
  context->cb = [&completion](const std::shared_ptr<Context>& ctx) {
    completion.Complete(ctx->status);
  };

  const Status status = co_await GetGlobalWorkers()->Submit(
      [this, fs_id, &context, &submit]() -> Status {
        blockaccess::BlockAccesser* accesser = nullptr;
        Status status = client_->GetOrCreate(fs_id, &accesser);
        if (!status.ok()) {
          return status;
        }
        if (!client_->running()) {
          return Status::Abort("object storage is shutting down");
        }
        submit(accesser, context);
        return Status::OK();
      });
  if (!status.ok()) {
    co_return status;
  }
  co_return co_await completion.promise.GetFuture();
}

blockaccess::PutPayload ObjectStorage::PayloadOf(BufferViews block) {
  std::vector<blockaccess::PayloadSegment> segments;
  segments.reserve(block.size());
  for (const BufferView& range : block) {
    segments.push_back({static_cast<const char*>(range.data), range.size});
  }
  return blockaccess::PutPayload::Build(std::move(segments));
}

bool ObjectStorage::IsRetriable(const Status& status) {
  return !status.IsNotFound() && !status.IsNotSupport() && !status.IsAbort();
}

uint64_t ObjectStorage::PutBackoffMs(uint32_t tried) {
  const uint64_t base = FLAGS_storage_upload_retry_backoff_base_ms;
  return std::min(base * tried * tried, kPutBackoffCapMs);
}

uint64_t ObjectStorage::GetBackoffMs(uint32_t tried) {
  const uint64_t base = FLAGS_storage_download_retry_backoff_base_ms;
  return std::min(base * tried, kGetBackoffCapMs);
}

uint64_t ObjectStorage::NotFoundBackoffMs(uint32_t tried) {
  const uint64_t base = FLAGS_storage_download_notfound_retry_backoff_base_ms;
  return std::min(base * tried, kGetBackoffCapMs);
}

Future<bool> ObjectStorage::WaitBackoff(uint64_t backoff_ms) {
  for (uint64_t waited = 0; waited < backoff_ms; waited += kBackoffSliceMs) {
    if (!client_->running()) {
      co_return false;
    }

    auto timeout = std::chrono::milliseconds(
        std::min(kBackoffSliceMs, backoff_ms - waited));
    co_await Sleep(timeout);
  }
  co_return client_->running();
}

}  // namespace blockcache
}  // namespace dingofs
