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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <string>

#include "common/blockaccess/accesser_common.h"
#include "common/blockaccess/block_accesser.h"
#include "common/blockaccess/rados/rados_accesser.h"
#include "common/blockaccess/s3/s3_accesser.h"
#include "common/status.h"

namespace dingofs {
namespace blockaccess {
namespace unit_test {

// ---- BlockAccesserImpl async-delete wrapper (LocalFile backend) ----

class BlockAccesserImplTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = "/tmp/dingofs_ba" + std::to_string(getpid());
    BlockAccessOptions options;
    options.type = AccesserType::kLocalFile;
    options.file_options.path = root_;
    // No throttle limits.
    options.throttle_options = BlockAccesserThrottleOptions{};

    accesser_ = std::make_unique<BlockAccesserImpl>(options);
    ASSERT_TRUE(accesser_->Init().ok());
  }

  void TearDown() override {
    if (accesser_) {
      accesser_->Destroy();
      accesser_.reset();
    }
    if (std::filesystem::exists(root_)) {
      std::filesystem::remove_all(root_);
    }
  }

  std::string root_;
  std::unique_ptr<BlockAccesserImpl> accesser_;
};

// The wrapper installs its own callback (log + bvar), but must restore the
// caller's original callback before invoking it, so the caller can reuse the
// same context on retry. Verify the origin callback fires with the final
// status on both the first call and a reuse (retry).
TEST_F(BlockAccesserImplTest, AsyncDeleteRestoresOriginCallbackOnRetry) {
  const std::string key = "wrap_delete_key";  // not present -> idempotent OK

  std::mutex mtx;
  std::condition_variable cv;
  int origin_calls = 0;
  Status last_status;

  auto ctx = std::make_shared<DeleteObjectAsyncContext>(key);
  ctx->cb = [&](const DeleteObjectAsyncContextSPtr& c) {
    std::lock_guard<std::mutex> lock(mtx);
    ++origin_calls;
    last_status = c->status;
    cv.notify_one();
  };

  auto wait_for = [&](int expected) {
    std::unique_lock<std::mutex> lock(mtx);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10),
                            [&] { return origin_calls >= expected; }));
  };

  // First call.
  accesser_->AsyncDelete(key, ctx);
  wait_for(1);
  EXPECT_TRUE(last_status.ok()) << last_status.ToString();

  // Reuse the SAME context (retry). If the wrapper did not restore the origin
  // callback, this would re-wrap its own callback; the origin must still fire.
  accesser_->AsyncDelete(key, ctx);
  wait_for(2);
  EXPECT_EQ(origin_calls, 2);
  EXPECT_TRUE(last_status.ok()) << last_status.ToString();
}

TEST_F(BlockAccesserImplTest, AsyncBatchDeleteFiresOriginCallback) {
  std::list<std::string> keys = {"wbd_k1", "wbd_k2"};

  std::mutex mtx;
  std::condition_variable cv;
  bool called = false;
  Status status;

  auto ctx = std::make_shared<BatchDeleteObjectAsyncContext>(keys);
  ctx->cb = [&](const BatchDeleteObjectAsyncContextSPtr& c) {
    std::lock_guard<std::mutex> lock(mtx);
    status = c->status;
    called = true;
    cv.notify_one();
  };

  accesser_->AsyncBatchDelete(keys, ctx);
  {
    std::unique_lock<std::mutex> lock(mtx);
    ASSERT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return called; }));
  }
  EXPECT_TRUE(status.ok()) << status.ToString();
}

// ---- Rados async batch delete: empty keys fast path (no cluster needed) ----

// An empty batch must short-circuit to OK and fire the callback synchronously
// without touching the (unconnected) cluster handle.
TEST(RadosAccesserTest, AsyncBatchDeleteEmptyKeys) {
  RadosOptions options;  // not connected; empty path must not touch cluster_
  RadosAccesser accesser(options);

  bool called = false;
  Status status = Status::Internal("unset");

  auto ctx =
      std::make_shared<BatchDeleteObjectAsyncContext>(std::list<std::string>{});
  ctx->cb = [&](const BatchDeleteObjectAsyncContextSPtr& c) {
    status = c->status;
    called = true;
  };

  accesser.AsyncBatchDelete({}, ctx);

  EXPECT_TRUE(called) << "empty-batch callback must fire synchronously";
  EXPECT_TRUE(status.ok()) << status.ToString();
}

// Sync BatchDelete must also short-circuit on empty keys (same semantics as
// AsyncBatchDelete) instead of issuing an empty DeleteObjects request. The
// empty path returns before touching the (unconstructed) S3 client.
TEST(S3AccesserTest, BatchDeleteEmptyKeys) {
  S3Options options;  // client not initialized; empty path must not use client_
  S3Accesser accesser(options);

  EXPECT_TRUE(accesser.BatchDelete({}).ok());
}

}  // namespace unit_test
}  // namespace blockaccess
}  // namespace dingofs
