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
#include <cstdlib>
#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/blockaccess/accesser_common.h"
#include "common/blockaccess/block_accesser.h"
#include "common/blockaccess/rados/rados_accesser.h"
#include "common/blockaccess/s3/aws/aws_s3_common.h"
#include "common/blockaccess/s3/s3_accesser.h"
#include "common/io_buffer.h"
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
      accesser_->Stop();
      accesser_.reset();
    }
    if (std::filesystem::exists(root_)) {
      std::filesystem::remove_all(root_);
    }
  }

  std::string root_;
  std::unique_ptr<BlockAccesserImpl> accesser_;
};

TEST(PutPayloadTest, SharesImmutableSegmentMetadata) {
  const std::string first = "hello";
  const std::string second = " world";
  auto payload = PutPayload::Build(
      {{first.data(), first.size()}, {second.data(), second.size()}});
  auto copy = payload;

  EXPECT_EQ(payload.Size(), first.size() + second.size());
  EXPECT_EQ(payload.SegmentCount(), 2);
  EXPECT_EQ(copy.Segments().data(), payload.Segments().data());
}

TEST(PutPayloadDeathTest, RejectsEmptyPayloadAndSegment) {
  EXPECT_DEATH((void)PutPayload::Build({}), "empty block payload");
  const char data = 'x';
  EXPECT_DEATH((void)PutPayload::Build({PayloadSegment{&data, 0}}),
               "empty segment");
}

TEST(SegmentedStreamBufTest, ReadsAndSeeksAcrossSegments) {
  const std::string first = "abc";
  const std::string second = "defgh";
  aws::SegmentedIOStream stream(PutPayload::Build(
      {{first.data(), first.size()}, {second.data(), second.size()}}));

  char output[6] = {};
  stream.read(output, 5);
  EXPECT_EQ(std::string(output, 5), "abcde");

  stream.clear();
  stream.seekg(2, std::ios_base::beg);
  stream.read(output, 6);
  EXPECT_EQ(std::string(output, 6), "cdefgh");

  stream.clear();
  stream.seekg(-3, std::ios_base::end);
  stream.read(output, 3);
  EXPECT_EQ(std::string(output, 3), "fgh");
}

TEST_F(BlockAccesserImplTest, SyncPutAcceptsSegmentedPayload) {
  const std::string first = "segmented ";
  const std::string second = "payload";
  auto payload = PutPayload::Build(
      {{first.data(), first.size()}, {second.data(), second.size()}});

  ASSERT_TRUE(accesser_->Put("multi_segment", payload).ok());
  std::string data;
  ASSERT_TRUE(accesser_->Get("multi_segment", &data).ok());
  EXPECT_EQ(data, first + second);
}

TEST_F(BlockAccesserImplTest, AsyncPutAcceptsSegmentedPayload) {
  const std::string first = "async ";
  const std::string second = "segments";
  auto ctx = std::make_shared<PutObjectAsyncContext>(
      "async_multi", PutPayload::Build({{first.data(), first.size()},
                                        {second.data(), second.size()}}));

  std::mutex mutex;
  std::condition_variable cv;
  bool complete = false;
  ctx->cb = [&](const PutObjectAsyncContextSPtr& result) {
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_TRUE(result->status.ok()) << result->status.ToString();
    complete = true;
    cv.notify_one();
  };
  accesser_->AsyncPut(ctx->origin_key, ctx);
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return complete; }));
  }

  std::string data;
  ASSERT_TRUE(accesser_->Get("async_multi", &data).ok());
  EXPECT_EQ(data, first + second);
}

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

// ---- Rados Put whole-object-replace semantics (needs a real cluster) ----

// Skipped unless a cluster is provided via environment:
//   DINGOFS_UT_RADOS_MON_HOST  e.g. 192.168.10.2
//   DINGOFS_UT_RADOS_KEY       e.g. $(ceph auth get-key client.admin)
//   DINGOFS_UT_RADOS_POOL      pool to write test objects into
//   DINGOFS_UT_RADOS_USER      optional, default client.admin
// Run once against a replicated pool and once against an EC pool with
// allow_ec_overwrites=false: a put over an existing object must succeed on
// both (offset-write based puts get EOPNOTSUPP on the EC pool), and a
// shorter re-put must truncate instead of leaving a stale tail.
class RadosPutSemanticsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const char* mon_host = std::getenv("DINGOFS_UT_RADOS_MON_HOST");
    const char* key = std::getenv("DINGOFS_UT_RADOS_KEY");
    const char* pool = std::getenv("DINGOFS_UT_RADOS_POOL");
    if (mon_host == nullptr || key == nullptr || pool == nullptr) {
      GTEST_SKIP() << "DINGOFS_UT_RADOS_{MON_HOST,KEY,POOL} not set";
    }

    RadosOptions options;
    options.mon_host = mon_host;
    options.key = key;
    options.pool_name = pool;
    const char* user = std::getenv("DINGOFS_UT_RADOS_USER");
    options.user_name = user != nullptr ? user : "client.admin";

    accesser_ = std::make_unique<RadosAccesser>(options);
    ASSERT_TRUE(accesser_->Init());

    key_ = std::string("dingofs_ut_put_semantics_") +
           ::testing::UnitTest::GetInstance()->current_test_info()->name();
  }

  void TearDown() override {
    if (accesser_) {
      (void)accesser_->Delete(key_);
      accesser_->Stop();
    }
  }

  // 1 MiB boundaries stay aligned to any practical EC stripe_width: EC pools
  // without overwrites accept the non-first segments only as aligned appends.
  static constexpr size_t kSegSize = 1ULL << 20;

  static std::string MakeSegment(char fill) {
    return std::string(kSegSize, fill);
  }

  static PutPayload BuildPayload(const std::vector<std::string>& segments) {
    std::vector<PayloadSegment> refs;
    refs.reserve(segments.size());
    for (const auto& segment : segments) {
      refs.push_back({segment.data(), segment.size()});
    }
    return PutPayload::Build(std::move(refs));
  }

  Status AsyncPutAndWait(const PutPayload& payload) {
    auto ctx = std::make_shared<PutObjectAsyncContext>(key_, payload);
    std::mutex mtx;
    std::condition_variable cv;
    bool done = false;
    ctx->cb = [&](const std::shared_ptr<PutObjectAsyncContext>&) {
      std::lock_guard<std::mutex> lock(mtx);
      done = true;
      cv.notify_one();
    };

    accesser_->AsyncPut(key_, ctx);

    std::unique_lock<std::mutex> lock(mtx);
    if (!cv.wait_for(lock, std::chrono::seconds(30), [&] { return done; })) {
      return Status::Internal("async put timed out");
    }
    return ctx->status;
  }

  std::unique_ptr<RadosAccesser> accesser_;
  std::string key_;
};

TEST_F(RadosPutSemanticsTest, MultiSegmentPutRoundTrip) {
  std::vector<std::string> segments = {MakeSegment('A'), MakeSegment('B')};
  auto status = accesser_->Put(key_, BuildPayload(segments));
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::string data;
  ASSERT_TRUE(accesser_->Get(key_, &data).ok());
  ASSERT_EQ(data.size(), 2 * kSegSize);
  EXPECT_EQ(data.front(), 'A');
  EXPECT_EQ(data[kSegSize - 1], 'A');
  EXPECT_EQ(data[kSegSize], 'B');
  EXPECT_EQ(data.back(), 'B');
}

TEST_F(RadosPutSemanticsTest, RePutExistingObject) {
  auto status =
      accesser_->Put(key_, BuildPayload({MakeSegment('A'), MakeSegment('A')}));
  ASSERT_TRUE(status.ok()) << status.ToString();

  // The re-upload of an already-persisted block (e.g. a reloaded stage
  // block): with offset writes this fails EOPNOTSUPP on EC pools.
  std::vector<std::string> segments = {MakeSegment('C'), MakeSegment('D')};
  status = accesser_->Put(key_, BuildPayload(segments));
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::string data;
  ASSERT_TRUE(accesser_->Get(key_, &data).ok());
  ASSERT_EQ(data.size(), 2 * kSegSize);
  EXPECT_EQ(data.front(), 'C');
  EXPECT_EQ(data.back(), 'D');
}

TEST_F(RadosPutSemanticsTest, ShorterRePutTruncates) {
  auto status =
      accesser_->Put(key_, BuildPayload({MakeSegment('A'), MakeSegment('A'),
                                         MakeSegment('A'), MakeSegment('A')}));
  ASSERT_TRUE(status.ok()) << status.ToString();

  status = accesser_->Put(key_, BuildPayload({MakeSegment('B')}));
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Get sizes the read from stat: a non-truncating put would return 4 MiB
  // here with a stale 'A' tail after the first MiB.
  std::string data;
  ASSERT_TRUE(accesser_->Get(key_, &data).ok());
  ASSERT_EQ(data.size(), kSegSize);
  EXPECT_EQ(data.front(), 'B');
  EXPECT_EQ(data.back(), 'B');
}

TEST_F(RadosPutSemanticsTest, AsyncRePutExistingObject) {
  auto status =
      accesser_->Put(key_, BuildPayload({MakeSegment('A'), MakeSegment('A')}));
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::vector<std::string> segments = {MakeSegment('E'), MakeSegment('F')};
  auto payload = BuildPayload(segments);
  status = AsyncPutAndWait(payload);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::string data;
  ASSERT_TRUE(accesser_->Get(key_, &data).ok());
  ASSERT_EQ(data.size(), 2 * kSegSize);
  EXPECT_EQ(data.front(), 'E');
  EXPECT_EQ(data.back(), 'F');
}

}  // namespace unit_test
}  // namespace blockaccess
}  // namespace dingofs
