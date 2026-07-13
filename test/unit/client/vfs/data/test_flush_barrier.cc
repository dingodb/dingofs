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

#include <gtest/gtest.h>

#include <utility>
#include <vector>

#include "client/vfs/data/slice/flush_barrier.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace {

FlushBarrier::BlockInfo Block(uint32_t index) {
  return {.index = index, .size = 4096, .start_us = index + 100};
}

StatusCallback CaptureCompletion(int* calls, Status* result) {
  return [calls, result](Status status) {
    ++(*calls);
    *result = std::move(status);
  };
}

TEST(FlushBarrierTest, TransientZeroBeforeAllUploadsRegisteredDoesNotComplete) {
  FlushBarrier barrier("ut-barrier");
  int calls = 0;
  Status result;
  barrier.TrackStreamingUpload(Block(0));

  barrier.FinishUpload(0, Status::OK());
  EXPECT_EQ(calls, 0);
  EXPECT_EQ(barrier.Inflight(), 0);

  EXPECT_TRUE(barrier.RegisterFinalUploads({Block(1)},
                                           CaptureCompletion(&calls, &result)));
  EXPECT_EQ(barrier.Inflight(), 1);

  barrier.FinishUpload(1, Status::OK());
  EXPECT_EQ(calls, 1);
  EXPECT_TRUE(result.ok());
}

TEST(FlushBarrierTest, EmptyFinalRegistrationCompletesImmediately) {
  FlushBarrier barrier("ut-barrier");
  int calls = 0;
  Status result;
  EXPECT_FALSE(
      barrier.RegisterFinalUploads({}, CaptureCompletion(&calls, &result)));
  EXPECT_EQ(calls, 1);
  EXPECT_TRUE(result.ok());
}

TEST(FlushBarrierTest, RegisteredUploadsCompleteOutOfOrderWithFirstError) {
  FlushBarrier barrier("ut-barrier");
  int calls = 0;
  Status result;
  barrier.TrackStreamingUpload(Block(0));
  EXPECT_TRUE(barrier.RegisterFinalUploads({Block(1), Block(2)},
                                           CaptureCompletion(&calls, &result)));

  barrier.FinishUpload(2, Status::IoError("first"));
  barrier.FinishUpload(0, Status::IoError("second"));
  EXPECT_EQ(calls, 0);
  barrier.FinishUpload(1, Status::OK());

  EXPECT_EQ(calls, 1);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.ToString().find("first"), std::string::npos);
}

TEST(FlushBarrierTest,
     ErrorBeforeFinalRegistrationFailsWithoutRegisteringRemaining) {
  FlushBarrier barrier("ut-barrier");
  int calls = 0;
  Status result;
  barrier.TrackStreamingUpload(Block(0));
  barrier.FinishUpload(0, Status::IoError("streaming"));

  EXPECT_FALSE(barrier.RegisterFinalUploads(
      {Block(1)}, CaptureCompletion(&calls, &result)));
  EXPECT_EQ(calls, 1);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(barrier.Inflight(), 0);
}

TEST(FlushBarrierTest, AbortDeliversOnceAndLateUploadOnlyCleansUp) {
  FlushBarrier barrier("ut-barrier");
  int calls = 0;
  Status result;
  barrier.TrackStreamingUpload(Block(0));

  barrier.AbortFlush(Status::Internal("orchestration"),
                     CaptureCompletion(&calls, &result));
  EXPECT_EQ(calls, 1);
  EXPECT_FALSE(result.ok());

  barrier.FinishUpload(0, Status::OK());
  EXPECT_EQ(calls, 1);
  EXPECT_EQ(barrier.Inflight(), 0);
}

}  // namespace
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
