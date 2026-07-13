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

#include "client/vfs/data/slice/flush_barrier.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <utility>

namespace dingofs {
namespace client {
namespace vfs {

void FlushBarrier::TrackStreamingUpload(BlockInfo block) {
  std::lock_guard<std::mutex> lock(mutex_);
  CHECK(state_ == State::kAcceptingUploads) << fmt::format(
      "{} streaming upload index {} registered after final upload "
      "registration",
      tag_, block.index);
  auto [_, inserted] = inflight_.emplace(block.index, block);
  CHECK(inserted) << fmt::format("{} duplicate inflight block index {}", tag_,
                                 block.index);
}

bool FlushBarrier::RegisterFinalUploads(const std::vector<BlockInfo>& remaining,
                                        StatusCallback callback) {
  std::optional<Completion> completion;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK(state_ == State::kAcceptingUploads)
        << fmt::format("{} final uploads registered more than once", tag_);
    CHECK(callback) << tag_;

    if (!first_error_.ok()) {
      state_ = State::kCompletionDelivered;
      completion = Completion{first_error_, std::move(callback)};
    } else {
      for (const auto& block : remaining) {
        auto [_, inserted] = inflight_.emplace(block.index, block);
        CHECK(inserted) << fmt::format("{} duplicate inflight block index {}",
                                       tag_, block.index);
      }

      callback_.swap(callback);
      state_ = State::kAllUploadsRegistered;
      completion = TakeCompletionUnlocked();
    }
  }

  bool should_submit = !completion.has_value();
  Deliver(std::move(completion));
  return should_submit;
}

void FlushBarrier::FinishUpload(uint32_t index, const Status& status) {
  std::optional<Completion> completion;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t erased = inflight_.erase(index);
    CHECK_EQ(erased, 1) << fmt::format("{} missing inflight block index {}",
                                       tag_, index);

    if (!status.ok() && first_error_.ok()) {
      first_error_ = status;
    }
    completion = TakeCompletionUnlocked();
  }
  Deliver(std::move(completion));
}

void FlushBarrier::AbortFlush(Status status, StatusCallback callback) {
  Completion completion;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK(state_ == State::kAcceptingUploads) << fmt::format(
        "{} flush aborted after final upload registration", tag_);
    CHECK(callback) << tag_;
    state_ = State::kCompletionDelivered;
    if (first_error_.ok()) {
      first_error_ = status;
    }
    completion = Completion{first_error_, std::move(callback)};
  }
  completion.callback(std::move(completion.status));
}

size_t FlushBarrier::Inflight() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return inflight_.size();
}

Status FlushBarrier::FirstError() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return first_error_;
}

std::optional<FlushBarrier::Completion> FlushBarrier::TakeCompletionUnlocked() {
  if (state_ != State::kAllUploadsRegistered || !inflight_.empty()) {
    return std::nullopt;
  }

  state_ = State::kCompletionDelivered;
  Completion completion{first_error_, nullptr};
  completion.callback.swap(callback_);
  CHECK(completion.callback) << tag_;
  return completion;
}

void FlushBarrier::Deliver(std::optional<Completion> completion) {
  if (completion.has_value()) {
    completion->callback(std::move(completion->status));
  }
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
