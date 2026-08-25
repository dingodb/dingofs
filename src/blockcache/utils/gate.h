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

#ifndef DINGOFS_BLOCKCACHE_UTILS_GATE_H_
#define DINGOFS_BLOCKCACHE_UTILS_GATE_H_

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>

#include "blockcache/core/reactor/coroutine.h"

namespace dingofs {
namespace blockcache {

template <typename Pred>
Future<> SleepWhile(Pred pred, uint64_t ms) {
  static constexpr uint64_t kStepMs = 100;
  while (ms > 0 && pred()) {
    const uint64_t step = std::min(ms, kStepMs);
    co_await Sleep(std::chrono::milliseconds(step));
    ms -= step;
  }
}

// Tracks in-flight background work so shutdown can wait for it.
class Gate {
 public:
  Gate() = default;

  Gate(const Gate&) = delete;
  Gate& operator=(const Gate&) = delete;
  class Holder {
   public:
    explicit Holder(Gate& g) : gate_(g.TryEnter() ? &g : nullptr) {}
    ~Holder() {
      if (gate_ != nullptr) {
        gate_->Leave();
      }
    }

    Holder(const Holder&) = delete;
    Holder& operator=(const Holder&) = delete;

    Holder(Holder&& o) noexcept : gate_(o.gate_) { o.gate_ = nullptr; }
    Holder& operator=(Holder&&) = delete;

    bool ok() const { return gate_ != nullptr; }

   private:
    Gate* gate_;
  };

  bool TryEnter() {
    if (closed_) {
      return false;
    }
    ++count_;
    return true;
  }

  void Leave() {
    if (--count_ == 0 && closed_) {
      out_.SetValue();
    }
  }

  Future<> Close() {
    closed_ = true;
    if (count_ == 0) {
      return MakeReadyFuture<>();
    }
    return out_.GetFuture();
  }

 private:
  size_t count_ = 0;
  bool closed_ = false;
  Promise<> out_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_UTILS_GATE_H_
