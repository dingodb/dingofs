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

#include "common/blockaccess/rados/osd_map_refresher.h"

#include <bvar/bvar.h>
#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <exception>
#include <random>
#include <utility>

namespace dingofs {
namespace blockaccess {
namespace {

constexpr auto kSlowRefreshThreshold = std::chrono::seconds(30);

bvar::Adder<int64_t> g_refresh_attempts{
    "dingofs_rados_map_refresh_attempt_total"};
bvar::Adder<int64_t> g_refresh_failures{
    "dingofs_rados_map_refresh_failure_total"};

}  // namespace

OsdMapRefresher::OsdMapRefresher(Options options, RefreshFn refresh_fn)
    : options_(std::move(options)),
      refresh_fn_(std::move(refresh_fn)),
      random_(std::random_device{}()) {}

OsdMapRefresher::~OsdMapRefresher() { Stop(); }

bool OsdMapRefresher::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (started_) {
    return true;
  }
  if (!options_.enabled) {
    return true;
  }
  if (!refresh_fn_ || options_.interval <= std::chrono::milliseconds::zero()) {
    LOG(ERROR) << "Invalid OSDMap refresher options";
    return false;
  }

  stopping_ = false;
  try {
    thread_ = std::thread(&OsdMapRefresher::Run, this);
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to create OSDMap refresher thread: " << e.what();
    return false;
  }
  started_ = true;
  return true;
}

void OsdMapRefresher::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!started_) {
      return;
    }
    stopping_ = true;
  }
  cv_.notify_all();

  if (thread_.joinable()) {
    // The public librados latest-map API has no request deadline or cancel
    // handle. If its MON version request or OSDMap subscription never
    // completes, this join can block shutdown indefinitely. Do not detach the
    // thread: refresh_fn_ may still access the rados_t owned by RadosAccesser.
    thread_.join();
  }

  std::lock_guard<std::mutex> lock(mutex_);
  started_ = false;
}

void OsdMapRefresher::Run() {
  while (true) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_) {
        return;
      }
    }

    g_refresh_attempts << 1;
    const auto begin = std::chrono::steady_clock::now();
    const int rc = refresh_fn_();
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    const auto duration_us =
        std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();

    if (rc != 0) {
      g_refresh_failures << 1;
    }

    if (rc != 0) {
      LOG(WARNING) << "Failed to refresh latest RADOS OSDMap, rc=" << rc
                   << ", duration=" << duration_us << " us";
    } else if (elapsed >= kSlowRefreshThreshold) {
      LOG(INFO) << "Slow refresh of latest RADOS OSDMap, duration="
                << duration_us << " us";
    } else {
      VLOG(1) << "Refreshed latest RADOS OSDMap in " << duration_us << " us";
    }

    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait_for(lock, NextInterval(), [this] { return stopping_; });
    if (stopping_) {
      return;
    }
  }
}

std::chrono::milliseconds OsdMapRefresher::NextInterval() {
  const uint32_t jitter_pct = std::min(options_.jitter_pct, 50U);
  if (jitter_pct == 0) {
    return options_.interval;
  }

  const int64_t base = options_.interval.count();
  const int64_t delta = base * jitter_pct / 100;
  std::uniform_int_distribution<int64_t> distribution(-delta, delta);
  return std::chrono::milliseconds(
      std::max<int64_t>(base + distribution(random_), 1));
}

}  // namespace blockaccess
}  // namespace dingofs
