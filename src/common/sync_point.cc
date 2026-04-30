/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "common/sync_point.h"

#ifndef NDEBUG

#include <atomic>
#include <mutex>
#include <unordered_map>

namespace dingofs {

struct SyncPoint::Data {
  std::atomic<bool> enabled{false};
  std::mutex mutex;
  std::unordered_map<std::string, std::function<void(void*)>> callbacks;
};

SyncPoint* SyncPoint::GetInstance() {
  static SyncPoint instance;
  return &instance;
}

SyncPoint::SyncPoint() : impl_(new Data) {}

SyncPoint::~SyncPoint() { delete impl_; }

void SyncPoint::SetCallBack(const std::string& point,
                            const std::function<void(void*)>& callback) {
  std::lock_guard<std::mutex> lk(impl_->mutex);
  impl_->callbacks[point] = callback;
}

void SyncPoint::ClearAllCallBacks() {
  std::lock_guard<std::mutex> lk(impl_->mutex);
  impl_->callbacks.clear();
}

void SyncPoint::EnableProcessing() {
  impl_->enabled.store(true, std::memory_order_release);
}

void SyncPoint::DisableProcessing() {
  impl_->enabled.store(false, std::memory_order_release);
}

void SyncPoint::Process(const std::string& point, void* cb_arg) {
  if (!impl_->enabled.load(std::memory_order_acquire)) return;
  std::function<void(void*)> cb;
  {
    std::lock_guard<std::mutex> lk(impl_->mutex);
    auto it = impl_->callbacks.find(point);
    if (it == impl_->callbacks.end()) return;
    cb = it->second;
  }
  cb(cb_arg);
}

}  // namespace dingofs

#endif  // NDEBUG
