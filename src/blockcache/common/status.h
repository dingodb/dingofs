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

#ifndef DINGOFS_BLOCKCACHE_COMMON_STATUS_H_
#define DINGOFS_BLOCKCACHE_COMMON_STATUS_H_

#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <new>
#include <utility>

#include "common/status.h"
#include "dingofs/error.pb.h"

namespace dingofs {
namespace blockcache {

template <typename T>
class StatusOr {
 public:
  StatusOr(Status status) : status_(std::move(status)) {
    CHECK(!status_.ok()) << "StatusOr built from an OK status carries no value";
  }

  StatusOr(T value) : has_value_(true) { new (&value_) T(std::move(value)); }

  ~StatusOr() { Destroy(); }

  StatusOr(const StatusOr&) = delete;
  StatusOr& operator=(const StatusOr&) = delete;

  StatusOr(StatusOr&& o) noexcept
      : status_(std::move(o.status_)), has_value_(o.has_value_) {
    if (has_value_) {
      new (&value_) T(std::move(o.value_));
      o.Destroy();
    }
  }

  StatusOr& operator=(StatusOr&& o) noexcept {
    if (this != &o) {
      Destroy();
      status_ = std::move(o.status_);
      has_value_ = o.has_value_;
      if (has_value_) {
        new (&value_) T(std::move(o.value_));
        o.Destroy();
      }
    }
    return *this;
  }

  bool ok() const { return has_value_; }
  const Status& status() const { return status_; }

  T& value() & {
    CHECK(has_value_) << "StatusOr has no value: " << status_.ToString();
    return value_;
  }

  T&& value() && {
    CHECK(has_value_) << "StatusOr has no value: " << status_.ToString();
    return std::move(value_);
  }

  T* operator->() { return &value(); }
  T& operator*() & { return value(); }
  T&& operator*() && { return std::move(value()); }

 private:
  void Destroy() {
    if (has_value_) {
      value_.~T();
      has_value_ = false;
    }
  }

  Status status_;
  bool has_value_ = false;
  union {
    T value_;
  };
};

Status ToStatus(int sys_code, const char* what);
Status ToStatus(pb::error::Errno errno_code);
Status ToStatus(ibv_wc_status status, const char* what);
pb::error::Errno ToErrno(const Status& status);

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_COMMON_STATUS_H_
