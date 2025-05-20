/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: Thur March 28th 2019
 * Author: lixiaocui
 */

#include "mds/idgenerator/etcd_id_generator.h"

#include <string>

#include "fmt/format.h"
#include "glog/logging.h"
#include "utils/string_util.h"

namespace dingofs {
namespace idgenerator {

static const int kRetryTimes = 16;

bool EtcdIdGenerator::Init() {
  CHECK(client_ != nullptr) << "client is nullptr.";
  CHECK(!key_.empty()) << "key is empty.";

  // get alloc id
  uint64_t alloc_id;
  if (!GetOrPutAllocId(&alloc_id)) {
    LOG(ERROR) << fmt::format("[idalloc.{}] init get alloc id fail.", key_);
    return false;
  }

  next_id_ = alloc_id;
  last_alloc_id_ = alloc_id;

  return true;
}

int EtcdIdGenerator::GenId(uint64_t num, uint64_t* id) {
  if (num == 0) {
    LOG(ERROR) << fmt::format("[idalloc.{}] num cant not 0.", key_);
    return -1;
  }

  utils::WriteLockGuard guard(lock_);

  if (next_id_ + num > last_alloc_id_) {
    if (!AllocateIds(std::max(num, bundle_size_))) {
      LOG(ERROR) << fmt::format("[idalloc.{}] allocate id fail.", key_);
      return -1;
    }
  }

  // allocate id
  *id = next_id_;
  next_id_ += num;
  VLOG(3) << fmt::format("[idalloc.{}] alloc id: {}", key_, *id);

  return 0;
}

bool EtcdIdGenerator::DecodeID(const std::string& value, uint64_t* out) {
  return utils::StringToUll(value, out);
}

std::string EtcdIdGenerator::EncodeID(uint64_t value) {
  return std::to_string(value);
}

bool EtcdIdGenerator::AllocateIds(uint64_t bundle_size) {
  uint64_t prev_last_alloc_id = last_alloc_id_;
  int retry = 0;
  do {
    std::string prev_value = EncodeID(last_alloc_id_);
    uint64_t new_alloc_id = last_alloc_id_ + bundle_size;
    int ret = client_->CompareAndSwap(key_, prev_value, EncodeID(new_alloc_id));
    if (ret == EtcdErrCode::EtcdOK) {
      LOG(INFO) << fmt::format("[idalloc.{}] allocate id range [{}, {}).", key_,
                               last_alloc_id_, new_alloc_id);
      if (last_alloc_id_ != prev_last_alloc_id) {
        next_id_ = last_alloc_id_;
      }
      last_alloc_id_ = new_alloc_id;
      return true;

    } else if (ret == EtcdErrCode::EtcdValueNotEqual) {
      uint64_t alloc_id;
      if (!GetOrPutAllocId(&alloc_id)) {
        return false;
      }

      LOG(WARNING) << fmt::format(
          "[idalloc.{}] compare value fail, pre_id({}) alloc_id({}).", key_,
          prev_value, alloc_id);

      if (last_alloc_id_ < alloc_id) {
        last_alloc_id_ = alloc_id;

      } else if (last_alloc_id_ > alloc_id) {
        int ret = client_->Put(key_, EncodeID(last_alloc_id_));
        if (ret != EtcdErrCode::EtcdOK) {
          LOG(ERROR) << fmt::format("[idalloc.{}] put value fail, ret: {}.",
                                    key_, ret);
          return false;
        }
      }

    } else {
      LOG(ERROR) << fmt::format("[idalloc.{}] cas fail, ret: {}.", key_, ret);
      return false;
    }

  } while (++retry < kRetryTimes);

  LOG(ERROR) << fmt::format("[idalloc.{}] exceed max retry times.", key_);

  return false;
}

bool EtcdIdGenerator::GetOrPutAllocId(uint64_t* alloc_id) {
  do {
    std::string value;
    int ret = client_->Get(key_, &value);
    if (ret == EtcdErrCode::EtcdOK) {
      CHECK(DecodeID(value, alloc_id))
          << fmt::format("[idalloc.{}] decode id valud error.", key_);
      return true;

    } else if (ret == EtcdErrCode::EtcdKeyNotExist) {
      int ret = client_->Put(key_, EncodeID(last_alloc_id_));
      if (ret != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << fmt::format("[idalloc.{}] put value fail, ret: {}.", key_,
                                  ret);
        return false;
      }

    } else {
      LOG(ERROR) << fmt::format("[idalloc.{}] get value fail, ret: {}.", key_,
                                ret);
      return false;
    }

  } while (true);

  return false;
}

}  // namespace idgenerator
}  // namespace dingofs
