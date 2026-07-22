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

/*
 * Project: DingoFS
 * Created Date: 2026-07-22
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/two_random_policy.h"

#include <glog/logging.h>

namespace dingofs {
namespace cache {

static CacheItem KV(const std::string& filename, const CacheValue& value) {
  CacheKey key;
  CHECK(key.ParseFromFilename(filename)) << "filename = " << filename;
  return CacheItem(key, value);
}

void TwoRandomPolicy::Add(const CacheKey& key, const CacheValue& value) {
  auto [iter, ok] = entries_.emplace(key.Filename(), Entry{value, 0});
  if (!ok) {  // duplicate add: refresh the value in place
    iter->second.value = value;
    return;
  }
  iter->second.index_pos = index_.size();
  index_.push_back(iter);
}

bool TwoRandomPolicy::Touch(const CacheKey& key, CacheValue* value) {
  auto iter = entries_.find(key.Filename());
  if (iter == entries_.end()) {
    return false;
  }

  auto& v = iter->second.value;
  v.atime = iutil::TimeNow();
  if (v.freq < 3) {
    v.freq++;
  }
  *value = v;
  return true;
}

bool TwoRandomPolicy::Exist(const CacheKey& key) const {
  return entries_.count(key.Filename()) != 0;
}

bool TwoRandomPolicy::Get(const CacheKey& key, CacheValue* value) const {
  auto iter = entries_.find(key.Filename());
  if (iter == entries_.end()) {
    return false;
  }
  *value = iter->second.value;
  return true;
}

bool TwoRandomPolicy::Delete(const CacheKey& key, CacheValue* deleted) {
  auto iter = entries_.find(key.Filename());
  if (iter == entries_.end()) {
    return false;
  }
  *deleted = iter->second.value;
  RemoveEntry(iter);
  return true;
}

CacheItems TwoRandomPolicy::Evict(FilterFunc filter) {
  CacheItems evicted;
  // bounded attempts so an all-skipping filter always terminates
  size_t attempts = entries_.size() + 1024;
  while (!entries_.empty() && attempts-- > 0) {
    auto victim = Sample();
    auto rc = filter(victim->second.value);
    if (rc == FilterStatus::kEvictIt) {
      evicted.emplace_back(KV(victim->first, victim->second.value));
      RemoveEntry(victim);
    } else if (rc == FilterStatus::kSkip) {
      // try another pair
    } else if (rc == FilterStatus::kFinish) {
      break;
    } else {
      CHECK(false);  // never happen
    }
  }
  return evicted;
}

CacheItems TwoRandomPolicy::Sweep(FilterFunc filter) {
  CacheItems evicted;
  for (auto iter = entries_.begin(); iter != entries_.end();) {
    auto curr = iter++;
    auto rc = filter(curr->second.value);
    if (rc == FilterStatus::kEvictIt) {
      evicted.emplace_back(KV(curr->first, curr->second.value));
      RemoveEntry(curr);
    } else if (rc == FilterStatus::kSkip) {
      // do nothing
    } else if (rc == FilterStatus::kFinish) {
      break;
    } else {
      CHECK(false);  // never happen
    }
  }
  return evicted;
}

size_t TwoRandomPolicy::Size() const { return entries_.size(); }

void TwoRandomPolicy::Clear() {
  entries_.clear();
  index_.clear();
}

void TwoRandomPolicy::RemoveEntry(Map::iterator iter) {
  // swap-remove from the sampling index
  size_t pos = iter->second.index_pos;
  index_[pos] = index_.back();
  index_[pos]->second.index_pos = pos;
  index_.pop_back();
  entries_.erase(iter);
}

// power of two choices: pick two uniform samples, victim is the older one
TwoRandomPolicy::Map::iterator TwoRandomPolicy::Sample() {
  auto a = index_[rng_() % index_.size()];
  auto b = index_[rng_() % index_.size()];
  return (a->second.value.atime < b->second.value.atime) ? a : b;
}

}  // namespace cache
}  // namespace dingofs
