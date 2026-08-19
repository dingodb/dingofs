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

#include "blockcache/core/runtime/mesh.h"

#include <glog/logging.h>

namespace dingofs {
namespace blockcache {

template <typename Func>
[[gnu::always_inline]] static inline void ForEachPeer(uint64_t bits,
                                                      unsigned base_peer,
                                                      Func func) {
  while (bits != 0) {
    func(base_peer + static_cast<unsigned>(__builtin_ctzll(bits)));
    bits &= bits - 1;
  }
}

bool MeshPoller::Poll() {
  Mesh& mesh = Mesh::Instance();
  bool work = mesh.Flush(shard_);
  work |= mesh.Drain(shard_);
  return work;
}

bool MeshPoller::PurePoll() { return Mesh::Instance().HasWork(shard_); }

void Mesh::Init(unsigned shard_count) {
  CHECK(channels_ == nullptr) << "mesh built twice";
  CHECK(shard_count != 0) << "mesh needs at least one shard";

  shard_count_ = shard_count;
  word_count_ = (shard_count + kBitsPerWord - 1) / kBitsPerWord;
  channels_ = new MeshChannel[shard_count * shard_count]();
  links_ = new MeshLink[shard_count * shard_count]();
  notify_words_ = new NotifyWord[shard_count * word_count_]();
  dirty_words_ = new DirtyWord[shard_count * word_count_]();
  reactors_ = new Reactor*[shard_count]();
  pollers_ = new MeshPoller[shard_count]();
  inboxes_ = new ShardInbox[shard_count]();
  for (unsigned s = 0; s < shard_count; ++s) {
    pollers_[s].Bind(s);
  }
  InitLinks();
}

void Mesh::Destroy() {
  delete[] channels_;
  delete[] links_;
  delete[] notify_words_;
  delete[] dirty_words_;
  delete[] reactors_;
  delete[] pollers_;
  delete[] inboxes_;
  channels_ = nullptr;
  links_ = nullptr;
  notify_words_ = nullptr;
  dirty_words_ = nullptr;
  reactors_ = nullptr;
  pollers_ = nullptr;
  inboxes_ = nullptr;
  shard_count_ = 0;
  word_count_ = 0;
}

bool Mesh::HasWork(unsigned shard) const {
  const NotifyWord* notify = &notify_words_[shard * word_count_];
  const DirtyWord* dirty = &dirty_words_[shard * word_count_];
  for (unsigned i = 0; i < word_count_; ++i) {
    if (notify[i].bits.load(std::memory_order_acquire) != 0) {
      return true;
    }

    if (dirty[i].bits != 0) {
      return true;
    }
  }
  return false;
}

bool Mesh::Flush(unsigned shard) {
  const DirtyWord* dirty = &dirty_words_[shard * word_count_];
  MeshLink* links = &links_[shard * shard_count_];
  bool work = false;
  for (unsigned i = 0; i < word_count_; ++i) {
    ForEachPeer(dirty[i].bits, i * kBitsPerWord,
                [&](unsigned peer) { work |= links[peer].Flush(); });
  }
  return work;
}

bool Mesh::Drain(unsigned shard) {
  NotifyWord* notify = &notify_words_[shard * word_count_];
  MeshLink* links = &links_[shard * shard_count_];
  bool work = false;
  for (unsigned i = 0; i < word_count_; ++i) {
    if (notify[i].bits.load(std::memory_order_relaxed) == 0) {
      continue;
    }

    const uint64_t bits = notify[i].bits.exchange(0, std::memory_order_acquire);
    ForEachPeer(bits, i * kBitsPerWord,
                [&](unsigned peer) { work |= links[peer].Drain(); });
  }
  return work;
}

void Mesh::InitLinks() {
  const unsigned n = shard_count_;
  const unsigned w = word_count_;
  for (unsigned owner = 0; owner < n; ++owner) {
    for (unsigned peer = 0; peer < n; ++peer) {
      links_[(owner * n) + peer].Init(
          &channels_[(peer * n) + owner],  // owner calls peer
          &channels_[(owner * n) + peer],  // peer calls owner
          NotifyBit(&notify_words_[(peer * w) + (owner / kBitsPerWord)].bits,
                    owner % kBitsPerWord),
          DirtyBit(&dirty_words_[(owner * w) + (peer / kBitsPerWord)].bits,
                   peer % kBitsPerWord),
          &reactors_[peer]);
    }
  }
}

}  // namespace blockcache
}  // namespace dingofs
