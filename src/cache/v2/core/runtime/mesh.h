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

#ifndef DINGOFS_CACHE_V2_CORE_RUNTIME_MESH_H_
#define DINGOFS_CACHE_V2_CORE_RUNTIME_MESH_H_

#include <atomic>
#include <cstdint>

#include "cache/v2/core/reactor/poller.h"
#include "cache/v2/core/reactor/reactor.h"
#include "cache/v2/core/reactor/task.h"
#include "cache/v2/core/runtime/shard_inbox.h"
#include "cache/v2/utils/align.h"
#include "cache/v2/utils/containers/spsc_ring.h"

// The process-wide per-shard resource table, and the messaging on top of it:
// one MeshLink per (shard, peer) pair for shard-to-shard, one ShardInbox per
// shard for everyone outside. Reaching a shard never needs a Runtime handle
// -- see smp.h for the verbs.

namespace dingofs {
namespace cache {
namespace v2 {

class MeshWork : public Task {
 public:
  void RunAndDispose() noexcept final { Run(); }

  virtual void Run() noexcept = 0;
  virtual void OnComplete() noexcept = 0;

  MeshWork* next = nullptr;

 protected:
  ~MeshWork() = default;
};

struct MeshChannel {
  static constexpr uint32_t kDepth = 128;

  SpscRing<MeshWork*, kDepth> pending;
  SpscRing<MeshWork*, kDepth> completed;
};

struct alignas(kCacheLineSize) NotifyWord {
  std::atomic<uint64_t> bits{0};
};
struct alignas(kCacheLineSize) DirtyWord {
  uint64_t bits = 0;
};

// A link's bit in its PEER's notify word: "there is traffic for you from me".
class NotifyBit {
 public:
  NotifyBit() = default;
  NotifyBit(std::atomic<uint64_t>* word, unsigned bit)
      : word_(word), mask_(1ULL << bit) {}

  void Set() { word_->fetch_or(mask_, std::memory_order_release); }

 private:
  std::atomic<uint64_t>* word_ = nullptr;
  uint64_t mask_ = 0;
};

// A link's bit in its OWN dirty word (unflushed work); single writer.
class DirtyBit {
 public:
  DirtyBit() = default;
  DirtyBit(uint64_t* word, unsigned bit) : word_(word), mask_(1ULL << bit) {}

  void Set() { *word_ |= mask_; }
  void Clear() { *word_ &= ~mask_; }

 private:
  uint64_t* word_ = nullptr;
  uint64_t mask_ = 0;
};

class MeshBatch {
 public:
  void Push(MeshWork* item) {
    item->next = nullptr;
    if (tail_ != nullptr) {
      tail_->next = item;
    } else {
      head_ = item;
    }
    tail_ = item;
    ++size_;
  }

  template <typename Ring>
  uint32_t DrainInto(Ring& ring) {
    MeshWork* item = head_;
    uint32_t pushed = 0;
    while (item != nullptr) {
      MeshWork* next = item->next;
      if (!ring.Push(item)) {
        break;
      }
      item = next;
      ++pushed;
    }

    head_ = item;
    if (item == nullptr) {
      tail_ = nullptr;
    }
    size_ -= pushed;
    return pushed;
  }

  bool empty() const { return head_ == nullptr; }
  uint32_t size() const { return size_; }

 private:
  MeshWork* head_ = nullptr;
  MeshWork* tail_ = nullptr;
  uint32_t size_ = 0;
};

class alignas(kCacheLineSize) MeshLink {
 public:
  void Init(MeshChannel* my_calls, MeshChannel* peer_calls, NotifyBit notify,
            DirtyBit dirty, Reactor** peer_reactor_slot) {
    my_calls_ = my_calls;
    peer_calls_ = peer_calls;
    notify_ = notify;
    dirty_ = dirty;
    peer_reactor_slot_ = peer_reactor_slot;
  }

  void Submit(MeshWork* item) { Batch(submissions_, item); }
  void Respond(MeshWork* item) { Batch(responses_, item); }

  [[gnu::noinline]] bool Flush() {
    const uint32_t sent = submissions_.DrainInto(my_calls_->pending) +
                          responses_.DrainInto(peer_calls_->completed);
    if (sent == 0) {
      return false;
    }

    if (submissions_.empty() && responses_.empty()) {
      dirty_.Clear();
    }

    notify_.Set();
    (*peer_reactor_slot_)->Wakeup();
    return true;
  }

  bool Drain() {
    Reactor& reactor = ThisReactor();

    bool work = peer_calls_->pending.ConsumeAll([&reactor](MeshWork* item) {
      __builtin_prefetch(item);  // written by another core
      reactor.Schedule(item);
    }) != 0;

    work |= my_calls_->completed.ConsumeAll(
                [](MeshWork* item) { item->OnComplete(); }) != 0;

    return work;
  }

 private:
  static constexpr uint32_t kFlushBatch = 16;

  void Batch(MeshBatch& batch, MeshWork* item) {
    batch.Push(item);
    dirty_.Set();
    if (batch.size() >= kFlushBatch) {
      Flush();
    }
  }

  MeshBatch submissions_;              // my calls, waiting for a flush
  DirtyBit dirty_;                     // set while either batch holds work
  MeshBatch responses_;                // the peer's calls I have finished
  MeshChannel* my_calls_ = nullptr;    // my calls out, their completions back
  MeshChannel* peer_calls_ = nullptr;  // their calls in, my completions back
  NotifyBit notify_;
  Reactor** peer_reactor_slot_ = nullptr;  // shards attach theirs after Build
};

class MeshPoller final : public Poller {
 public:
  void Bind(unsigned shard) { shard_ = shard; }

  bool Poll() override;
  bool PurePoll() override;

 private:
  unsigned shard_ = 0;
};

class Mesh {
 public:
  static Mesh& Instance() {
    static Mesh instance;
    return instance;
  }

  Mesh() = default;

  Mesh(const Mesh&) = delete;
  Mesh& operator=(const Mesh&) = delete;

  void Init(unsigned shard_count);
  void Destroy();

  void AttachReactor(unsigned shard, Reactor* reactor) {
    DCHECK(reactors_[shard] == nullptr) << "shard attached twice";
    reactors_[shard] = reactor;
  }

  bool HasWork(unsigned shard) const;
  bool Flush(unsigned shard);
  bool Drain(unsigned shard);

  // False before Init and after Destroy, i.e. whenever no runtime is up.
  bool built() const { return shard_count_ != 0; }

  unsigned shard_count() const {
    DCHECK(shard_count_ != 0) << "mesh not built";
    return shard_count_;
  }

  MeshLink& LinkOf(unsigned owner, unsigned peer) {
    return links_[(owner * shard_count_) + peer];
  }

  Poller* PollerFor(unsigned shard) {
    return shard_count_ > 1 ? &pollers_[shard] : nullptr;
  }

  ShardInbox& InboxFor(unsigned shard) { return inboxes_[shard]; }

 private:
  static constexpr unsigned kBitsPerWord = 64;

  void InitLinks();

  MeshChannel* channels_ = nullptr;     // [callee * n + caller]
  MeshLink* links_ = nullptr;           // [owner * n + peer]   (owner-only)
  NotifyWord* notify_words_ = nullptr;  // [shard * words + w]  remote-set
  DirtyWord* dirty_words_ = nullptr;    // [shard * words + w]  owner-only
  Reactor** reactors_ = nullptr;        // [shard] peer wakeup handles only
  MeshPoller* pollers_ = nullptr;       // [shard]
  ShardInbox* inboxes_ = nullptr;       // [shard] outside -> shard
  unsigned shard_count_ = 0;
  unsigned word_count_ = 0;  // ceil(shard_count_ / kBitsPerWord)
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_RUNTIME_MESH_H_
