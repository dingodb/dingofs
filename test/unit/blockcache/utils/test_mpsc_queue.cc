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
 * Created Date: 2026-10-04
 * Author: AI
 */

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "blockcache/utils/align.h"
#include "blockcache/utils/mpsc_queue.h"

namespace dingofs {
namespace blockcache {

// Work handed to a shard's inbox, which links itself into the queue.
struct Work : MpscNode {
  int producer = 0;
  int seq = 0;
};

static std::vector<int> GetSeqs(MpscNode* node) {
  std::vector<int> seqs;
  for (; node != nullptr; node = node->next) {
    seqs.push_back(static_cast<Work*>(node)->seq);
  }
  return seqs;
}

TEST(MpscQueueTest, PushBack) {
  MpscQueue queue;
  std::vector<Work> works(3);
  for (int i = 0; i < 3; i++) {
    works[i].seq = i;
    queue.PushBack(&works[i]);
    EXPECT_FALSE(queue.empty());
  }
  EXPECT_EQ(GetSeqs(queue.TakeAll()), (std::vector<int>{0, 1, 2}));
}

TEST(MpscQueueTest, TakeAll) {
  MpscQueue queue;
  std::vector<Work> works(4);
  for (int i = 0; i < 4; i++) {
    works[i].seq = i;
  }

  {  // everything at once, oldest first, and the queue is left empty
    for (int i = 0; i < 2; i++) {
      queue.PushBack(&works[i]);
    }
    EXPECT_EQ(GetSeqs(queue.TakeAll()), (std::vector<int>{0, 1}));
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.TakeAll(), nullptr);
  }

  {  // what comes after is the next batch
    for (int i = 2; i < 4; i++) {
      queue.PushBack(&works[i]);
    }
    EXPECT_EQ(GetSeqs(queue.TakeAll()), (std::vector<int>{2, 3}));
  }
}

TEST(MpscQueueTest, ConsumeAll) {
  MpscQueue queue;
  std::vector<int> seqs;
  const auto consume = [&seqs](MpscNode* node) {
    seqs.push_back(static_cast<Work*>(node)->seq);
  };

  {  // every node in push order; returns how many there were
    std::vector<Work> works(3);
    for (int i = 0; i < 3; i++) {
      works[i].seq = i;
      queue.PushBack(&works[i]);
    }
    EXPECT_EQ(queue.ConsumeAll(consume), 3);
    EXPECT_EQ(seqs, (std::vector<int>{0, 1, 2}));
    EXPECT_TRUE(queue.empty());
  }

  {  // none queued
    EXPECT_EQ(queue.ConsumeAll(consume), 0);
    EXPECT_EQ(seqs.size(), 3);
  }

  {  // the callback owns the node and may free it
    for (int i = 0; i < 3; i++) {
      auto* work = new Work();
      work->seq = i;
      queue.PushBack(work);
    }
    int freed = 0;
    EXPECT_EQ(queue.ConsumeAll([&freed](MpscNode* node) {
      delete static_cast<Work*>(node);
      freed++;
    }),
              3);
    EXPECT_EQ(freed, 3);
  }
}

TEST(MpscQueueTest, Empty) {
  MpscQueue queue;
  Work work;
  EXPECT_TRUE(queue.empty());
  queue.PushBack(&work);
  EXPECT_FALSE(queue.empty());
  queue.TakeAll();
  EXPECT_TRUE(queue.empty());
}

TEST(MpscQueueTest, CacheLine) {
  // Every producer and the consumer write the head, so nothing else may share
  // its line: not the next shard's inbox, nor the fields Post reads.
  EXPECT_EQ(alignof(MpscQueue), kCacheLineSize);
  EXPECT_EQ(sizeof(MpscQueue), kCacheLineSize);
}

TEST(MpscQueueTest, ManyProducers) {
  // Several threads post to one shard, which drains its inbox: nothing gets
  // lost or doubled, and each thread's posts arrive in the order it made them.
  constexpr int kProducers = 4;
  constexpr int kPerProducer = 250000;

  MpscQueue queue;
  std::vector<std::vector<Work>> works(kProducers,
                                       std::vector<Work>(kPerProducer));
  std::vector<int> next(kProducers, 0);
  int consumed = 0;

  std::vector<std::thread> producers;
  producers.reserve(kProducers);
  for (int p = 0; p < kProducers; p++) {
    producers.emplace_back([&queue, &works, p] {
      for (int seq = 0; seq < kPerProducer; seq++) {
        Work& work = works[p][seq];
        work.producer = p;
        work.seq = seq;
        queue.PushBack(&work);
      }
    });
  }

  // The consumer stops on the producers being done, not on what the queue
  // hands back, so a lost node fails the checks below instead of hanging.
  std::atomic<bool> produced{false};
  std::thread consumer([&] {
    const auto consume = [&](MpscNode* node) {
      const auto* work = static_cast<Work*>(node);
      EXPECT_EQ(work->seq, next[work->producer]++);
    };
    bool last = false;
    while (!last) {
      // Read before draining: once it is true, this drain sees every push.
      last = produced.load(std::memory_order_acquire);
      consumed += static_cast<int>(queue.ConsumeAll(consume));
    }
  });

  for (std::thread& producer : producers) {
    producer.join();
  }
  produced.store(true, std::memory_order_release);
  consumer.join();

  EXPECT_EQ(consumed, kProducers * kPerProducer);
  EXPECT_EQ(next, std::vector<int>(kProducers, kPerProducer));
  EXPECT_TRUE(queue.empty());
}

}  // namespace blockcache
}  // namespace dingofs
