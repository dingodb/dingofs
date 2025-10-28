/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-17
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_MEMORY_PAGE_ALLOCATOR_H_
#define DINGOFS_SRC_CLIENT_MEMORY_PAGE_ALLOCATOR_H_

#include <sys/types.h>

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "client/memory/memory_pool.h"
#include "common/metrics/client/memory/page_allocator_metric.h"

namespace dingofs {
namespace client {

struct PageAllocatorStat {
  uint64_t total_pages;
  uint64_t free_pages;
  uint64_t page_size;

  std::string ToString() const;
};

class PageAllocator {
 public:
  virtual ~PageAllocator() = default;

  virtual bool Init(uint64_t page_size, uint64_t num_pages) = 0;

  virtual char* Allocate() = 0;

  virtual void DeAllocate(char* page) = 0;

  virtual uint64_t GetFreePages() = 0;

  virtual PageAllocatorStat GetStat() = 0;
};

class DefaultPageAllocator : public PageAllocator {
 public:
  DefaultPageAllocator();

  ~DefaultPageAllocator() override = default;

  bool Init(uint64_t page_size, uint64_t num_pages) override;

  char* Allocate() override;

  void DeAllocate(char* page) override;

  uint64_t GetFreePages() override;

  PageAllocatorStat GetStat() override;

 private:
  std::unique_ptr<PageAllocatorMetric> metric_;

  std::mutex mutex_;
  std::condition_variable can_allocate_;
  uint64_t total_pages_;
  uint64_t num_free_pages_;
  uint64_t page_size_;
};

class PagePool : public PageAllocator {
 public:
  PagePool();

  ~PagePool() override;

  bool Init(uint64_t page_size, uint64_t num_pages) override;

  char* Allocate() override;

  void DeAllocate(char* p) override;

  uint64_t GetFreePages() override;

  PageAllocatorStat GetStat() override;

 private:
  std::unique_ptr<PageAllocatorMetric> metric_;
  // TODO: use multi-slots or thread local to reduce mutex overhead
  std::mutex mutex_;
  std::condition_variable can_allocate_;
  uint64_t total_pages_;
  uint64_t num_free_pages_;
  uint64_t page_size_;
  std::unique_ptr<MemoryPool> mem_pool_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_MEMORY_PAGE_ALLOCATOR_H_
