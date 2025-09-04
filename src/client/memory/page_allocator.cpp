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

#include "client/memory/page_allocator.h"

#include <fmt/format.h>

#include <cassert>
#include <cstring>

namespace dingofs {
namespace client {

static std::string kPageMetricPrefix = "dingofs_memory";

std::string PageAllocatorStat::ToString() const {
  return fmt::format("total_pages: {}, free_pages: {}, page_size: {}",
                     total_pages, free_pages, page_size);
}

DefaultPageAllocator::DefaultPageAllocator()
    : page_size_(0),
      num_free_pages_(0),
      metric_(std::make_unique<PageAllocatorMetric>(
          kPageMetricPrefix, false, [this] { return GetFreePages(); })) {}

bool DefaultPageAllocator::Init(uint64_t page_size, uint64_t num_pages) {
  total_pages_ = num_pages;
  num_free_pages_ = num_pages;
  page_size_ = page_size;
  metric_->total_bytes.set_value(num_pages * page_size_);
  return true;
}

char* DefaultPageAllocator::Allocate() {
  std::unique_lock<std::mutex> lk(mutex_);
  while (num_free_pages_ == 0) {
    can_allocate_.wait(lk);
  }

  char* page = new (std::nothrow) char[page_size_];
  std::memset(page, 0, page_size_);
  num_free_pages_--;
  metric_->used_bytes << page_size_;
  return page;
}

void DefaultPageAllocator::DeAllocate(char* page) {
  std::unique_lock<std::mutex> lk(mutex_);
  delete[] page;
  num_free_pages_++;
  metric_->used_bytes << -1 * page_size_;
  can_allocate_.notify_one();
}

uint64_t DefaultPageAllocator::GetFreePages() {
  std::unique_lock<std::mutex> lk(mutex_);
  return num_free_pages_;
}

PageAllocatorStat DefaultPageAllocator::GetStat() {
  std::unique_lock<std::mutex> lk(mutex_);
  return PageAllocatorStat{.total_pages = total_pages_,
                           .free_pages = num_free_pages_,
                           .page_size = page_size_};
}

PagePool::PagePool()
    : page_size_(0),
      num_free_pages_(0),
      mem_pool_(std::make_unique<MemoryPool>()),
      metric_(std::make_unique<PageAllocatorMetric>(
          kPageMetricPrefix, true, [this] { return GetFreePages(); })) {}

PagePool::~PagePool() { mem_pool_->DestroyPool(); }

bool PagePool::Init(uint64_t page_size, uint64_t num_pages) {
  total_pages_ = num_pages;
  num_free_pages_ = num_pages;
  page_size_ = page_size;
  metric_->total_bytes.set_value(num_pages * page_size_);
  return mem_pool_->CreatePool(page_size, num_pages);
}

char* PagePool::Allocate() {
  std::unique_lock<std::mutex> lk(mutex_);
  while (num_free_pages_ == 0) {
    can_allocate_.wait(lk);
  }

  void* page = mem_pool_->Allocate();
  assert(page != nullptr);
  num_free_pages_--;
  metric_->used_bytes << page_size_;
  return reinterpret_cast<char*>(page);
}

void PagePool::DeAllocate(char* page) {
  std::unique_lock<std::mutex> lk(mutex_);
  mem_pool_->DeAllocate(reinterpret_cast<void*>(page));
  num_free_pages_++;
  metric_->used_bytes << -1 * page_size_;
  can_allocate_.notify_one();
}

uint64_t PagePool::GetFreePages() {
  std::unique_lock<std::mutex> lk(mutex_);
  return num_free_pages_;
}

PageAllocatorStat PagePool::GetStat() {
  std::unique_lock<std::mutex> lk(mutex_);
  return PageAllocatorStat{.total_pages = total_pages_,
                           .free_pages = num_free_pages_,
                           .page_size = page_size_};
}

}  // namespace client
}  // namespace dingofs
