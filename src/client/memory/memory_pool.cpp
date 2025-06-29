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

#include "client/memory/memory_pool.h"

#include <butil/logging.h>
#include <butil/time.h>
#include <glog/logging.h>
#include <stdlib.h>
#include <sys/mman.h>

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>

namespace dingofs {
namespace client {

using Timer = ::butil::Timer;

MemoryPool::MemoryPool()
    : size_each_block_(0),
      num_total_blocks_(0),
      num_free_blocks_(0),
      num_initialized_blocks_(0),
      mem_start_(nullptr),
      next_free_index_(nullptr) {}

bool MemoryPool::CreatePool(size_t size_each_block, uint64_t num_total_blocks) {
  Timer timer;
  uint64_t total_size = size_each_block * num_total_blocks;
  void** memptr = reinterpret_cast<void**>(&mem_start_);

  timer.start();
  int rc = posix_memalign(memptr, 1 << 21, total_size);
  if (rc == 0) {
    void* addr = reinterpret_cast<void*>(mem_start_);
    rc = madvise(addr, total_size, MADV_HUGEPAGE);
  }
  timer.stop();

  if (rc != 0) {
    LOG(ERROR) << "Alloc huge page memory failed: rc = " << rc;
    return false;
  }

  size_each_block_ = size_each_block;
  num_total_blocks_ = num_total_blocks;
  num_free_blocks_ = num_total_blocks_;
  next_free_index_ = mem_start_;

  AllocateAllBlocksOnce();

  LOG(INFO) << "Memory pool init success: preallocate " << num_total_blocks
            << " blocks with " << size_each_block << " bytes each block"
            << ", costs " << timer.u_elapsed() / 1e6 << " seconds.";
  return true;
}

void MemoryPool::DestroyPool() {
  free(mem_start_);
  mem_start_ = nullptr;
}

void* MemoryPool::Allocate() {
  InitOneBlock();

  void* block = nullptr;
  if (num_free_blocks_ > 0) {
    block = reinterpret_cast<void*>(next_free_index_);
    --num_free_blocks_;
    RemoveFront();
  }
  return block;
}

void MemoryPool::DeAllocate(void* block) {
  AddFront(block);
  num_free_blocks_++;
};

uint64_t MemoryPool::GetFreeBlocks() const { return num_free_blocks_; }

void MemoryPool::AllocateAllBlocksOnce() {
  std::vector<void*> blocks;
  size_t num_total_blocks = num_total_blocks_;

  // Allocate once.
  for (uint64_t i = 0; i < num_total_blocks; i++) {
    void* block = Allocate();
    volatile char* mem_start = reinterpret_cast<char*>(block);
    std::fill(mem_start, mem_start + size_each_block_, 0);
    blocks.emplace_back(block);
  }

  // Then deallocate.
  for (uint64_t i = 0; i < num_total_blocks; i++) {
    DeAllocate(blocks[i]);
  }
}

void MemoryPool::InitOneBlock() {
  if (num_initialized_blocks_ < num_total_blocks_) {
    uint64_t* next_index =
        reinterpret_cast<uint64_t*>(AddrFromIndex(num_initialized_blocks_));
    *next_index = num_initialized_blocks_ + 1;
    num_initialized_blocks_++;
  }
}

void MemoryPool::AddFront(void* block) {
  uint64_t next_index;
  if (next_free_index_ != nullptr) {
    next_index = IndexFromAddr(next_free_index_);
  } else {
    next_index = num_total_blocks_;
  }

  *reinterpret_cast<uint64_t*>(block) = next_index;
  next_free_index_ = reinterpret_cast<char*>(block);
}

void MemoryPool::RemoveFront() {
  if (num_free_blocks_ != 0) {
    uint64_t index = *reinterpret_cast<uint64_t*>(next_free_index_);
    next_free_index_ = AddrFromIndex(index);
  } else {
    next_free_index_ = nullptr;
  }
}

char* MemoryPool::AddrFromIndex(uint64_t index) const {
  return mem_start_ + (index * size_each_block_);
}

uint64_t MemoryPool::IndexFromAddr(const char* addr) const {
  return static_cast<uint64_t>(addr - mem_start_) / size_each_block_;
}

}  // namespace client
}  // namespace dingofs
