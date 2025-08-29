
/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_METRICS_CLIENT_MEMORY_PAGE_ALLOCATOR_METRIC_H_
#define DINGOFS_METRICS_CLIENT_MEMORY_PAGE_ALLOCATOR_METRIC_H_

#include <bvar/bvar.h>
#include <bvar/passive_status.h>
#include <bvar/reducer.h>
#include <bvar/status.h>

#include <cstdint>
#include <functional>
#include <string>

namespace dingofs {
namespace client {

static uint64_t FreePagesWrapper(void* arg);

struct PageAllocatorMetric {
  bvar::Status<bool> use_page_pool;
  bvar::Status<uint64_t> total_bytes;
  bvar::Adder<uint64_t> used_bytes;
  bvar::PassiveStatus<uint64_t> free_pages;
  std::function<uint64_t()> free_pages_func;

  PageAllocatorMetric(const std::string& p_prefix, bool p_use_pool,
                      std::function<uint64_t()> p_free_pages_func)
      : use_page_pool(p_prefix, "use_page_pool", p_use_pool),
        total_bytes(p_prefix, "total_bytes", 0),
        used_bytes(p_prefix, "used_bytes"),
        free_pages(p_prefix, "free_pages", &FreePagesWrapper, this),
        free_pages_func(std::move(p_free_pages_func)) {}
};

static uint64_t FreePagesWrapper(void* arg) {
  auto* metric = static_cast<PageAllocatorMetric*>(arg);
  return metric->free_pages_func();
}

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_METRICS_CLIENT_MEMORY_PAGE_ALLOCATOR_METRIC_H_
