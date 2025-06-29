
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

#ifndef DINGOFS_OPTIONS_CLIENT_MEMORY_PAGE_OPTION_H_
#define DINGOFS_OPTIONS_CLIENT_MEMORY_PAGE_OPTION_H_

#include "base/math/math.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {

using ::dingofs::base::math::kMiB;

struct PageOption {
  uint64_t page_size;
  uint64_t total_size;
  bool use_pool;
};

static void InitMemoryPageOption(utils::Configuration* c, PageOption* option) {
  // page option
  c->GetValueFatalIfFail("data_stream.page.size", &option->page_size);
  c->GetValueFatalIfFail("data_stream.page.total_size_mb", &option->total_size);
  c->GetValueFatalIfFail("data_stream.page.use_pool", &option->use_pool);

  if (option->page_size == 0) {
    CHECK(false) << "Page size must greater than 0.";
  }

  option->total_size = option->total_size * kMiB;
  if (option->total_size < 64 * kMiB) {
    CHECK(false) << "Page total size must greater than 64MiB.";
  }
}

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_OPTIONS_CLIENT_MEMORY_PAGE_OPTION_H_