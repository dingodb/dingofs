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

#include "common/readmempool/read_buf.h"

#include "common/readmempool/read_mem_pool.h"

namespace dingofs {

// Defined here (not inline in the header) because returning the slot needs the
// complete ReadMemPool type. ReleaseExternal frees by address reverse-lookup --
// the same path the IOBuf deleter (ReadMemPool::Deleter) takes -- so an owning
// ReadBuf and a handed-off slot are released identically.
void ReadBuf::Reset() {
  if (pool_ != nullptr) {
    pool_->ReleaseExternal(data_);
    pool_ = nullptr;
    data_ = nullptr;
  }
}

}  // namespace dingofs
