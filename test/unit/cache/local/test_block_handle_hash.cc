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
 * Created Date: 2026-07-23
 * Author: AI
 */

#include <gtest/gtest.h>

#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/block/tensor_key.h"

namespace dingofs {

// Equal keys must compare equal and hash equal (required by the cache index).
// Distinct keys must compare unequal (the index relies on operator== to
// disambiguate hash collisions, so this is the correctness-critical guarantee).

TEST(BlockHandleHashTest, BlockKeyEqualityAndHash) {
  BlockHandle a(1, BlockKey(10, 2, 4096));
  BlockHandle b(1, BlockKey(10, 2, 4096));
  BlockHandle c(1, BlockKey(10, 3, 4096));  // different index
  BlockHandle d(1, BlockKey(11, 2, 4096));  // different id

  EXPECT_TRUE(a == b);
  EXPECT_EQ(a.Hash(), b.Hash());
  EXPECT_FALSE(a == c);
  EXPECT_FALSE(a == d);
}

TEST(BlockHandleHashTest, FsIdIsNotPartOfBlockIdentity) {
  // Regression: the on-disk cache path has no fs_id, so the loader recovers
  // cache blocks with fs_id 0 while runtime reads use the real fs_id. The two
  // MUST hash-and-compare equal or the warm cache breaks after a restart.
  BlockHandle from_loader(0, BlockKey(10, 2, 4096));
  BlockHandle from_runtime(7, BlockKey(10, 2, 4096));
  EXPECT_TRUE(from_loader == from_runtime);
  EXPECT_EQ(from_loader.Hash(), from_runtime.Hash());
}

TEST(BlockHandleHashTest, SizeParticipatesInIdentity) {
  // Same (id,index) but a different size are distinct blocks/files today, so
  // they must not alias in the index.
  BlockHandle a(1, BlockKey(10, 2, 4096));
  BlockHandle b(1, BlockKey(10, 2, 8192));
  EXPECT_FALSE(a == b);
}

TEST(BlockHandleHashTest, TensorKeyEqualityAndHash) {
  BlockHandle a(TensorKey("llama", 8, 1, "abcd1234", "float16"));
  BlockHandle b(TensorKey("llama", 8, 1, "abcd1234", "float16"));
  BlockHandle c(TensorKey("llama", 8, 2, "abcd1234", "float16"));  // worker_id
  BlockHandle e(TensorKey("llama", 8, 1, "beef5678", "float16"));  // chunk_hash

  EXPECT_TRUE(a == b);
  EXPECT_EQ(a.Hash(), b.Hash());
  EXPECT_FALSE(a == c);
  EXPECT_FALSE(a == e);
}

TEST(BlockHandleHashTest, BlockAndTensorNeverEqual) {
  BlockHandle blk(1, BlockKey(10, 2, 4096));
  BlockHandle tsr(TensorKey("llama", 8, 1, "abcd1234", "float16"));
  EXPECT_FALSE(blk == tsr);
}

}  // namespace dingofs
