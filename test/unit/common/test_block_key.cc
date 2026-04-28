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

#include "common/block/tensor_key.h"

#include <gtest/gtest.h>

namespace dingofs {

TEST(TensorKeyTest, FilenameSanitizesPathAndFieldSeparators) {
  TensorKey key{"meta-llama/Llama-3@8B", 2, 1, "abcd/ef@01", "bf16/raw"};

  EXPECT_EQ(key.Filename(), "meta-llama_Llama-3_8B@2@1@abcd_ef_01@bf16_raw");
  EXPECT_EQ(key.StoreKey(),
            "tensor/ab/abcd/meta-llama_Llama-3_8B@2@1@abcd_ef_01@bf16_raw");
}

}  // namespace dingofs
