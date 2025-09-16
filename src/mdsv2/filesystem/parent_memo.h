// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDV2_FILESYSTEM_PARENT_MEMO_H_
#define DINGOFS_MDV2_FILESYSTEM_PARENT_MEMO_H_

#include <cstdint>
#include <unordered_map>

#include "json/value.h"
#include "mdsv2/common/type.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mdsv2 {

class ParentMemo;
using ParentMemoSPtr = std::shared_ptr<ParentMemo>;

class ParentMemo {
 public:
  ParentMemo(uint64_t fs_id);
  ~ParentMemo() = default;

  static ParentMemoSPtr New(uint64_t fs_id) { return std::make_shared<ParentMemo>(fs_id); }

  void Remeber(Ino ino, Ino parent);

  void Forget(Ino ino);

  bool GetParent(Ino ino, Ino& parent);

  void DescribeByJson(Json::Value& value);

 private:
  uint64_t fs_id_{0};

  utils::RWLock rwlock_;
  // ino -> parent
  std::unordered_map<Ino, Ino> parent_map_;

  // statistics
  bvar::Adder<int64_t> count_metrics_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_PARENT_MEMO_H_