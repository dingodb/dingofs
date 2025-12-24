// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_SRC_CACHE_IUTIL_KETAMA_CON_HASH_H_
#define DINGOFS_SRC_CACHE_IUTIL_KETAMA_CON_HASH_H_

#include <cstdint>
#include <unordered_map>

#include "cache/iutil/con_hash.h"

namespace dingofs {
namespace cache {
namespace iutil {

class KetamaConHash : public ConHash {
 public:
  KetamaConHash() {
    nodes_.clear();
    continuum_.clear();
  }

  ~KetamaConHash() override = default;

  // ConnNode weight must be greater than 0 else will be core dump
  // ConnNode with same key will be replaced by the last one
  void InitWithNodes(const std::vector<ConNode>& nodes) override;

  // Make sure the node weight is greater than 0
  // and the node key is unique else will be core dump
  // If the node key already exists, first remove it then add the new one
  void AddNode(const std::string& key, uint32_t weight = 10) override;

  // Make sure the node weight is greater than 0
  // and the node key is unique else will be core dump
  void AddNode(const ConNode& node) override;

  bool RemoveNode(const std::string& key) override;

  bool Lookup(const std::string& key, ConNode& node) override;

  // Final before lookup
  // return true if the node exists
  // return false if the node not exists
  void Final() override;

  void Dump() override;

 private:
  void CreateContinuum();
  // Node key -> Node
  std::unordered_map<std::string, ConNode> nodes_;
  // Point -> Node key
  std::vector<std::pair<uint32_t, std::string>> continuum_;
};

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_IUTIL_KETAMA_CON_HASH_H_
