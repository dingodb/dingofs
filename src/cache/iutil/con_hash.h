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

#ifndef DINGOFS_SRC_CACHE_IUTIL_CON_HASH_H_
#define DINGOFS_SRC_CACHE_IUTIL_CON_HASH_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace dingofs {
namespace cache {
namespace iutil {

struct ConNode {
  std::string key;
  uint32_t weight{10};
};

// Consistent hash interface
// Call the Final method after all nodes are added
class ConHash {
 public:
  ConHash() = default;

  virtual ~ConHash() = default;

  // Clean all nodes then init with new nodes, node can be empty
  // It's client duty to ensure the weight of ConnNode is greater than 0
  // and the key of ConnNode is unique
  virtual void InitWithNodes(const std::vector<ConNode>& nodes) = 0;

  // It's client duty to ensure the weight of ConnNode is greater than 0
  // and the key of ConnNode is unique
  // If the node key already exists, first remove it then add the new one
  virtual void AddNode(const std::string& key, uint32_t weight) = 0;

  // It's client duty to ensure the weight of ConnNode is greater than 0
  // and the key of ConnNode is unique
  // If the node key already exists, first remove it then add the new one
  virtual void AddNode(const ConNode& node) = 0;

  // Remove node by key
  // return true if the node exists
  // return false if the node not exists
  virtual bool RemoveNode(const std::string& key) = 0;

  // Final before lookup
  // return true if the node exists
  // return false if the node not exists
  virtual bool Lookup(const std::string& key, ConNode& node) = 0;

  // Generate consistent hash ring
  virtual void Final() = 0;

  virtual void Dump() = 0;
};

using ConHashUPtr = std::unique_ptr<ConHash>;

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_IUTIL_CON_HASH_H_
