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

#include "cache/utils/ketama_con_hash.h"

#include <fcntl.h>

#include <algorithm>
#include <cmath>
#include <cstdint>

#include "brpc/policy/hasher.h"
#include "glog/logging.h"

namespace dingofs {
namespace cache {

// 40 hashes, 4 numbers per hash = 160 points per node
const static uint32_t kHashNumPerNode = 40;
const static uint32_t kPointPerHash = 4;

void KetamaConHash::InitWithNodes(const std::vector<ConNode>& nodes) {
  nodes_.clear();
  nodes_.reserve(nodes.size());

  for (const auto& node : nodes) {
    CHECK_GT(node.weight, 0) << "node weight must be greater than 0";
    nodes_.emplace(node.key, node);
  }
}

void KetamaConHash::AddNode(const std::string& key, uint32_t weight) {
  ConNode node;
  node.key = key;
  node.weight = weight;

  AddNode(node);
}

void KetamaConHash::AddNode(const ConNode& node) {
  CHECK_GT(node.weight, 0) << "node weight must be greater than 0";
  CHECK(nodes_.emplace(node.key, node).second)
      << "node already exists, key: " << node.key;
}

bool KetamaConHash::RemoveNode(const std::string& key) {
  auto iter = nodes_.find(key);
  if (iter == nodes_.end()) {
    return false;
  }

  nodes_.erase(iter);
  return true;
}

static void Md5Digest(const std::string& key, unsigned char* digest) {
  brpc::policy::MD5HashSignature(key.data(), key.length(), digest);
}

bool KetamaConHash::Lookup(const std::string& key, ConNode& node) {
  if (continuum_.empty()) {
    return false;
  }

  unsigned char digest[16];
  Md5Digest(key, digest);

  uint32_t search_point =
      (digest[3] << 24) | (digest[2] << 16) | (digest[1] << 8) | digest[0];

  // find the first point that is greater than or equal to the search_point
  auto it = std::lower_bound(continuum_.begin(), continuum_.end(),
                             std::make_pair(search_point, ""),
                             [](const std::pair<uint32_t, std::string>& a,
                                const std::pair<uint32_t, std::string>& b) {
                               return a.first < b.first;
                             });

  // if the search_point is greater than all points, use the first point
  if (it == continuum_.end()) {
    it = continuum_.begin();
  }

  node = nodes_[it->second];
  return true;
}

void KetamaConHash::Final() { CreateContinuum(); }

void KetamaConHash::Dump() {
  LOG(INFO) << "node count: " << nodes_.size();

  for (const auto& key_node : nodes_) {
    LOG(INFO) << "node: " << key_node.first
              << ", weight: " << key_node.second.weight;
  }

  LOG(INFO) << "continuum point count: " << continuum_.size();
  for (const auto& point_node : continuum_) {
    LOG(INFO) << "point: " << point_node.first
              << ", node: " << point_node.second;
  }
}

// the core algorithm of ketama consistent hash from
// https://github.com/RJ/ketama/blob/master/libketama/ketama.c
void KetamaConHash::CreateContinuum() {
  continuum_.clear();

  int total_node_num = nodes_.size();

  uint32_t total_weight = 0;
  for (const auto& key_node : nodes_) {
    total_weight += key_node.second.weight;
  }

  for (const auto& key_node : nodes_) {
    const ConNode& node = key_node.second;

    float pct = (float)node.weight / total_weight;
    int hash_num = floorf(pct * total_node_num * 40.0);
    VLOG(9) << "node: " << key_node.first << ", weight: " << node.weight
            << ", pct: " << pct << ", hash_num: " << hash_num
            << ", total_weight: " << total_weight
            << ", total_node_num: " << total_node_num
            << ", kHashNumPerNode: " << kHashNumPerNode
            << ", kPointPerHash: " << kPointPerHash;

    for (int i = 0; i < hash_num; i++) {
      unsigned char digest[16];

      std::string hash_key = node.key + "-" + std::to_string(i);

      Md5Digest(hash_key, digest);
      /* Use successive 4-bytes from hash as numbers
       * for the points on the circle: */
      int h;
      for (h = 0; h < 4; h++) {
        uint32_t point = (digest[3 + h * 4] << 24) | (digest[2 + h * 4] << 16) |
                         (digest[1 + h * 4] << 8) | digest[h * 4];

        continuum_.emplace_back(std::make_pair(point, key_node.first));
      }
    }
  }

  // sort the continuum by point
  std::sort(continuum_.begin(), continuum_.end(),
            [](const std::pair<uint32_t, std::string>& a,
               const std::pair<uint32_t, std::string>& b) {
              return a.first < b.first;
            });

  VLOG(9) << "create continuum with " << continuum_.size() << " points for "
          << total_node_num << " nodes";
}

}  // namespace cache
}  // namespace dingofs
