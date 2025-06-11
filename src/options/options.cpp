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
/*
 * Project: DingoFS
 * Created Date: 2025-05-06
 * Author: Jingli Chen (Wine93)
 */

#include "options/options.h"

#include <iostream>

namespace dingofs {
namespace options {

bool BaseOption::Parse(const std::string& filepath) {
  const auto input = toml::try_parse(filepath);
  if (input.is_ok()) {
    const auto& root = input.unwrap();
    return Walk(root);
  }

  for (const auto& err : input.unwrap_err()) {
    std::cerr << "parse toml file failed: " << err << std::endl;
  }

  return false;
}

bool BaseOption::Walk(const toml::value& node) {
  if (!node.is_table()) {
    return true;
  }

  for (const auto& [key, value] : node.as_table()) {
    bool succ =
        value.is_table() ? HandleTable(key, value) : HandleNormal(key, value);
    if (!succ) {
      std::cerr << "handle kv pair failed: key = " << key << std::endl;
      return false;
    }
  }

  bool succ = true;
  if (rewrite_func_) {
    succ = rewrite_func_(this);
    if (succ && validate_func_) {
      succ = validate_func_(this);
    }
  }
  return succ;
}

bool BaseOption::HandleTable(const std::string& key, const toml::value& value) {
  auto iter = childs_.find(key);
  if (iter == childs_.end()) {
    std::cerr << "unknown section: " << key << std::endl;
    return false;
  }

  const auto& child = iter->second;
  return child->Walk(value);
}

bool BaseOption::HandleNormal(const std::string& key,
                              const toml::value& value) {
  auto iter = items_.find(key);
  if (iter == items_.end()) {
    std::cerr << "unknown option: " << key << std::endl;
    return false;
  }

  auto& item = iter->second;
  return item->SetValue(value);
}

}  // namespace options
}  // namespace dingofs
