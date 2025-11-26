/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_SRC_UTILS_DOUBLY_MAP_H_
#define DINGOFS_SRC_UTILS_DOUBLY_MAP_H_

#include <functional>
#include <utility>
#include <vector>

#include "butil/containers/doubly_buffered_data.h"
#include "butil/containers/flat_map.h"
#include "glog/logging.h"

namespace dingofs {
namespace utils {

#define RESP_OK 1

template <typename M>
class DoublyMap {
 public:
  using MapType = M;
  using KeyType = typename M::key_type;
  using ValueType = typename M::mapped_type;
  using DBDMapType = butil::DoublyBufferedData<MapType>;
  using TypeScopedPtr = typename DBDMapType::ScopedPtr;
  using IterFn =
      std::function<void(const KeyType& key, const ValueType& value)>;
  using FilterFn = std::function<bool(const KeyType&, const ValueType&)>;

  DoublyMap() = default;
  ~DoublyMap() = default;

  bool Init(int64_t) { return true; }

  bool Resize(int64_t) { return true; }

  size_t Size() {
    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return 0;

    return ptr->size();
  }

  ValueType Get(const KeyType& key) {
    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return {};

    auto it = ptr->find(key);
    if (it == ptr->end()) return {};

    return it->second;
  }

  std::vector<ValueType> Get(const std::vector<KeyType>& keys) {
    std::vector<ValueType> values;

    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return values;

    for (const auto& key : keys) {
      auto it = ptr->find(key);
      if (it != ptr->end()) {
        values.emplace_back(it->second);
      }
    }

    return values;
  }

  std::vector<ValueType> GetAll() {
    std::vector<ValueType> values;

    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return values;

    for (auto it = ptr->begin(); it != ptr->end(); ++it) {
      values.emplace_back(it->second);
    }

    return values;
  }

  ValueType Filter(FilterFn&& fn) {
    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return {};

    for (auto it = ptr->begin(); it != ptr->end(); ++it) {
      if (fn(it->first, it->second)) {
        return it->second;
      }
    }

    return {};
  }

  std::vector<ValueType> Filters(FilterFn&& fn) {
    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return {};

    std::vector<ValueType> results;
    for (auto it = ptr->begin(); it != ptr->end(); ++it) {
      if (fn(it->first, it->second)) {
        results.emplace_back(it->second);
      }
    }

    return results;
  }

  void Iterate(IterFn&& func) {
    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return;

    for (auto it = ptr->begin(); it != ptr->end(); ++it) {
      func(it->first, it->second);
    }
  }

  bool IsExist(const KeyType& key) {
    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return false;

    auto* v_ptr = ptr->seek(key);
    if (v_ptr == nullptr) return false;

    return true;
  }

  bool Put(const KeyType& key, const ValueType& value) {
    if (map_.Modify(
            [](MapType& m, const KeyType& k, const ValueType& v) {
              m[k] = v;
              return RESP_OK;
            },
            key, value) > 0) {
      return true;
    }

    return false;
  }

  bool MultiPut(const std::vector<KeyType>& key_list,
                const std::vector<ValueType>& value_list) {
    if (map_.Modify(
            [](MapType& m, const std::vector<KeyType>& keys,
               const std::vector<ValueType>& values) {
              CHECK(!keys.empty()) << "keys is empty";
              CHECK_EQ(keys.size(), values.size())
                  << "keys size not equal values size";

              for (size_t i = 0; i < keys.size(); ++i) {
                m[keys[i]] = values[i];
              }

              return keys.size();
            },
            key_list, value_list) > 0) {
      return true;
    }

    return false;
  }

  // erase multi keys
  int MultiErase(const std::vector<KeyType>& keys) {
    if (map_.Modify(
            [](MapType& m, const std::vector<KeyType>& keys) {
              CHECK(!keys.empty()) << "keys is empty";

              int erase_count = 0;
              for (const auto& key : keys) {
                erase_count += m.erase(key);
              }

              return erase_count;
            },
            keys) > 0) {
      return true;
    }

    return false;
  }

  // put key-value pair into map if key exists
  bool PutIfExist(const KeyType& key, const ValueType& value) {
    if (map_.Modify(
            [](MapType& m, const KeyType& k, const ValueType& v) {
              auto it = m.find(k);
              if (it == m.end()) return 0;

              it->second = v;
              return RESP_OK;
            },
            key, value) > 0) {
      return true;
    }

    return false;
  }

  // put key-value pair into map if key not exists
  bool PutIfAbsent(const KeyType& key, const ValueType& value) {
    if (map_.Modify(
            [](MapType& m, const KeyType& k, const ValueType& v) {
              auto it = m.find(k);
              if (it != m.end()) return 0;

              m[k] = v;

              return RESP_OK;
            },
            key, value) > 0) {
      return true;
    }

    return false;
  }

  // put key-value pair into map if key exists and value not equals
  bool PutIfNotEqual(const KeyType& key, const ValueType& value) {
    if (map_.Modify(
            [](MapType& m, const KeyType& k, const ValueType& v) {
              auto it = m.find(k);
              if (it == m.end()) return 0;
              if (it->second == v) return 0;

              it->second = v;

              return RESP_OK;
            },
            key, value) > 0) {
      return true;
    }

    return false;
  }

  // erase key-value pair from map
  bool Erase(const KeyType& key) {
    if (map_.Modify(
            [](MapType& m, const KeyType& k) {
              m.erase(k);
              return RESP_OK;
            },
            key) > 0) {
      return true;
    }

    return false;
  }

  // erase all key-value pairs from map
  bool Clear() {
    if (map_.Modify([](MapType& m) {
          m.clear();
          return 1;
        }) > 0) {
      return true;
    }

    return false;
  }

  ValueType operator[](KeyType& key) const { return Get(key); }

 private:
  DBDMapType map_;
};

template <typename K, typename V>
class DoublyMap<butil::FlatMap<K, V>> {
 public:
  using MapType = butil::FlatMap<K, V>;
  using KeyType = typename MapType::key_type;
  using ValueType = typename MapType::mapped_type;
  using DBDMapType = butil::DoublyBufferedData<MapType>;
  using TypeScopedPtr = typename DBDMapType::ScopedPtr;
  using IterFn =
      std::function<void(const KeyType& key, const ValueType& value)>;
  using FilterFn = std::function<bool(const KeyType&, const ValueType&)>;

  DoublyMap() = default;
  ~DoublyMap() { Clear(); }

  bool Init(int64_t capacity) {
    if (map_.Modify(
            [](MapType& m, const int64_t& capacity) {
              CHECK_EQ(0, m.init(capacity));
              return RESP_OK;
            },
            capacity) > 0) {
      return true;
    }

    return false;
  }

  bool Resize(int64_t capacity) {
    if (map_.Modify(
            [](MapType& m, const int64_t& capacity) {
              CHECK_EQ(0, m.resize(capacity));
              return RESP_OK;
            },
            capacity) > 0) {
      return true;
    }

    return false;
  }

  size_t Size() {
    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return 0;

    return ptr->size();
  }

  ValueType Get(const KeyType& key) {
    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return {};

    auto* v_ptr = ptr->seek(key);
    if (v_ptr == nullptr) return {};

    return *v_ptr;
  }

  std::vector<ValueType> Get(const std::vector<KeyType>& keys) {
    std::vector<ValueType> values;

    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return values;

    for (const auto& key : keys) {
      auto* v_ptr = ptr->seek(key);
      if (v_ptr != nullptr) {
        values.emplace_back(*v_ptr);
      }
    }

    return values;
  }

  std::vector<ValueType> GetAll() {
    std::vector<ValueType> values;

    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return values;

    for (auto it = ptr->begin(); it != ptr->end(); ++it) {
      values.emplace_back(it->second);
    }

    return values;
  }

  ValueType Filter(FilterFn&& fn) {
    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return {};

    for (auto it = ptr->begin(); it != ptr->end(); ++it) {
      if (fn(it->first, it->second)) {
        return it->second;
      }
    }

    return {};
  }

  std::vector<ValueType> Filters(FilterFn&& fn) {
    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return {};

    std::vector<ValueType> results;
    for (auto it = ptr->begin(); it != ptr->end(); ++it) {
      if (fn(it->first, it->second)) {
        results.emplace_back(it->second);
      }
    }

    return results;
  }

  void Iterate(IterFn&& func) {
    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return;

    for (auto it = ptr->begin(); it != ptr->end(); ++it) {
      func(it->first, it->second);
    }
  }

  bool IsExist(const KeyType& key) {
    TypeScopedPtr ptr;
    if (map_.Read(&ptr) != 0) return false;

    auto* v_ptr = ptr->seek(key);
    if (v_ptr == nullptr) return false;

    return true;
  }

  bool Put(const KeyType& key, const ValueType& value) {
    if (map_.Modify(
            [](MapType& m, const KeyType& k, const ValueType& v) {
              m.insert(k, v);
              return RESP_OK;
            },
            key, value) > 0) {
      return true;
    }

    return false;
  }

  bool MultiPut(const std::vector<KeyType>& key_list,
                const std::vector<ValueType>& value_list) {
    if (map_.Modify(
            [](MapType& m, const std::vector<KeyType>& keys,
               const std::vector<ValueType>& values) {
              CHECK(!keys.empty()) << "keys is empty";
              CHECK_EQ(keys.size(), values.size())
                  << "keys size not equal values size";

              for (size_t i = 0; i < keys.size(); ++i) {
                m.insert(keys[i], values[i]);
              }

              return keys.size();
            },
            key_list, value_list) > 0) {
      return true;
    }

    return false;
  }

  // erase multi keys
  int MultiErase(const std::vector<KeyType>& keys) {
    if (map_.Modify(
            [](MapType& m, const std::vector<KeyType>& keys) {
              CHECK(!keys.empty()) << "keys is empty";

              int erase_count = 0;
              for (const auto& key : keys) {
                erase_count += m.erase(key);
              }

              return erase_count;
            },
            keys) > 0) {
      return true;
    }

    return false;
  }

  // put key-value pair into map if key exists
  bool PutIfExist(const KeyType& key, const ValueType& value) {
    if (map_.Modify(
            [](MapType& m, const KeyType& k, const ValueType& v) {
              auto* v_ptr = m.seek(k);
              if (v_ptr == nullptr) return 0;

              *v_ptr = v;
              return RESP_OK;
            },
            key, value) > 0) {
      return true;
    }

    return false;
  }

  // put key-value pair into map if key not exists
  bool PutIfAbsent(const KeyType& key, const ValueType& value) {
    if (map_.Modify(
            [](MapType& m, const KeyType& k, const ValueType& v) {
              auto* v_ptr = m.seek(k);
              if (v_ptr != nullptr) return 0;

              m.insert(k, v);

              return RESP_OK;
            },
            key, value) > 0) {
      return true;
    }

    return false;
  }

  // put key-value pair into map if key exists and value not equals
  bool PutIfNotEqual(const KeyType& key, const ValueType& value) {
    if (map_.Modify(
            [](MapType& m, const KeyType& k, const ValueType& v) {
              auto* v_ptr = m.seek(k);
              if (v_ptr == nullptr) return 0;
              if (*v_ptr == v) return 0;

              *v_ptr = v;

              return RESP_OK;
            },
            key, value) > 0) {
      return true;
    }

    return false;
  }

  // erase key-value pair from map
  bool Erase(const KeyType& key) {
    if (map_.Modify(
            [](MapType& m, const KeyType& k) {
              m.erase(k);
              return RESP_OK;
            },
            key) > 0) {
      return true;
    }

    return false;
  }

  // erase all key-value pairs from map
  bool Clear() {
    if (map_.Modify([](MapType& m) {
          m.clear();
          return RESP_OK;
        }) > 0) {
      return true;
    }

    return false;
  }

  ValueType operator[](KeyType& key) const { return Get(key); }

 private:
  DBDMapType map_;
};

}  // namespace utils
}  // namespace dingofs

#endif  // DINGOFS_SRC_UTILS_DOUBLY_MAP_H_