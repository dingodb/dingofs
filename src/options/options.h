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

#ifndef DINGOFS_SRC_OPTIONS_OPTIONS_H_
#define DINGOFS_SRC_OPTIONS_OPTIONS_H_

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include <toml.hpp>
#include <type_traits>

namespace dingofs {
namespace options {

// interface for option item
class IItem {
 public:
  virtual ~IItem() = default;

  virtual bool SetValue(const toml::value& value) = 0;
};

template <class T>
class Item : public IItem {
 public:
  Item(const std::string& name, T* value, const std::string& comment)
      : name_(name), value_(value), comment_(comment) {}

  bool SetValue(const toml::value& value) override {
    if constexpr (std::is_same_v<T, bool>) {
      return SetBoolean(value);
    } else if constexpr (std::is_integral_v<T>) {
      return SetInteger(value);
    } else if constexpr (std::is_floating_point_v<T>) {
      return SetFloating(value);
    } else if constexpr (std::is_same_v<T, std::string>) {
      return SetString(value);
    } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
      return SetArray(value);
    } else if constexpr (std::is_same_v<T, std::vector<uint32_t>>) {
      return SetArray(value);
    }
    return false;
  }

 private:
  bool SetBoolean(const toml::value& value) {
    if (value.is_boolean()) {
      *value_ = toml::get<bool>(value);
      return true;
    }
    return false;
  }

  bool SetInteger(const toml::value& value) {
    if (value.is_integer()) {
      *value_ = toml::get<T>(value);
      return true;
    }
    return false;
  }

  bool SetInt(const toml::value& value) {
    if (value.is_integer()) {
      *value_ = toml::get<T>(value);
      return true;
    }
    return false;
  }

  bool SetFloating(const toml::value& value) {
    if (value.is_floating()) {
      *value_ = toml::get<T>(value);
      return true;
    }
    return false;
  }

  bool SetString(const toml::value& value) {
    if (value.is_string()) {
      *value_ = toml::get<T>(value);
      return true;
    }
    return false;
  }

  bool SetArray(const toml::value& value) {
    if (value.is_array()) {
      *value_ = toml::get<T>(value);
      return true;
    }
    return false;
  }

  std::string name_;
  T* value_;
  std::string comment_;
};

class BaseOption {
 public:
  BaseOption() = default;

  bool Parse(const std::string& filepath);

 protected:
  bool Walk(const toml::value& node);

  bool HandleTable(const std::string& key, const toml::value& value);

  bool HandleNormal(const std::string& key, const toml::value& value);

  std::function<bool(BaseOption*)> rewrite_func_{nullptr};
  std::function<bool(BaseOption*)> validate_func_{nullptr};
  std::unordered_map<std::string, BaseOption*> childs_;
  std::unordered_map<std::string, IItem*> items_;
};

}  // namespace options
}  // namespace dingofs

// macros

// bind base
#define BIND_any(T, name, default_value, comment)                             \
 public:                                                                      \
  T& name() { return name##_; }                                               \
  const T& name() const { return name##_; }                                   \
  void set_##name(T value) { name##Item_.SetValue(value); }                   \
                                                                              \
 private:                                                                     \
  T name##_{default_value};                                                   \
  Item<T> name##Item_ = Item<T>(#name, &name##_, comment);                    \
  [[maybe_unused]] bool name##Insert_ = [this]() {                            \
    BaseOption::items_[#name] = reinterpret_cast<IItem*>(&this->name##Item_); \
    return true;                                                              \
  }()

// bind anonymous base
#define BIND_ANON_any(T, name, default_value, comment) \
 public:                                               \
  T& name() { return name##_; }                        \
  const T& name() const { return name##_; }            \
  void set_##name(T value) { name##_ = value; }        \
                                                       \
 private:                                              \
  T name##_{default_value};

// bind gflags
#define BIND_FLAG_any(T, name, gflag_name)                                    \
 public:                                                                      \
  T& name() { return FLAGS_##gflag_name; }                                    \
  const T& name() const { return FLAGS_##gflag_name; }                        \
  void set_##name(T value) { FLAGS_##gflag_name = value; }                    \
                                                                              \
 private:                                                                     \
  Item<T> name##Item_ = Item<T>(#name, &FLAGS_##gflag_name, "");              \
  [[maybe_unused]] bool name##Insert_ = [this]() {                            \
    BaseOption::items_[#name] = reinterpret_cast<IItem*>(&this->name##Item_); \
    return true;                                                              \
  }()

// bind suboption
#define BIND_suboption(name, child_name, cls)          \
 public:                                               \
  cls& name() { return name##_; }                      \
  const cls& name() const { return name##_; }          \
                                                       \
 private:                                              \
  cls name##_;                                         \
  [[maybe_unused]] bool name##Insert_ = [this]() {     \
    BaseOption::childs_[child_name] =                  \
        reinterpret_cast<BaseOption*>(&this->name##_); \
    return true;                                       \
  }()

// { REWRITE, VALIDATE }_BY
#define REWRITE_BY(rewrite_func)              \
 private:                                     \
  [[maybe_unused]] bool Rewrite_ = [this]() { \
    BaseOption::rewrite_func_ = rewrite_func; \
    return true;                              \
  }()

#define VALIDATE_BY(validate_func)              \
 private:                                       \
  [[maybe_unused]] bool Validate_ = [this]() {  \
    BaseOption::validate_func_ = validate_func; \
    return true;                                \
  }()

// BIND_*: bind configure item to member
#define BIND_bool(name, default_value, comment) \
  BIND_any(bool, name, default_value, comment)

#define BIND_int32(name, default_value, comment) \
  BIND_any(int32_t, name, default_value, comment)

#define BIND_uint32(name, default_value, comment) \
  BIND_any(uint32_t, name, default_value, comment)

#define BIND_int64(name, default_value, comment) \
  BIND_any(int64_t, name, default_value, comment)

#define BIND_uint64(name, default_value, comment) \
  BIND_any(uint64_t, name, default_value, comment)

#define BIND_double(name, default_value, comment) \
  BIND_any(double, name, default_value, comment)

#define BIND_string(name, default_value, comment) \
  BIND_any(std::string, name, default_value, comment)

#define BIND_string_array(name, default_value, comment) \
  BIND_any(std::vector<std::string>, name, default_value, comment)

#define BIND_uint32_array(name, default_value, comment) \
  BIND_any(std::vector<uint32_t>, name, default_value, comment)

// BIND_ANON_*: bind anonymous to member (means bind nothing, declare member
// only)
#define BIND_ANON_bool(name, default_value, comment) \
  BIND_ANON_any(bool, name, default_value, comment)

#define BIND_ANON_int32(name, default_value, comment) \
  BIND_ANON_any(int32_t, name, default_value, comment)

#define BIND_ANON_uint32(name, default_value, comment) \
  BIND_ANON_any(uint32_t, name, default_value, comment)

#define BIND_ANON_int64(name, default_value, comment) \
  BIND_ANON_any(int64_t, name, default_value, comment)

#define BIND_ANON_uint64(name, default_value, comment) \
  BIND_ANON_any(uint64_t, name, default_value, comment)

#define BIND_ANON_double(name, default_value, comment) \
  BIND_ANON_any(double, name, default_value, comment)

#define BIND_ANON_string(name, default_value, comment) \
  BIND_ANON_any(std::string, name, default_value, comment)

// BIND_FLAG_*: bind gflag to member
#define BIND_FLAG_bool(name, gflag_name) BIND_FLAG_any(bool, name, gflag_name)

#define BIND_FLAG_int32(name, gflag_name) \
  BIND_FLAG_any(int32_t, name, gflag_name)

#define BIND_FLAG_uint32(name, gflag_name) \
  BIND_FLAG_any(uint32_t, name, gflag_name)

#define BIND_FLAG_int64(name, gflag_name) \
  BIND_FLAG_any(int64_t, name, gflag_name)

#define BIND_FLAG_uint64(name, gflag_name) \
  BIND_FLAG_any(uint64_t, name, gflag_name)

#define BIND_FLAG_double(name, gflag_name) \
  BIND_FLAG_any(double, name, gflag_name)

#define BIND_FLAG_string(name, gflag_name) \
  BIND_FLAG_any(std::string, name, gflag_name)

// utils
#define STR_ARRAY std::vector<std::string>
#define UINT32_ARRAY std::vector<uint32_t>

// /* We always want to import declared variables, dll or no */ \
// namespace fL##shorttype { extern GFLAGS_DLL_DECLARE_FLAG type FLAGS_##name; } \
// using fL##shorttype::FLAGS_##name

// options
#define DECLARE_OPTION(name, type) \
  namespace __g_options__ {        \
  extern type OPTIONS_##name;      \
  }                                \
  using __g_options__::OPTIONS_##name

#define DEFINE_OPTION(name, type) \
  namespace __g_options__ {       \
  type OPTIONS_##name;            \
  }                               \
  using __g_options__::OPTIONS_##name

#endif  // DINGOFS_SRC_OPTIONS_OPTIONS_H_
