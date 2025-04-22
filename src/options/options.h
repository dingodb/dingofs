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

  std::unordered_map<std::string, BaseOption*> childs_;
  std::unordered_map<std::string, IItem*> items_;
};

namespace internal {

bool pass_bool(const char*, bool);
bool pass_int32(const char*, int32_t);
bool pass_uint32(const char*, uint32_t);
bool pass_int64(const char*, int64_t);
bool pass_uint64(const char*, uint64_t);
bool pass_double(const char*, double);
bool pass_string(const char*, std::string);

};  // namespace internal

}  // namespace options
}  // namespace dingofs

// macros

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

// bind base
#define BIND_base(T, name, default_value, comment)                            \
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

// bind gflags
#define BIND_ONFLY_base(T, name, gflag_name)                                  \
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

// bind_*
#define BIND_bool(name, default_value, comment) \
  BIND_base(bool, name, default_value, comment)

#define BIND_int32(name, default_value, comment) \
  BIND_base(int32_t, name, default_value, comment)

#define BIND_uint32(name, default_value, comment) \
  BIND_base(uint32_t, name, default_value, comment)

#define BIND_int64(name, default_value, comment) \
  BIND_base(int64_t, name, default_value, comment)

#define BIND_uint64(name, default_value, comment) \
  BIND_base(uint64_t, name, default_value, comment)

#define BIND_double(name, default_value, comment) \
  BIND_base(double, name, default_value, comment)

#define BIND_string(name, default_value, comment) \
  BIND_base(std::string, name, default_value, comment)

#define BIND_string_array(name, default_value, comment) \
  BIND_base(std::vector<std::string>, name, default_value, comment)

#define BIND_uint32_array(name, default_value, comment) \
  BIND_base(std::vector<uint32_t>, name, default_value, comment)

// declare_onfly_*
#define DECLARE_ONFLY_bool(name) DECLARE_bool(name);
#define DECLARE_ONFLY_int32(name) DECLARE_int32(name);
#define DECLARE_ONFLY_int64(name) DECLARE_int64(name);
#define DECLARE_ONFLY_uint32(name) DECLARE_uint32(name);
#define DECLARE_ONFLY_uint64(name) DECLARE_uint64(name);
#define DECLARE_ONFLY_double(name) DECLARE_double(name);
#define DECLARE_ONFLY_string(name) DECLARE_string(name);

// declare_onfly_*
#define DEFINE_ONFLY_bool(name, default_value, comment) \
  DEFINE_bool(name, default_value, comment);            \
  DEFINE_validator(name, &internal::pass_bool);

#define DEFINE_ONFLY_int32(name, default_value, comment) \
  DEFINE_int32(name, default_value, comment);            \
  DEFINE_validator(name, &internal::pass_int32);

#define DEFINE_ONFLY_uint32(name, default_value, comment) \
  DEFINE_uint32(name, default_value, comment);            \
  DEFINE_validator(name, &internal::pass_uint32);

#define DEFINE_ONFLY_int64(name, default_value, comment) \
  DEFINE_int64(name, default_value, comment);            \
  DEFINE_validator(name, &internal::pass_int64);

#define DEFINE_ONFLY_uint64(name, default_value, comment) \
  DEFINE_uint64(name, default_value, comment);            \
  DEFINE_validator(name, &internal::pass_uint64);

#define DEFINE_ONFLY_double(name, default_value, comment) \
  DEFINE_double(name, default_value, comment);            \
  DEFINE_validator(name, &internal::pass_double);

#define DEFINE_ONFLY_string(name, default_value, comment) \
  DEFINE_string(name, default_value, comment);            \
  DEFINE_validator(name, &internal::pass_string);

// bind_onfly_*
#define BIND_ONFLY_bool(name, gflag_name) \
  BIND_ONFLY_base(bool, name, gflag_name)

#define BIND_ONFLY_int32(name, gflag_name) \
  BIND_ONFLY_base(int32_t, name, gflag_name)

#define BIND_ONFLY_uint32(name, gflag_name) \
  BIND_ONFLY_base(uint32_t, name, gflag_name)

#define BIND_ONFLY_int64(name, gflag_name) \
  BIND_ONFLY_base(int64_t, name, gflag_name)

#define BIND_ONFLY_uint64(name, gflag_name) \
  BIND_ONFLY_base(uint64_t, name, gflag_name)

#define BIND_ONFLY_double(name, gflag_name) \
  BIND_ONFLY_base(double, name, gflag_name)

#define BIND_ONFLY_string(name, gflag_name) \
  BIND_ONFLY_base(std::string, name, gflag_name)

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
