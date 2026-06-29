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

#include "cache/tools/bench/common/flags.h"

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>

#include "cache/tools/bench/common/format.h"

namespace dingofs {
namespace cache {
namespace bench {

namespace {
const char* TypeName(int t) {
  switch (t) {
    case 0: return "string";
    case 1: return "bool";
    case 2: return "uint32";
    case 3: return "uint64";
    case 4: return "double";
    case 5: return "size";
    default: return "?";
  }
}
}  // namespace

bool ParseSize(const std::string& input, uint64_t* out) {
  std::string text = input;
  // trim
  auto sp = [](unsigned char c) { return std::isspace(c) != 0; };
  while (!text.empty() && sp(text.front())) text.erase(text.begin());
  while (!text.empty() && sp(text.back())) text.pop_back();
  if (text.empty()) return false;

  size_t pos = 0;
  while (pos < text.size() && (std::isdigit(static_cast<unsigned char>(text[pos])) != 0)) {
    ++pos;
  }
  if (pos == 0) return false;

  uint64_t number = 0;
  try {
    number = std::stoull(text.substr(0, pos));
  } catch (const std::exception&) {
    return false;
  }

  std::string suffix = text.substr(pos);
  std::transform(suffix.begin(), suffix.end(), suffix.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  uint64_t mul = 1;
  if (suffix.empty() || suffix == "b") {
    mul = 1;
  } else if (suffix == "k" || suffix == "kb" || suffix == "kib") {
    mul = 1024;
  } else if (suffix == "m" || suffix == "mb" || suffix == "mib") {
    mul = 1024ULL * 1024;
  } else if (suffix == "g" || suffix == "gb" || suffix == "gib") {
    mul = 1024ULL * 1024 * 1024;
  } else {
    return false;
  }
  if (number > std::numeric_limits<uint64_t>::max() / mul) return false;
  *out = number * mul;
  return true;
}

void FlagSet::Str(const std::string& name, std::string* val,
                  const std::string& help) {
  entries_.push_back({name, Type::kStr, val, help, *val});
}
void FlagSet::Switch(const std::string& name, bool* val,
                     const std::string& help) {
  entries_.push_back({name, Type::kBool, val, help, *val ? "true" : "false"});
}
void FlagSet::U32(const std::string& name, uint32_t* val,
                  const std::string& help) {
  entries_.push_back({name, Type::kU32, val, help, std::to_string(*val)});
}
void FlagSet::U64(const std::string& name, uint64_t* val,
                  const std::string& help) {
  entries_.push_back({name, Type::kU64, val, help, std::to_string(*val)});
}
void FlagSet::Real(const std::string& name, double* val,
                   const std::string& help) {
  std::ostringstream os;
  os << *val;
  entries_.push_back({name, Type::kReal, val, help, os.str()});
}
void FlagSet::Size(const std::string& name, uint64_t* val,
                   const std::string& help) {
  entries_.push_back({name, Type::kSize, val, help, FormatBytes(*val)});
}

const FlagSet::Entry* FlagSet::Find(const std::string& name) const {
  for (const auto& e : entries_) {
    if (e.name == name) return &e;
  }
  return nullptr;
}

bool FlagSet::Assign(const Entry& e, const std::string& value,
                     std::string* err) {
  try {
    switch (e.type) {
      case Type::kStr:
        *static_cast<std::string*>(e.ptr) = value;
        break;
      case Type::kBool:
        *static_cast<bool*>(e.ptr) =
            (value == "true" || value == "1" || value == "yes" || value.empty());
        break;
      case Type::kU32:
        *static_cast<uint32_t*>(e.ptr) =
            static_cast<uint32_t>(std::stoul(value));
        break;
      case Type::kU64:
        *static_cast<uint64_t*>(e.ptr) = std::stoull(value);
        break;
      case Type::kReal:
        *static_cast<double*>(e.ptr) = std::stod(value);
        break;
      case Type::kSize: {
        uint64_t bytes = 0;
        if (!ParseSize(value, &bytes)) {
          *err = "invalid size for --" + e.name + ": " + value;
          return false;
        }
        *static_cast<uint64_t*>(e.ptr) = bytes;
        break;
      }
    }
  } catch (const std::exception&) {
    *err = "invalid value for --" + e.name + ": " + value;
    return false;
  }
  return true;
}

bool FlagSet::Parse(int argc, char** argv, std::string* err) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h" || arg == "help") {
      help_ = true;
      return true;
    }
    if (arg.rfind("--", 0) != 0) {
      *err = "unexpected argument: " + arg;
      return false;
    }
    arg = arg.substr(2);

    std::string name = arg;
    std::string value;
    bool has_value = false;
    auto eq = arg.find('=');
    if (eq != std::string::npos) {
      name = arg.substr(0, eq);
      value = arg.substr(eq + 1);
      has_value = true;
    }

    // --noXxx turns a bool off.
    if (!has_value && name.rfind("no", 0) == 0) {
      const Entry* e = Find(name.substr(2));
      if (e != nullptr && e->type == Type::kBool) {
        *static_cast<bool*>(e->ptr) = false;
        continue;
      }
    }

    const Entry* e = Find(name);
    if (e == nullptr) {
      *err = "unknown flag: --" + name;
      return false;
    }
    if (!has_value) {
      if (e->type == Type::kBool) {
        *static_cast<bool*>(e->ptr) = true;
        continue;
      }
      if (i + 1 >= argc) {
        *err = "flag --" + name + " needs a value";
        return false;
      }
      value = argv[++i];
    }
    if (!Assign(*e, value, err)) {
      return false;
    }
  }
  return true;
}

void FlagSet::PrintHelp(const std::string& program, const std::string& usage,
                        const std::string& examples) const {
  std::vector<Entry> sorted = entries_;
  std::sort(sorted.begin(), sorted.end(),
            [](const Entry& a, const Entry& b) { return a.name < b.name; });
  size_t width = 0;
  for (const auto& e : sorted) {
    width = std::max(width, e.name.size() + std::string(TypeName(static_cast<int>(e.type))).size());
  }

  std::cout << program << "\n\n";
  std::cout << "Usage:\n  " << usage << "\n\n";
  if (!examples.empty()) {
    std::cout << "Examples:\n" << examples;
    if (examples.back() != '\n') std::cout << '\n';
    std::cout << '\n';
  }
  std::cout << "Options:\n";
  for (const auto& e : sorted) {
    std::ostringstream key;
    key << "--" << e.name << "=" << TypeName(static_cast<int>(e.type));
    std::cout << "  " << std::left << std::setw(static_cast<int>(width) + 5)
              << key.str() << e.help << " (default: " << e.default_str << ")\n";
  }
  std::cout << "  " << std::left << std::setw(static_cast<int>(width) + 5)
            << "--help" << "show this help\n";
}

}  // namespace bench
}  // namespace cache
}  // namespace dingofs
