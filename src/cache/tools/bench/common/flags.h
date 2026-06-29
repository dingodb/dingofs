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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_FLAGS_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_FLAGS_H_

#include <cstdint>
#include <string>
#include <vector>

namespace dingofs {
namespace cache {
namespace bench {

// A tiny, self-contained flag parser. Each subcommand builds its own FlagSet
// bound to its Options fields, so subcommands never share a global flag
// namespace (no gflags-style collisions across subcommands) and each gets its
// own filtered --help. Supports: --name=value, --name value, --flag / --noflag.
class FlagSet {
 public:
  void Str(const std::string& name, std::string* val, const std::string& help);
  void Switch(const std::string& name, bool* val, const std::string& help);
  void U32(const std::string& name, uint32_t* val, const std::string& help);
  void U64(const std::string& name, uint64_t* val, const std::string& help);
  void Real(const std::string& name, double* val, const std::string& help);
  // Parses sizes like 4KiB / 128MiB / 1g / 4194304 into bytes.
  void Size(const std::string& name, uint64_t* val, const std::string& help);

  // Parses argv[1..] into the bound fields. Returns false on error (*err set).
  // Sets HelpRequested() when --help/-h is seen (then stops, returns true).
  bool Parse(int argc, char** argv, std::string* err);
  bool HelpRequested() const { return help_; }

  void PrintHelp(const std::string& program, const std::string& usage,
                 const std::string& examples) const;

 private:
  enum class Type : uint8_t { kStr, kBool, kU32, kU64, kReal, kSize };
  struct Entry {
    std::string name;
    Type type;
    void* ptr;
    std::string help;
    std::string default_str;
  };

  const Entry* Find(const std::string& name) const;
  bool Assign(const Entry& e, const std::string& value, std::string* err);

  std::vector<Entry> entries_;
  bool help_{false};
};

bool ParseSize(const std::string& input, uint64_t* out);

}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_FLAGS_H_
