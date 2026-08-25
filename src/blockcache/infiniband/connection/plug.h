/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_PLUG_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_PLUG_H_

namespace dingofs {
namespace blockcache {
namespace infiniband {

struct PlugList;

class Pluggable {
 public:
  virtual ~Pluggable() = default;

  Pluggable() = default;

  Pluggable(const Pluggable&) = delete;
  Pluggable& operator=(const Pluggable&) = delete;
  Pluggable(Pluggable&&) noexcept {}
  Pluggable& operator=(Pluggable&&) noexcept { return *this; }

  void Plug();
  virtual void Unplug() = 0;

 private:
  friend struct PlugList;

  Pluggable* next_ = nullptr;
  bool plugged_ = false;
};

struct PlugList {
  Pluggable* head = nullptr;

  void UnplugAll();
};

constinit inline thread_local PlugList tls_plugs;
inline PlugList& ThisPlugs() { return tls_plugs; }

inline void Pluggable::Plug() {
  if (plugged_) {
    return;
  }

  PlugList& plugs = ThisPlugs();
  plugged_ = true;
  next_ = plugs.head;
  plugs.head = this;
}

inline void PlugList::UnplugAll() {
  Pluggable* plug = head;
  head = nullptr;
  while (plug != nullptr) {
    Pluggable* next = plug->next_;
    plug->next_ = nullptr;
    plug->plugged_ = false;
    plug->Unplug();
    plug = next;
  }
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_PLUG_H_
