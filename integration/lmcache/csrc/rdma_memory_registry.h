// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef DINGOFS_INTEGRATION_LMCACHE_RDMA_MEMORY_REGISTRY_H_
#define DINGOFS_INTEGRATION_LMCACHE_RDMA_MEMORY_REGISTRY_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cache/infiniband/infiniband.h"

namespace dingofs {
namespace integration {
namespace lmcache {

struct MemoryRegion;

struct RegisteredMemoryRegion {
  std::uintptr_t addr;
  std::size_t length;
  std::uint32_t lkey;
  std::uint32_t rkey;
};

class RdmaMemoryRegistry {
 public:
  static std::unique_ptr<RdmaMemoryRegistry> Create(
      const std::vector<MemoryRegion>& regions);

  ~RdmaMemoryRegistry() = default;

  RdmaMemoryRegistry(const RdmaMemoryRegistry&) = delete;
  RdmaMemoryRegistry& operator=(const RdmaMemoryRegistry&) = delete;

  const std::vector<RegisteredMemoryRegion>& registered_regions() const {
    return registered_regions_;
  }

  const std::string& device_name() const { return device_name_; }

  bool Covers(const void* data, std::size_t length) const;

  // Returns the rkey of the registered region fully covering [p, p+n), or 0
  // when no region covers it. 0 signals "not RDMA-registered" to callers, who
  // then fall back to the cache client's staging pool.
  std::uint32_t RkeyFor(const void* p, std::size_t n) const;

 private:
  RdmaMemoryRegistry() = default;

  void RegisterRegions(const std::vector<MemoryRegion>& regions);

  std::string device_name_;
  std::vector<RegisteredMemoryRegion> registered_regions_;
  std::vector<cache::infiniband::MemoryRegionUPtr> owned_regions_;
};

}  // namespace lmcache
}  // namespace integration
}  // namespace dingofs

#endif  // DINGOFS_INTEGRATION_LMCACHE_RDMA_MEMORY_REGISTRY_H_
