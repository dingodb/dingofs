/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "connector_pybind_utils.h"
#include "native_cache_engine.h"

namespace py = pybind11;

PYBIND11_MODULE(_native, m) {
  m.doc() =
      "DingoFS-backed IStorageConnector for LMCache. "
      "Talks to dingo-cache servers via brpc, releases the GIL on every "
      "submission, and exposes the standard LMCache native connector ABI.";

  py::class_<dingofs::lmcache::NativeCacheEngine>(m, "NativeCacheEngine")
      .def(py::init<const std::vector<std::string>&, const std::string&,
                    uint32_t, int, uint32_t>(),
           py::arg("mds_addrs"), py::arg("group_name"), py::arg("fs_id"),
           py::arg("num_workers") = 16,
           py::arg("request_timeout_ms") = 5000)
      .def_property_readonly(
          "service_version",
          &dingofs::lmcache::NativeCacheEngine::service_version)
          LMCACHE_BIND_CONNECTOR_METHODS(dingofs::lmcache::NativeCacheEngine);
}
