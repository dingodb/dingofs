// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include <gflags/gflags.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/block/tensor_key.h"
#include "completion_queue.h"
#include "native_engine.h"

namespace dingofs {
namespace integration {
namespace lmcache {

namespace py = pybind11;

namespace {

// Native key wire format: "{model_name}@{kv_rank_hex}@{chunk_hash_hex}".
//
// This matches lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter
// ._object_key_to_string verbatim, so DingoFSL2Adapter can inherit the
// upstream NativeConnectorL2Adapter unchanged. DingoFSConnector emits the
// same format on the RemoteConnector path. dtype is fixed at "lmcache" —
// chunk_hash is already a content hash, so dtype carries no information we
// need to disambiguate keys.
//
// Layout of kv_rank (upstream contract):
//   (world_size << 24) | (worker_id << 16) | (local_ws << 8) | (local_rank)
// We only need world_size / worker_id for dingofs path generation.
constexpr const char* kFixedDtype = "lmcache";

TensorKey ToTensorKey(const py::handle& obj) {
  std::string s;
  try {
    s = py::cast<std::string>(obj);
  } catch (...) {
    throw py::type_error("key must be a 'model@kv_rank_hex@hash_hex' string");
  }
  size_t a = s.find('@');
  if (a == std::string::npos) {
    throw py::value_error("key missing first '@' delimiter: " + s);
  }
  size_t b = s.find('@', a + 1);
  if (b == std::string::npos) {
    throw py::value_error("key missing second '@' delimiter: " + s);
  }

  std::string model = s.substr(0, a);
  std::string kv_rank_hex = s.substr(a + 1, b - a - 1);
  std::string chunk_hash = s.substr(b + 1);
  if (chunk_hash.size() <= 4) {
    // dingofs TensorKey shards on chunk_hash[0:2] and [0:4]; enforce minimum.
    throw py::value_error(
        "chunk_hash must be > 4 hex chars (got '" + chunk_hash + "')");
  }

  uint32_t kv_rank = 0;
  try {
    kv_rank = static_cast<uint32_t>(std::stoul(kv_rank_hex, nullptr, 16));
  } catch (...) {
    throw py::value_error("invalid kv_rank hex '" + kv_rank_hex + "'");
  }
  uint32_t world_size = (kv_rank >> 24) & 0xff;
  uint32_t worker_id = (kv_rank >> 16) & 0xff;

  return TensorKey{std::move(model), world_size, worker_id,
                   std::move(chunk_hash), kFixedDtype};
}

py::list CompletionsToPy(std::vector<Completion> in) {
  py::list out;
  for (auto& c : in) {
    py::object per_key;
    if (c.per_key.empty()) {
      per_key = py::none();
    } else {
      py::list lst(c.per_key.size());
      for (size_t i = 0; i < c.per_key.size(); ++i) {
        lst[i] = py::bool_(c.per_key[i] != 0);
      }
      per_key = std::move(lst);
    }
    out.append(py::make_tuple(c.future_id, c.ok, c.error, per_key));
  }
  return out;
}

uint64_t SubmitBatchSet(NativeEngine& self, const py::sequence& keys,
                        const py::sequence& bufs) {
  size_t n = keys.size();
  if (n != bufs.size()) {
    throw py::value_error("keys and bufs must have the same length");
  }
  std::vector<SetItem> items;
  items.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    py::buffer buf = py::reinterpret_borrow<py::buffer>(bufs[i]);
    py::buffer_info info = buf.request(/*writable=*/false);
    items.push_back(SetItem{
        ToTensorKey(keys[i]),
        info.ptr,
        static_cast<size_t>(info.size) * info.itemsize,
    });
  }
  py::gil_scoped_release release;
  return self.SubmitBatchSet(std::move(items));
}

uint64_t SubmitBatchGet(NativeEngine& self, const py::sequence& keys,
                        const py::sequence& bufs) {
  size_t n = keys.size();
  if (n != bufs.size()) {
    throw py::value_error("keys and bufs must have the same length");
  }
  std::vector<GetItem> items;
  items.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    py::buffer buf = py::reinterpret_borrow<py::buffer>(bufs[i]);
    py::buffer_info info = buf.request(/*writable=*/true);
    items.push_back(GetItem{
        ToTensorKey(keys[i]),
        info.ptr,
        static_cast<size_t>(info.size) * info.itemsize,
    });
  }
  py::gil_scoped_release release;
  return self.SubmitBatchGet(std::move(items));
}

uint64_t SubmitBatchExists(NativeEngine& self, const py::sequence& keys) {
  std::vector<TensorKey> tkeys;
  tkeys.reserve(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    tkeys.push_back(ToTensorKey(keys[i]));
  }
  py::gil_scoped_release release;
  return self.SubmitBatchExists(std::move(tkeys));
}

}  // namespace

PYBIND11_MODULE(_dingofs_native, m) {
  m.doc() = "DingoFS native bridge for the LMCache connector.";

  // Read-only gflag accessor. Used by tests to assert that a conf file's
  // flags actually landed in process-global gflags storage; also handy for
  // diagnostics. Returns the flag's current value as a string. Raises
  // ValueError for unknown flag names.
  m.def(
      "get_flag",
      [](const std::string& name) {
        std::string out;
        if (!::gflags::GetCommandLineOption(name.c_str(), &out)) {
          throw py::value_error("unknown gflag: " + name);
        }
        return out;
      },
      py::arg("name"));

  using PyMemoryRegion = std::pair<std::uintptr_t, std::size_t>;

  py::class_<NativeEngine>(m, "RemoteCache")
      .def(py::init([](std::string mds_addrs, std::string cache_group,
                       std::string conf_file,
                       const std::vector<PyMemoryRegion>& rdma_pools) {
             NativeEngine::InitOptions opts;
             opts.mds_addrs = std::move(mds_addrs);
             opts.cache_group = std::move(cache_group);
             opts.conf_file = std::move(conf_file);
             opts.rdma_pools.reserve(rdma_pools.size());
             for (const auto& [addr, length] : rdma_pools) {
               opts.rdma_pools.push_back(MemoryRegion{addr, length});
             }
             return NativeEngine::Create(opts);
           }),
           py::arg("mds_addrs"), py::arg("cache_group"),
           py::arg("conf_file") = std::string{},
           py::arg("rdma_pools") = std::vector<PyMemoryRegion>{},
           R"doc(
Create the singleton RemoteCache native engine.

Args:
    mds_addrs:  comma-separated MDS endpoints, e.g. "10.0.0.1:6700,10.0.0.2:6700"
    cache_group: cache group name configured on the dingo-cache cluster
    conf_file:  optional path to a gflags-format conf file
                (one ``--flag=value`` per line, ``#`` comments allowed).
                Same format dingo-mds / dingo-client / cache-bench accept.
    rdma_pools: optional list of (addr, length) tuples covering CPU memory
                regions that should later be registered with the RDMA NIC
                (ibv_reg_mr). Typically the LMCache LocalCPUBackend's pinned
                arena. Stored for future RDMA bring-up; no registration
                happens at construction time.

Only one instance per process (gflags are process-global).
)doc")
      .def(
          "rdma_pools",
          [](const NativeEngine& self) {
            std::vector<PyMemoryRegion> out;
            out.reserve(self.rdma_pools().size());
            for (const auto& r : self.rdma_pools()) {
              out.emplace_back(r.addr, r.length);
            }
            return out;
          },
          "Return the CPU memory regions stashed for RDMA registration as a "
          "list of (addr, length) tuples. Read-only.")
      .def("event_fd", &NativeEngine::event_fd,
           "Native eventfd; signaled when at least one completion is ready.")
      .def(
          "exists_sync",
          [](NativeEngine& self, const py::handle& key) {
            TensorKey k = ToTensorKey(key);
            std::string err;
            bool ok;
            {
              py::gil_scoped_release release;
              ok = self.ExistsSync(k, &err);
            }
            if (!err.empty()) throw std::runtime_error(err);
            return ok;
          },
          py::arg("key"),
          "Synchronous existence probe. NotFound returns False; transport "
          "errors raise.")
      .def(
          "ping",
          [](NativeEngine& self) {
            std::string err;
            bool ok;
            {
              py::gil_scoped_release release;
              ok = self.Ping(&err);
            }
            if (!ok) {
              throw std::runtime_error(err.empty() ? "ping failed" : err);
            }
          },
          "Health probe; raises on failure.")
      .def("submit_batch_set", &SubmitBatchSet, py::arg("keys"),
           py::arg("bufs"))
      .def("submit_batch_get", &SubmitBatchGet, py::arg("keys"),
           py::arg("bufs"))
      .def("submit_batch_exists", &SubmitBatchExists, py::arg("keys"))
      .def(
          "drain_completions",
          [](NativeEngine& self) {
            std::vector<Completion> out;
            {
              py::gil_scoped_release release;
              out = self.Drain();
            }
            return CompletionsToPy(std::move(out));
          },
          R"doc(
Drain queued completions.

Returns a list of (future_id, ok, error_msg, per_key) tuples where per_key is
either a list[bool] (one entry per submitted key) or None for empty batches.
)doc")
      .def("close", &NativeEngine::Shutdown,
           "Shut down the engine and wait for in-flight bthreads to drain.");
}

}  // namespace lmcache
}  // namespace integration
}  // namespace dingofs
