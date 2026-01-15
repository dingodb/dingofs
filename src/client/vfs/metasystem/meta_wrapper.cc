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

#include "client/vfs/metasystem/meta_wrapper.h"

#include <absl/strings/str_format.h>

#include <cstdint>

#include "client/vfs/metasystem/local/metasystem.h"
#include "client/vfs/metasystem/mds/metasystem.h"
#include "client/vfs/metasystem/memory/metasystem.h"
#include "client/vfs/metasystem/meta_log.h"
#include "client/vfs/metasystem/meta_system.h"
#include "client/vfs/vfs_meta.h"
#include "common/const.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

static MetaSystemUPtr BuildMetaSystem(const VFSConfig& vfs_conf,
                                      ClientId& client_id,
                                      TraceManager& trace_manager,
                                      Compactor& compactor) {
  if (vfs_conf.metasystem_type == MetaSystemType::MEMORY) {
    return std::make_unique<memory::MemoryMetaSystem>();

  } else if (vfs_conf.metasystem_type == MetaSystemType::LOCAL) {
    return std::make_unique<local::LocalMetaSystem>(
        dingofs::Helper::ExpandPath(kDefaultMetaDBDir), vfs_conf.fs_name,
        vfs_conf.storage_info);

  } else if (vfs_conf.metasystem_type == MetaSystemType::MDS) {
    return meta::MDSMetaSystem::Build(vfs_conf.fs_name, vfs_conf.mds_addrs,
                                      client_id, trace_manager, compactor);
  }

  return nullptr;
}

MetaWrapper::MetaWrapper(const VFSConfig& vfs_conf, ClientId& client_id,
                         TraceManager& trace_manager, Compactor& compactor)
    : target_(BuildMetaSystem(vfs_conf, client_id, trace_manager, compactor)),
      slice_metric_(std::make_unique<metrics::client::SliceMetric>()) {}

Status MetaWrapper::Flush(ContextSPtr ctx, Ino ino, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("flush (%d): %s [fh:%d]", ino, s.ToString(), fh);
  });

  s = target_->Flush(ctx, ino, fh);
  return s;
}

Status MetaWrapper::NewSliceId(ContextSPtr ctx, Ino ino, uint64_t* id) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("new_slice_id: (%d) %d, %s", ino, *id, s.ToString());
  });
  metrics::client::SliceMetricGuard guard(&s, &slice_metric_->new_sliceid,
                                          butil::cpuwide_time_us());

  s = target_->NewSliceId(ctx, ino, id);
  return s;
}

Status MetaWrapper::ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                              uint64_t fh, std::vector<Slice>* slices,
                              uint64_t& version) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("read_slice (%d,%d): %s [fh:%d]  %d %lu", ino, index,
                           s.ToString(), fh, slices->size(), version);
  });
  metrics::client::SliceMetricGuard guard(&s, &slice_metric_->read_slice,
                                          butil::cpuwide_time_us());

  s = target_->ReadSlice(ctx, ino, index, fh, slices, version);
  return s;
}

Status MetaWrapper::WriteSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                               uint64_t fh, const std::vector<Slice>& slices) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("write_slice (%d,%d,%d): %s %d", ino, index, fh,
                           s.ToString(), slices.size());
  });
  metrics::client::SliceMetricGuard guard(&s, &slice_metric_->write_slice,
                                          butil::cpuwide_time_us());

  s = target_->WriteSlice(ctx, ino, index, fh, slices);
  return s;
}

Status MetaWrapper::AsyncWriteSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                                    uint64_t fh,
                                    const std::vector<Slice>& slices,
                                    DoneClosure done) {
  uint64_t start_us = butil::cpuwide_time_us();
  uint64_t slice_count = slices.size();

  auto wrapped_done = [this, start_us, slice_count, ctx = std::move(ctx), ino,
                       index, fh,
                       done = std::move(done)](const Status& status) {
    MetaLogGuard log_guard(start_us, [&]() {
      return absl::StrFormat("async_write_slice (%d,%d,%d): %s %d", ino, index,
                             fh, status.ToString(), slice_count);
    });

    metrics::client::SliceMetricGuard guard(
        &status, &slice_metric_->write_slice, start_us);

    done(status);
  };

  return target_->AsyncWriteSlice(ctx, ino, index, fh, slices,
                                  std::move(wrapped_done));
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
