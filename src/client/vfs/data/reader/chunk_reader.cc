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

#include "client/vfs/data/reader/chunk_reader.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "client/vfs/common/helper.h"
#include "client/vfs/data/reader/chunk_req.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "common/trace/trace_manager.h"

namespace dingofs {
namespace client {
namespace vfs {

namespace {

std::string SlicesToString(const std::vector<Slice>& slices) {
  std::ostringstream oss;
  oss << "[";
  for (size_t i = 0; i < slices.size(); ++i) {
    oss << Slice2Str(slices[i]);
    if (i < slices.size() - 1) {
      oss << ", ";
    }
  }
  oss << "]";
  return oss.str();
}

};  // namespace

ChunkReader::ChunkReader(VFSHub* hub, uint64_t fh, const ChunkReq& req)
    : hub_(hub), fh_(fh), reader_(new ChunkReqReader(hub, req)) {}

Status ChunkReader::GetSlices(ContextSPtr ctx, ChunkSlices* chunk_slices) {
  auto span = hub_->GetTraceManager().StartChildSpan("ChunkReader::GetSlices",
                                                     ctx->GetTraceSpan());

  std::vector<Slice> slices;
  uint64_t chunk_version = 0;
  DINGOFS_RETURN_NOT_OK(hub_->GetMetaSystem().ReadSlice(
      SpanScope::GetContext(span), reader_->chunk_.ino, reader_->chunk_.index,
      fh_, &slices, chunk_version));

  chunk_slices->version = chunk_version;
  chunk_slices->slices = std::move(slices);
  VLOG(9) << fmt::format("{} GetSlices, version: {}, slice_num: {}, slices: {}",
                         UUID(), chunk_slices->version,
                         chunk_slices->slices.size(),
                         SlicesToString(chunk_slices->slices));

  return Status::OK();
}

void ChunkReader::ReadAsync(ContextSPtr ctx, StatusCallback cb) {
  auto span = hub_->GetTraceManager().StartChildSpan("ChunkReader::ReadAsync",
                                                     ctx->GetTraceSpan());

  ChunkSlices chunk_slices;
  Status s = GetSlices(SpanScope::GetContext(span), &chunk_slices);
  if (!s.ok()) {
    LOG(WARNING) << fmt::format("{} Failed GetSlices, status: {}", UUID(),
                                s.ToString());
    cb(s);
    return;
  }

  reader_->ReadAsync(SpanScope::GetContext(span), chunk_slices.slices,
                     std::move(cb));
}

IOBuffer ChunkReader::GetDataBuffer() const { return reader_->GetDataBuffer(); }

}  // namespace vfs

}  // namespace client

}  // namespace dingofs
