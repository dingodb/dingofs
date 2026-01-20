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

#include "client/vfs/compaction/compactor.h"

#include <glog/logging.h>

#include <cstdint>

#include "client/vfs/common/basync_util.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/compaction/compact_utils.h"
#include "client/vfs/data/reader/chunk_req_reader.h"
#include "client/vfs/data/slice/common.h"
#include "client/vfs/data/slice/slice_writer.h"
#include "client/vfs/hub/vfs_hub.h"

namespace dingofs {
namespace client {
namespace vfs {
using namespace compaction;

Status Compactor::DoCompact(ContextSPtr ctx, Ino ino, uint64_t chunk_index,
                            const std::vector<Slice>& slices,
                            Slice& out_slice) {
  CHECK(!slices.empty()) << "invalid compact, no slices to compact";
  auto span = vfs_hub_->GetTraceManager().StartChildSpan("Compactor::DoCompact",
                                                         ctx->GetTraceSpan());
  auto fs_info = vfs_hub_->GetFsInfo();

  int64_t chunk_size = fs_info.chunk_size;

  FileRange file_range = GetSlicesFileRange(slices);
  int64_t offset_in_chunk = file_range.offset % chunk_size;

  ChunkReq req(ino, chunk_index, offset_in_chunk, file_range);

  VLOG(9) << "Start comaction for req: " << req.ToString();

  std::string to_write;
  {
    // read data
    ChunkReqReader reader(vfs_hub_, req);

    Status s;
    BSynchronizer sync;
    reader.ReadAsync(SpanScope::GetContext(span), slices,
                     sync.AsStatusCallBack(s));
    sync.Wait();

    if (!s.ok()) {
      LOG(WARNING) << "Faile compaction because read failed: " << s.ToString()
                   << ", req: " << req.ToString();
      return s;
    }

    IOBuffer data = reader.GetDataBuffer();
    uint64_t data_size = data.Size();
    CHECK_EQ(data_size, static_cast<uint64_t>(file_range.len));

    to_write.resize(data.Size());
    data.CopyTo(to_write.data());
  }

  Slice compacted;

  {
    auto page_size = vfs_hub_->GetWriteBufferManager()->GetPageSize();

    SliceDataContext ctx(fs_info.id, ino, chunk_index, chunk_size,
                         fs_info.block_size, page_size);

    SliceWriter writer(ctx, vfs_hub_, offset_in_chunk);
    Status ret = writer.Write(SpanScope::GetContext(span), to_write.data(),
                              to_write.size(), offset_in_chunk);
    CHECK(ret.ok()) << "Compaction write slice failed: " << ret.ToString()
                    << ", ino: " << ino << ", chunk_index: " << chunk_index;

    Status s;
    BSynchronizer sync;
    writer.FlushAsync(sync.AsStatusCallBack(s));
    sync.Wait();
    if (!s.ok()) {
      LOG(WARNING) << "Fail compaction because flush failed: " << s.ToString()
                   << ", ino: " << ino << ", chunk_index: " << chunk_index;
      return s;
    }

    compacted = writer.GetCommitSlice();
    VLOG(9) << "Success compaction, compacted_slice: " << Slice2Str(compacted);
  }

  out_slice = compacted;
  return Status::OK();
}

// TODO: delete compact object after fail
Status Compactor::Compact(ContextSPtr ctx, Ino ino, uint64_t chunk_index,
                          const std::vector<Slice>& slices,
                          std::vector<Slice>& out_slices) {
  VLOG(9) << "Compactor::Compact ino: " << ino
          << ", chunk_index: " << chunk_index
          << ", slices count: " << slices.size();
  CHECK(!slices.empty()) << "invalid compact, no slices to compact";
  auto span = vfs_hub_->GetTraceManager().StartChildSpan("Compactor::Compact",
                                                         ctx->GetTraceSpan());

  std::vector<Slice> to_compact;
  int32_t skip = Skip(slices);
  VLOG(9) << "Compactor::Compact skip count: " << skip;

  for (size_t i = skip; i < slices.size(); ++i) {
    to_compact.push_back(slices[i]);
  }

  if (to_compact.empty()) {
    LOG(INFO) << "No slices to compact after skipping";
    return Status::OK();
  }

  VLOG(9) << "Compact slices count: " << to_compact.size()
          << ", skip count: " << skip
          << ", origin slices count: " << slices.size();

  Slice compacted;
  DINGOFS_RETURN_NOT_OK(DoCompact(SpanScope::GetContext(span), ino, chunk_index,
                                  to_compact, compacted));

  std::vector<Slice> out;
  out.reserve(skip + 1);

  for (int i = 0; i < skip; i++) {
    out.push_back(slices[i]);
  }
  out.push_back(compacted);

  out_slices.swap(out);

  return Status::OK();
}

Status Compactor::ForceCompact(ContextSPtr ctx, Ino ino, uint64_t chunk_index,
                               const std::vector<Slice>& slices,
                               std::vector<Slice>& out_slices) {
  VLOG(9) << "Compactor::ForceCompact ino: " << ino
          << ", chunk_index: " << chunk_index
          << ", slices count: " << slices.size();
  CHECK(!slices.empty()) << "invalid compact, no slices to compact";
  auto span = vfs_hub_->GetTraceManager().StartChildSpan("Compactor::Compact",
                                                         ctx->GetTraceSpan());
  Slice compacted;
  DINGOFS_RETURN_NOT_OK(DoCompact(SpanScope::GetContext(span), ino, chunk_index,
                                  slices, compacted));
  out_slices.push_back(compacted);
  return Status::OK();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
