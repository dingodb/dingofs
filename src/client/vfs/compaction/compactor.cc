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

#include <algorithm>
#include <cstdint>

#include "client/vfs/common/basync_util.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/data/reader/chunk_req_reader.h"
#include "client/vfs/data/slice/common.h"
#include "client/vfs/data/slice/slice_writer.h"
#include "client/vfs/hub/vfs_hub.h"

namespace dingofs {
namespace client {
namespace vfs {

// TODO: delete compact object after fail
Status Compactor::Compact(ContextSPtr ctx, Ino ino, uint64_t chunk_index,
                          const std::vector<Slice>& slices,
                          std::vector<Slice>& out_slices) {
  CHECK(!slices.empty()) << "invalid compact, no slices to compact";
  auto span = vfs_hub_->GetTraceManager().StartChildSpan("Compactor::Compact",
                                                         ctx->GetTraceSpan());
  auto fs_info = vfs_hub_->GetFsInfo();

  int64_t chunk_size = fs_info.chunk_size;

  int64_t fpos = (chunk_index * chunk_size) + chunk_size;
  int64_t fend = 0;

  for (const auto& slice : slices) {
    fpos = std::min((int64_t)slice.offset, fpos);
    fend = std::max((int64_t)(slice.offset + slice.length), fend);
  }

  int64_t offset_in_chunk = fpos % chunk_size;
  int64_t len = fend - fpos;

  ChunkReq req(ino, chunk_index, offset_in_chunk,
               FileRange{.offset = fpos, .len = len});

  VLOG(12) << "Start comaction for req: " << req.ToString();
  CHECK_GT(fend, fpos) << "invalid slices for compaction";

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
    CHECK_EQ(data_size, static_cast<uint64_t>(len));

    to_write.resize(data.Size());
    data.CopyTo(to_write.data());
  }

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

    Slice slice = writer.GetCommitSlice();
    VLOG(12) << "Success compaction, finish_slice: " << Slice2Str(slice);
    out_slices.push_back(slice);
  }

  return Status::OK();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
