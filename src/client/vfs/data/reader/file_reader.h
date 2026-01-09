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

#ifndef DINGOFS_CLIENT_VFS_DATA_READER_FILE_READER_H_
#define DINGOFS_CLIENT_VFS_DATA_READER_FILE_READER_H_

#include <fmt/format.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>

#include "client/vfs/data/reader/chunk_reader.h"
#include "client/vfs/data/reader/read_request.h"
#include "client/vfs/data/reader/readahead_policy.h"
#include "client/vfs/data_buffer.h"
#include "common/status.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

class FileReader {
 public:
  FileReader(VFSHub* hub, uint64_t fh, uint64_t ino);

  ~FileReader();

  Status Open();

  void Close();

  Status Read(ContextSPtr ctx, DataBuffer* data_buffer, int64_t size,
              int64_t offset, uint64_t* out_rsize);

  // NOTE: if we manage filehandle by ino,
  // then write/commit_slice/fallocate/truncate/copyfile_range should call this
  void Invalidate(int64_t offset, int64_t size);

  void AcquireRef();

  // caller should ensure ReleaseRef called outside of lock
  void ReleaseRef();

 private:
  Status GetAttr(ContextSPtr ctx, Attr* attr);

  void CheckPrefetch(ContextSPtr ctx, const Attr& attr,
                     const FileRange& frange);

  void ShrinkMem();
  void SchedulePeriodicShrink();
  void RunPeriodicShrink();

  void TakeMem(int64_t size);
  void ReleaseMem(int64_t size);
  int64_t TotalMem() const;
  int64_t UsedMem() const;

  // pretected by mutex_
  void RunReadRequest(ReadRequestSptr req);
  void OnReadRequestComplete(ChunkReader* reader, ReadRequestSptr req,
                             Status s);
  // pretected by mutex_
  void DoReadRequst(ReadRequestSptr req);

  // pretected by mutex_
  ReadRequestSptr NewReadRequest(int64_t s, int64_t e);
  // pretected by mutex_
  void DeleteReadRequestUnlock(ReadRequestSptr req);
  void DeleteReadRequest(ReadRequestSptr req);
  void DeleteReadRequestAsync(ReadRequestSptr req);

  // pretected by mutex_
  void CheckReadahead(ContextSPtr ctx, const FileRange& frange, int64_t flen);
  // pretected by mutex_
  void MakeReadahead(ContextSPtr ctx, const FileRange& frange);

  // pretected by mutex_
  std::vector<int64_t> SplitRange(ContextSPtr ctx, const FileRange& frange);
  // pretected by mutex_
  std::vector<PartialReadRequest> PrepareRequests(
      ContextSPtr ctx, const std::vector<int64_t>& ranges);

  // pretected by mutex_
  bool IsProtectedReq(const ReadRequestSptr& req) const;
  // pretected by mutex_
  void CleanUpRequest(ContextSPtr ctx, const FileRange& frange);

  Status WaitAllReadRequest(ContextSPtr ctx,
                            std::vector<PartialReadRequest> reqs,
                            uint64_t* out_rsize);
  VFSHub* vfs_hub_;
  const uint64_t fh_;
  const uint64_t ino_;
  const std::string uuid_;
  const uint64_t chunk_size_{0};
  const uint64_t block_size_{0};

  std::atomic<int64_t> refs_{0};

  uint64_t last_intime_warmup_mtime_{0};
  uint64_t last_intime_warmup_trigger_{0};

  std::atomic<bool> closing_{false};

  std::mutex mutex_;
  std::unique_ptr<ReadaheadPoclicy> policy_;
  // TODO : use dec/inc refs
  // seq -> ReadRequestSptr
  std::map<int64_t, ReadRequestSptr> requests_;
};

using FileReaderUPtr = std::unique_ptr<FileReader>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_FILE_READER_H_