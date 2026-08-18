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

#include "client/vfs/data/reader/read_request.h"
#include "client/vfs/data/reader/readahead_policy.h"
#include "client/vfs/data_buffer.h"
#include "common/status.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;
class FileReaderTestPeer;

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

  Ino GetIno() const { return ino_; }

 private:
  friend class FileReaderTestPeer;
  friend void intrusive_ptr_add_ref(FileReader* reader) {
    reader->AcquireRef();
  }
  friend void intrusive_ptr_release(FileReader* reader) {
    reader->ReleaseRef();
  }

  Status GetAttr(ContextSPtr ctx, Attr* attr);

  void CheckPrefetch(ContextSPtr ctx, const Attr& attr,
                     const FileRange& frange);

  void ShrinkMem();
  void SchedulePeriodicShrink();
  void RunPeriodicShrink();

  int64_t TotalMem() const;
  int64_t UsedMem() const;
  double UsedRatio() const;

  // Posts DoReadRequst to the read executor; takes no locks itself.
  void RunReadRequest(ReadRequestSptr req);
  // Run on executor threads WITHOUT mutex_; they only take req->mutex. This is
  // why cleanup eligibility must be re-established under mutex_ before erase.
  void OnReadRequestComplete(ReadRequestSptr req, Status s);
  void DoReadRequst(ReadRequestSptr req);

  // Caller must hold mutex_.
  ReadRequestSptr NewReadRequest(int64_t s, int64_t e);
  // Caller must hold mutex_.
  void DeleteReadRequestUnlock(ReadRequestSptr req);
  void DeleteReadRequest(ReadRequestSptr req);
  void ScheduleReadRequestCleanup(ReadRequestSptr req);

  // Caller must hold mutex_.
  void CheckReadahead(ContextSPtr ctx, const FileRange& frange, int64_t flen);
  // Caller must hold mutex_.
  void MakeReadahead(ContextSPtr ctx, const FileRange& frange);

  // Caller must hold mutex_.
  std::vector<int64_t> SplitRange(ContextSPtr ctx, const FileRange& frange);
  // Caller must hold mutex_.
  std::vector<PartialReadRequest> PrepareRequests(
      ContextSPtr ctx, const std::vector<int64_t>& ranges);

  // Caller must hold mutex_.
  bool IsProtectedReq(const ReadRequestSptr& req) const;
  // Caller must hold mutex_.
  void CleanUpRequest(ContextSPtr ctx, const FileRange& frange);

  Status WaitAllReadRequest(ContextSPtr ctx,
                            std::vector<PartialReadRequest> reqs,
                            uint64_t* out_rsize);
  VFSHub* vfs_hub_;
  const uint64_t fh_;
  const uint64_t ino_;
  const std::string uuid_;
  const int32_t chunk_size_{0};
  const int32_t block_size_{0};

  std::atomic<int64_t> refs_{0};

  // Guards the two warmup watermarks below: CheckPrefetch runs before Read
  // takes mutex_, and check-plus-advance must be atomic across readers.
  std::mutex intime_warmup_mutex_;
  uint64_t last_intime_warmup_mtime_{0};
  uint64_t last_intime_warmup_trigger_{0};

  std::atomic<bool> closing_{false};

  std::mutex mutex_;
  std::unique_ptr<ReadaheadPoclicy> policy_;
  // TODO : use dec/inc refs
  // seq -> ReadRequestSptr
  std::map<int64_t, ReadRequestSptr> requests_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_FILE_READER_H_
