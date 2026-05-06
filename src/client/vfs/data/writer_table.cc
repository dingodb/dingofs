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

#include "client/vfs/data/writer_table.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <utility>
#include <vector>

#include "client/vfs/data/writer/file_writer.h"

namespace dingofs {
namespace client {
namespace vfs {

WriterTable::WriterTable(VFSHub* hub) : vfs_hub_(hub) {}

WriterTable::~WriterTable() {
  // Defensive: caller should Stop() explicitly. If not, mark stopped now.
  Stop();
}

Status WriterTable::Start() {
  std::lock_guard<std::mutex> lg(mutex_);
  if (stopped_) {
    return Status::Internal("WriterTable already stopped");
  }
  LOG(INFO) << "WriterTable started";
  return Status::OK();
}

void WriterTable::Stop() {
  std::lock_guard<std::mutex> lg(mutex_);
  if (stopped_) return;
  stopped_ = true;
  LOG(INFO) << "WriterTable stopped";
}

Status WriterTable::FlushAll() {
  // Snapshot live writers (transient AcquireRef on each), flush outside lock.
  // The transient ref does NOT touch holders — it is an internal hold.
  std::vector<FileWriter*> snap;
  {
    std::lock_guard<std::mutex> lg(mutex_);
    snap.reserve(writers_.size());
    for (auto& [ino, e] : writers_) {
      e.writer->AcquireRef();
      snap.push_back(e.writer);
    }
  }

  Status final_status;
  for (auto* w : snap) {
    Status s = w->Flush();
    if (!s.ok() && final_status.ok()) {
      final_status = s;
      LOG(WARNING) << fmt::format("FlushAll: writer flush failed: {}",
                                  s.ToString());
    }
    // Release the transient ref directly via FileWriter — the snap path
    // never touched holders, so it must not call ReleaseWriter.
    w->ReleaseRef();
  }
  return final_status;
}

size_t WriterTable::Size() const {
  std::lock_guard<std::mutex> lg(mutex_);
  return writers_.size();
}

FileWriter* WriterTable::AcquireWriter(uint64_t ino) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (stopped_) {
    LOG(WARNING) << "AcquireWriter on stopped WriterTable, ino=" << ino;
    return nullptr;
  }

  auto it = writers_.find(ino);
  if (it != writers_.end()) {
    it->second.writer->AcquireRef();
    it->second.holders++;
    return it->second.writer;
  }

  // First-time create. fh argument is a sentinel (0) — Phase 1.5 will remove
  // fh from the FileWriter constructor entirely.
  auto* w = new FileWriter(vfs_hub_, /*fh*/ 0, ino);
  w->AcquireRef();   // ref balance for the AcquireWriter caller
  Status s = w->Open();
  if (!s.ok()) {
    LOG(ERROR) << fmt::format("AcquireWriter Open failed, ino={}, status={}",
                              ino, s.ToString());
    // refs=1; ReleaseRef triggers delete-this.
    w->ReleaseRef();
    return nullptr;
  }
  writers_.emplace(ino, Entry{w, /*holders*/ 1});
  return w;
}

FileWriter* WriterTable::PeekWriter(uint64_t ino) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (stopped_) return nullptr;
  auto it = writers_.find(ino);
  if (it == writers_.end()) return nullptr;
  it->second.writer->AcquireRef();
  it->second.holders++;
  return it->second.writer;
}

void WriterTable::ReleaseWriter(FileWriter* writer) {
  if (writer == nullptr) return;

  uint64_t ino = writer->Ino();
  bool need_close = false;
  {
    std::lock_guard<std::mutex> lg(mutex_);
    auto it = writers_.find(ino);
    if (it == writers_.end()) {
      // Defensive: shouldn't happen if Acquire/Release are balanced. Still
      // call ReleaseRef below so the writer doesn't leak.
      LOG(WARNING) << "ReleaseWriter: ino " << ino << " not in table";
    } else {
      CHECK_EQ(it->second.writer, writer)
          << "ReleaseWriter: pointer mismatch for ino=" << ino;
      it->second.holders--;
      CHECK_GE(it->second.holders, 0);
      if (it->second.holders == 0) {
        writers_.erase(it);
        need_close = true;
      }
    }
  }

  // Close + ReleaseRef are intentionally outside the table mutex:
  //   - Close drains any inflight flush task; if held under mutex_ it
  //     would block all other Acquire/Release.
  //   - ReleaseRef may transitively call delete-this; doing it outside
  //     also keeps the table mutex's critical section short.
  if (need_close) {
    writer->Close();   // flips closed_ so SchedulePeriodicFlush stops arming
  }
  writer->ReleaseRef();   // may delete-this once refs_ reaches 0
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
