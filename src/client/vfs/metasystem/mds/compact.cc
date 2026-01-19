// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/vfs/metasystem/mds/compact.h"

#include "fmt/format.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

const std::string kCompactWorkerSetName = "compact_worker_set";

DEFINE_uint32(compact_worker_num, 4, "number of compact workers");
DEFINE_uint32(compact_worker_max_pending_num, 1024,
              "compact worker max pending num");
DEFINE_bool(compact_worker_use_pthread, false, "compact worker use pthread");

void CompactChunkTask::Run() {
  auto status = Compact();
  if (!status.ok() && !status.IsNotFit()) {
    LOG(ERROR) << fmt::format(
        "[meta.compact.{}.{}.{}] compact chunk fail, status({}).", ino_,
        chunk_->GetIndex(), Id(), status.ToString());
  }

  status_ = status;

  Signal();
}

Status CompactChunkTask::Compact() {
  const uint32_t chunk_index = chunk_->GetIndex();

  auto status = chunk_->IsNeedCompaction(false);
  if (!status.ok()) return status;

  // do compact
  uint64_t version = 0;
  auto old_slices = chunk_->GetCommitedSlice(version);
  if (old_slices.empty()) return Status::OK();

  LOG(INFO) << fmt::format(
      "[meta.compact.{}.{}.{}] do compact chunk, old_slices({}) version({}).",
      ino_, chunk_index, Id(), old_slices.size(), version);

  std::vector<Slice> new_slices;
  ContextSPtr ctx = std::make_shared<Context>("");
  status = compactor_.Compact(ctx, ino_, chunk_index, old_slices, new_slices);
  if (!status.ok()) return status;

  MDSClient::CompactChunkParam param;
  param.version = version;
  param.start_pos = 0;
  param.start_slice_id = old_slices.front().id;
  param.end_pos = old_slices.size() - 1;
  param.end_slice_id = old_slices.back().id;

  for (auto& slice : new_slices) {
    param.new_slices.push_back(Helper::ToSlice(slice));
  }

  mds::ChunkEntry chunk_entry;
  status = mds_client_.CompactChunk(ctx, ino_, chunk_->GetIndex(), param,
                                    chunk_entry);
  if (!status.ok()) return status;

  bool extra_local_compact = false;
  if (!chunk_->Put(chunk_entry)) {
    extra_local_compact =
        chunk_->Compact(param.start_pos, param.start_slice_id, param.end_pos,
                        param.end_slice_id, new_slices);
  }

  LOG(INFO) << fmt::format(
      "[meta.compact.{}.{}.{}] do compact chunk finish, version({}->{}) "
      "old_slice({}) new_slices({}) final_slices({}) extra({}).",
      ino_, chunk_index, Id(), version, chunk_entry.version(),
      old_slices.size(), Helper::ToString(new_slices),
      chunk_entry.slices_size(), extra_local_compact);

  return Status::OK();
}

bool CompactProcessor::Init() {
  worker_set_ = mds::ExecqWorkerSet::NewUnique(
      kCompactWorkerSetName, FLAGS_compact_worker_num,
      FLAGS_compact_worker_max_pending_num);

  if (!worker_set_->Init()) {
    LOG(ERROR) << "init compact worker set fail.";
    return false;
  }

  return true;
}

void CompactProcessor::Stop() { worker_set_->Destroy(); }  // NOLINT

Status CompactProcessor::LaunchCompact(Ino ino, ChunkSPtr& chunk,  // NOLINT
                                       MDSClient& mds_client,
                                       Compactor& compactor, bool is_async) {
  auto task = CompactChunkTask::New(ino, chunk, mds_client, compactor);

  int64_t hash_id = ino + chunk->GetIndex();
  if (!worker_set_->ExecuteHash(hash_id, task)) {
    LOG(WARNING) << fmt::format(
        "[meta.compact.{}.{}] commit compact task fail.", ino,
        chunk->GetIndex());
    return Status::Internal("commit compact task fail");
  }

  if (!is_async) {
    task->Wait();

    return task->GetStatus();
  }

  return Status::OK();
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs