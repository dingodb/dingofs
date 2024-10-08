/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Wed Dec 04 2019
 * Author: xuchaojie
 */

#include "test/integration/snapshotcloneserver/fake_snapshot_data_store.h"

#include <fiu-control.h>
#include <fiu.h>

#include <memory>

namespace curve {
namespace snapshotcloneserver {

int FakeSnapshotDataStore::Init(const std::string& path) { return 0; }

int FakeSnapshotDataStore::PutChunkIndexData(const ChunkIndexDataName& name,
                                             const ChunkIndexData& meta) {
  std::lock_guard<std::mutex> guard(indexMapMutex_);
  fiu_return_on(
      "test/integration/snapshotcloneserver/"
      "FakeSnapshotDataStore.PutChunkIndexData",
      -1);  // NOLINT
  indexDataMap_.emplace(name.ToIndexDataChunkKey(), meta);
  return 0;
}

int FakeSnapshotDataStore::GetChunkIndexData(const ChunkIndexDataName& name,
                                             ChunkIndexData* meta) {
  std::lock_guard<std::mutex> guard(indexMapMutex_);
  fiu_return_on(
      "test/integration/snapshotcloneserver/"
      "FakeSnapshotDataStore.GetChunkIndexData",
      -1);  // NOLINT
  std::string key = name.ToIndexDataChunkKey();
  *meta = indexDataMap_[key];
  return 0;
}

int FakeSnapshotDataStore::DeleteChunkIndexData(
    const ChunkIndexDataName& name) {
  std::lock_guard<std::mutex> guard(indexMapMutex_);
  fiu_return_on(
      "test/integration/snapshotcloneserver/"
      "FakeSnapshotDataStore.DeleteChunkIndexData",
      -1);  // NOLINT
  std::string key = name.ToIndexDataChunkKey();
  indexDataMap_.erase(key);
  return 0;
}

bool FakeSnapshotDataStore::ChunkIndexDataExist(
    const ChunkIndexDataName& name) {
  std::lock_guard<std::mutex> guard(indexMapMutex_);
  std::string key = name.ToIndexDataChunkKey();
  return indexDataMap_.find(key) != indexDataMap_.end();
}

int FakeSnapshotDataStore::DeleteChunkData(const ChunkDataName& name) {
  std::lock_guard<std::mutex> guard(chunkDataMutex_);
  fiu_return_on(
      "test/integration/snapshotcloneserver/"
      "FakeSnapshotDataStore.DeleteChunkData",
      -1);  // NOLINT
  chunkData_.erase(name.ToDataChunkKey());
  return 0;
}

bool FakeSnapshotDataStore::ChunkDataExist(const ChunkDataName& name) {
  std::lock_guard<std::mutex> guard(chunkDataMutex_);
  return chunkData_.find(name.ToDataChunkKey()) != chunkData_.end();
}

int FakeSnapshotDataStore::DataChunkTranferInit(
    const ChunkDataName& name, std::shared_ptr<TransferTask> task) {
  return 0;
}

int FakeSnapshotDataStore::DataChunkTranferAddPart(
    const ChunkDataName& name, std::shared_ptr<TransferTask> task, int partNum,
    int partSize, const char* buf) {
  return 0;
}

int FakeSnapshotDataStore::DataChunkTranferComplete(
    const ChunkDataName& name, std::shared_ptr<TransferTask> task) {
  std::lock_guard<std::mutex> guard(chunkDataMutex_);
  fiu_return_on(
      "test/integration/snapshotcloneserver/"
      "FakeSnapshotDataStore.DataChunkTranferComplete",
      -1);  // NOLINT
  chunkData_.insert(name.ToDataChunkKey());
  return 0;
}

int FakeSnapshotDataStore::DataChunkTranferAbort(
    const ChunkDataName& name, std::shared_ptr<TransferTask> task) {
  return 0;
}

}  // namespace snapshotcloneserver
}  // namespace curve
