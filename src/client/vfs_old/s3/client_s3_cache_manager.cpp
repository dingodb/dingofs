/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Project: dingo
 * Created Date: 21-8-18
 * Author: huyao
 */

#include "client/vfs_old/s3/client_s3_cache_manager.h"

#include <butil/time.h>
#include <bvar/bvar.h>
#include <glog/logging.h>
#include <malloc.h>
#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "absl/synchronization/blocking_counter.h"
#include "client/blockcache/cache_store.h"
#include "client/blockcache/error.h"
#include "client/blockcache/local_filesystem.h"
#include "client/common/dynamic_config.h"
#include "client/datastream/data_stream.h"
#include "client/vfs_old/filesystem/meta.h"
#include "client/vfs_old/kvclient/kvclient_manager.h"
#include "client/vfs_old/s3/client_s3_adaptor.h"
#include "stub/metric/metric.h"

static dingofs::stub::metric::S3MultiManagerMetric* g_s3MultiManagerMetric =
    new dingofs::stub::metric::S3MultiManagerMetric();

namespace dingofs {
namespace client {

using dataaccess::aws::GetObjectAsyncCallBack;
using dataaccess::aws::PutObjectAsyncCallBack;
using dataaccess::aws::PutObjectAsyncContext;

using blockcache::BCACHE_ERROR;
using blockcache::Block;
using blockcache::BlockContext;
using blockcache::BlockFrom;
using blockcache::BlockKey;
using blockcache::StrErr;

using datastream::DataStream;
using filesystem::Ino;
using utils::CountDownEvent;
using utils::ReadLockGuard;
using utils::RWLock;
using utils::WriteLockGuard;

using pb::mds::FSStatusCode;
using pb::metaserver::Inode;
using pb::metaserver::S3ChunkInfo;
using pb::metaserver::S3ChunkInfoList;

using common::FLAGS_fuse_read_max_retry_s3_not_exist;

void FsCacheManager::DataCacheNumInc() {
  g_s3MultiManagerMetric->writeDataCacheNum << 1;
  VLOG(9) << "DataCacheNumInc() v: 1, wDataCacheNum:"
          << wDataCacheNum_.load(std::memory_order_relaxed);
  wDataCacheNum_.fetch_add(1, std::memory_order_relaxed);
}

void FsCacheManager::DataCacheNumFetchSub(uint64_t v) {
  g_s3MultiManagerMetric->writeDataCacheNum << -1 * v;
  VLOG(9) << "DataCacheNumFetchSub() v:" << v << ", wDataCacheNum_:"
          << wDataCacheNum_.load(std::memory_order_relaxed);
  assert(wDataCacheNum_.load(std::memory_order_relaxed) >= v);
  wDataCacheNum_.fetch_sub(v, std::memory_order_relaxed);
}

void FsCacheManager::DataCacheByteInc(uint64_t v) {
  g_s3MultiManagerMetric->writeDataCacheByte << v;
  VLOG(9) << "DataCacheByteInc() v:" << v << ", wDataCacheByte:"
          << wDataCacheByte_.load(std::memory_order_relaxed);
  wDataCacheByte_.fetch_add(v, std::memory_order_relaxed);
}

void FsCacheManager::DataCacheByteDec(uint64_t v) {
  g_s3MultiManagerMetric->writeDataCacheByte << -1 * v;
  VLOG(9) << "DataCacheByteDec() v:" << v << ", wDataCacheByte:"
          << wDataCacheByte_.load(std::memory_order_relaxed);
  assert(wDataCacheByte_.load(std::memory_order_relaxed) >= v);
  wDataCacheByte_.fetch_sub(v, std::memory_order_relaxed);
}

FileCacheManagerPtr FsCacheManager::FindFileCacheManager(uint64_t inodeId) {
  ReadLockGuard readLockGuard(rwLock_);

  auto it = fileCacheManagerMap_.find(inodeId);
  if (it != fileCacheManagerMap_.end()) {
    return it->second;
  }

  return nullptr;
}

FileCacheManagerPtr FsCacheManager::FindOrCreateFileCacheManager(
    uint64_t fsId, uint64_t inodeId) {
  WriteLockGuard writeLockGuard(rwLock_);

  auto it = fileCacheManagerMap_.find(inodeId);
  if (it != fileCacheManagerMap_.end()) {
    return it->second;
  }

  FileCacheManagerPtr fileCacheManager = std::make_shared<FileCacheManager>(
      fsId, inodeId, s3ClientAdaptor_, kvClientManager_, readTaskPool_);
  auto ret = fileCacheManagerMap_.emplace(inodeId, fileCacheManager);
  g_s3MultiManagerMetric->fileManagerNum << 1;
  assert(ret.second);
  (void)ret;
  return fileCacheManager;
}

void FsCacheManager::ReleaseFileCacheManager(uint64_t inodeId) {
  WriteLockGuard writeLockGuard(rwLock_);

  auto iter = fileCacheManagerMap_.find(inodeId);
  if (iter == fileCacheManagerMap_.end()) {
    VLOG(1) << "ReleaseFileCacheManager, do not find file cache manager of "
               ", inodeId="
            << inodeId;
    return;
  }

  fileCacheManagerMap_.erase(iter);
  g_s3MultiManagerMetric->fileManagerNum << -1;
}

bool FsCacheManager::Set(DataCachePtr dataCache,
                         std::list<DataCachePtr>::iterator* outIter) {
  std::lock_guard<std::mutex> lk(lruMtx_);
  VLOG(3) << "lru current byte:" << lruByte_
          << ",lru max byte:" << readCacheMaxByte_
          << ", dataCache len:" << dataCache->GetLen();
  if (readCacheMaxByte_ == 0) {
    return false;
  }
  // trim cache without consider dataCache's size, because its size is
  // expected to be very smaller than `readCacheMaxByte_`
  if (lruByte_ >= readCacheMaxByte_) {
    uint64_t retiredBytes = 0;
    auto iter = lruReadDataCacheList_.end();

    while (lruByte_ >= readCacheMaxByte_) {
      --iter;
      auto& trim = *iter;
      trim->SetReadCacheState(false);
      lruByte_ -= trim->GetActualLen();
      retiredBytes += trim->GetActualLen();
    }

    std::list<DataCachePtr> retired;
    retired.splice(retired.end(), lruReadDataCacheList_, iter,
                   lruReadDataCacheList_.end());

    VLOG(3) << "lru release " << retiredBytes << " bytes, retired "
            << retired.size() << " data cache";

    releaseReadCache_.Release(&retired);
  }

  lruByte_ += dataCache->GetActualLen();
  dataCache->SetReadCacheState(true);
  lruReadDataCacheList_.push_front(std::move(dataCache));
  *outIter = lruReadDataCacheList_.begin();
  return true;
}

void FsCacheManager::Get(std::list<DataCachePtr>::iterator iter) {
  std::lock_guard<std::mutex> lk(lruMtx_);

  if (!(*iter)->InReadCache()) {
    return;
  }

  lruReadDataCacheList_.splice(lruReadDataCacheList_.begin(),
                               lruReadDataCacheList_, iter);
}

bool FsCacheManager::Delete(std::list<DataCachePtr>::iterator iter) {
  std::lock_guard<std::mutex> lk(lruMtx_);

  if (!(*iter)->InReadCache()) {
    return false;
  }

  (*iter)->SetReadCacheState(false);
  lruByte_ -= (*iter)->GetActualLen();
  lruReadDataCacheList_.erase(iter);
  return true;
}

DINGOFS_ERROR FsCacheManager::FsSync(bool force) {
  std::unordered_map<uint64_t, FileCacheManagerPtr> pending;
  {
    WriteLockGuard writeLockGuard(rwLock_);
    pending = fileCacheManagerMap_;
  }

  auto post_flush = [&](Ino ino, FileCacheManagerPtr file, DINGOFS_ERROR ret) {
    if (ret == DINGOFS_ERROR::OK) {
      WriteLockGuard writeLockGuard(rwLock_);
      auto iter1 = fileCacheManagerMap_.find(ino);
      if (iter1 == fileCacheManagerMap_.end()) {
        VLOG(1) << "FsSync, chunk cache for inodeId=" << ino << " is removed";
      } else {
        VLOG(9) << "FileCacheManagerPtr count:" << iter1->second.use_count()
                << ", inodeId=" << iter1->first;
        // tmp and fileCacheManagerMap_ has this FileCacheManagerPtr, so
        // count is 2 if count more than 2, this mean someone thread has
        // this FileCacheManagerPtr
        // TODO(@huyao) https://github.com/opendingo/dingo/issues/1473
        if ((iter1->second->IsEmpty()) && (iter1->second.use_count() <= 2)) {
          VLOG(9) << "Release FileCacheManager, inodeId="
                  << iter1->second->GetInodeId();
          fileCacheManagerMap_.erase(iter1);
          g_s3MultiManagerMetric->fileManagerNum << -1;
        }
      }
    } else if (ret == DINGOFS_ERROR::NOTEXIST) {
      file->ReleaseCache();
      WriteLockGuard writeLockGuard(rwLock_);
      auto iter1 = fileCacheManagerMap_.find(ino);
      if (iter1 != fileCacheManagerMap_.end()) {
        VLOG(9) << "Release FileCacheManager, inodeId="
                << iter1->second->GetInodeId();
        fileCacheManagerMap_.erase(iter1);
        g_s3MultiManagerMetric->fileManagerNum << -1;
      }
    } else {
      LOG(ERROR) << "fs fssync error, ret: " << ret;
    }
  };

  std::atomic<uint64_t> count(pending.size());
  CountDownEvent count_down_event(count);
  DINGOFS_ERROR rc = DINGOFS_ERROR::OK;
  for (const auto& item : pending) {
    Ino ino = item.first;
    auto file = item.second;
    DataStream::GetInstance().EnterFlushFileQueue([&, ino, file, post_flush]() {
      auto code = file->Flush(force);
      post_flush(ino, file, code);
      if (code != DINGOFS_ERROR::OK && code != DINGOFS_ERROR::NOTEXIST) {
        rc = code;
      }
      count_down_event.Signal();
    });
  }
  count_down_event.Wait();
  return rc;
}

int FileCacheManager::Write(uint64_t offset, uint64_t length,
                            const char* dataBuf) {
  uint64_t chunk_size = s3ClientAdaptor_->GetChunkSize();
  uint64_t index = offset / chunk_size;
  uint64_t chunk_pos = offset % chunk_size;
  uint64_t write_len = 0;
  uint64_t write_offset = 0;

  while (length > 0) {
    if (chunk_pos + length > chunk_size) {
      write_len = chunk_size - chunk_pos;
    } else {
      write_len = length;
    }

    WriteChunk(index, chunk_pos, write_len, (dataBuf + write_offset));

    length -= write_len;
    index++;
    write_offset += write_len;
    chunk_pos = (chunk_pos + write_len) % chunk_size;
  }

  return write_offset;
}

void FileCacheManager::WriteChunk(uint64_t index, uint64_t chunkPos,
                                  uint64_t writeLen, const char* dataBuf) {
  VLOG(9) << "WriteChunk start, chunkIndex: " << index
          << ", chunkPos: " << chunkPos;
  ChunkCacheManagerPtr chunk_cache_manager =
      FindOrCreateChunkCacheManager(index);
  WriteLockGuard write_lock_guard(chunk_cache_manager->rwLockChunk_);  // todo

  DataCachePtr data_cache;
  std::vector<DataCachePtr> merge_data_cache_ver;
  data_cache = chunk_cache_manager->FindWriteableDataCache(
      chunkPos, writeLen, &merge_data_cache_ver, inode_);

  if (data_cache) {
    data_cache->Write(chunkPos, writeLen, dataBuf, merge_data_cache_ver);
  } else {
    chunk_cache_manager->WriteNewDataCache(s3ClientAdaptor_, chunkPos, writeLen,
                                           dataBuf);
  }

  VLOG(9) << "WriteChunk end, chunkIndex: " << index
          << ", chunkPos: " << chunkPos;
}

ChunkCacheManagerPtr FileCacheManager::FindOrCreateChunkCacheManager(
    uint64_t index) {
  WriteLockGuard writeLockGuard(rwLock_);

  auto it = chunkCacheMap_.find(index);
  if (it != chunkCacheMap_.end()) {
    return it->second;
  }

  ChunkCacheManagerPtr chunkCacheManager = std::make_shared<ChunkCacheManager>(
      index, s3ClientAdaptor_, kvClientManager_);
  auto ret = chunkCacheMap_.emplace(index, chunkCacheManager);
  g_s3MultiManagerMetric->chunkManagerNum << 1;
  assert(ret.second);
  (void)ret;
  return chunkCacheManager;
}

void FileCacheManager::GetChunkLoc(uint64_t offset, uint64_t* index,
                                   uint64_t* chunkPos, uint64_t* chunkSize) {
  *chunkSize = s3ClientAdaptor_->GetChunkSize();
  *index = offset / *chunkSize;
  *chunkPos = offset % *chunkSize;
}

void FileCacheManager::GetBlockLoc(uint64_t offset, uint64_t* chunkIndex,
                                   uint64_t* chunkPos, uint64_t* blockIndex,
                                   uint64_t* blockPos) {
  uint64_t chunkSize = 0;
  uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
  GetChunkLoc(offset, chunkIndex, chunkPos, &chunkSize);

  *blockIndex = offset % chunkSize / blockSize;
  *blockPos = offset % chunkSize % blockSize;
}

void FileCacheManager::ReadFromMemCache(
    uint64_t offset, uint64_t length, char* dataBuf, uint64_t* actualReadLen,
    std::vector<ReadRequest>* memCacheMissRequest) {
  uint64_t index = 0, chunk_pos = 0, chunk_size = 0;
  GetChunkLoc(offset, &index, &chunk_pos, &chunk_size);

  uint64_t data_buffer_offset = 0;
  while (length > 0) {
    // |--------------------------------|
    // 0                             chunksize
    //                chunkPos                   length + chunkPos
    //                   |-------------------------|
    //                   |--------------|
    //                    currentReadLen
    uint64_t current_read_len =
        chunk_pos + length > chunk_size ? chunk_size - chunk_pos : length;

    // 1. read from local memory cache
    // 2. generate cache miss request
    ChunkCacheManagerPtr chunk_cache_manager =
        FindOrCreateChunkCacheManager(index);
    std::vector<ReadRequest> tmp_miss_requests;
    chunk_cache_manager->ReadChunk(index, chunk_pos, current_read_len, dataBuf,
                                   data_buffer_offset, &tmp_miss_requests);
    memCacheMissRequest->insert(memCacheMissRequest->end(),
                                tmp_miss_requests.begin(),
                                tmp_miss_requests.end());

    length -= current_read_len;              // left length
    index++;                                 // next index
    data_buffer_offset += current_read_len;  // next data buffer offset
    chunk_pos = (chunk_pos + current_read_len) % chunk_size;  // next chunkPos
  }

  *actualReadLen = data_buffer_offset;

  VLOG_IF(3, memCacheMissRequest->empty()) << "great! memory cache all hit.";
}

int FileCacheManager::GenerateKVRequest(
    const std::shared_ptr<InodeWrapper>& inode_wrapper,
    const std::vector<ReadRequest>& read_request, char* data_buf,
    std::vector<S3ReadRequest>* kv_request) {
  ::dingofs::utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();

  const Inode* inode = inode_wrapper->GetInodeLocked();
  const auto* s3chunkinfo = inode_wrapper->GetChunkInfoMap();

  for_each(
      read_request.begin(), read_request.end(), [&](const ReadRequest& req) {
        VLOG(6) << "inodeId=" << inode->inodeid()
                << " requset: " << req.DebugString();

        auto info_iter = s3chunkinfo->find(req.index);
        if (info_iter == s3chunkinfo->end()) {
          VLOG(6) << "inodeId=" << inode->inodeid()
                  << " s3chunkinfo do not find index = " << req.index;
          memset(data_buf + req.bufOffset, 0, req.len);
          return;
        } else {
          std::vector<S3ReadRequest> tmp_kv_requests;
          GenerateS3Request(req, info_iter->second, data_buf, &tmp_kv_requests,
                            inode->fsid(), inode->inodeid());
          kv_request->insert(kv_request->end(), tmp_kv_requests.begin(),
                             tmp_kv_requests.end());
        }
      });

  VLOG(9) << "inodeId=" << inode->inodeid() << " process "
          << S3ReadRequestVecDebugString(*kv_request) << " ok";

  return 0;
}

int FileCacheManager::HandleReadS3NotExist(
    uint32_t retry, const std::shared_ptr<InodeWrapper>& inode_wrapper) {
  uint32_t max_interval_ms =
      s3ClientAdaptor_->GetMaxReadRetryIntervalMs();  // hardcode, fixme
  uint32_t retry_interval_ms = s3ClientAdaptor_->GetReadRetryIntervalMs();

  if (retry == 1) {
    dingofs::utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();
    if (DINGOFS_ERROR::OK != inode_wrapper->RefreshS3ChunkInfo()) {
      LOG(ERROR) << "refresh inodeId=" << inode_wrapper->GetInodeId()
                 << " fail";
      return -1;
    }
  } else if (retry * retry_interval_ms < max_interval_ms) {
    LOG(WARNING) << "read inodeId=" << inode_wrapper->GetInodeId()
                 << " retry = " << retry << " will sleep "
                 << retry_interval_ms * retry << " ms";
    bthread_usleep(retry_interval_ms * retry * 1000);
  } else {
    LOG(WARNING) << "read inodeId=" << inode_wrapper->GetInodeId()
                 << " retry = " << retry
                 << ", reach max interval = " << max_interval_ms << " ms"
                 << " , will sleep " << max_interval_ms << " ms";
    bthread_usleep(max_interval_ms * 1000);
  }

  if (retry > FLAGS_fuse_read_max_retry_s3_not_exist) {
    LOG(ERROR) << "Fail read inodeId=" << inode_wrapper->GetInodeId()
               << " retry = " << retry << ", reach max retry = "
               << FLAGS_fuse_read_max_retry_s3_not_exist;
    return -1;
  }

  return 0;
}

int FileCacheManager::Read(uint64_t inode_id, uint64_t offset, uint64_t length,
                           char* data_buf) {
  VLOG(1) << "read inodeId=" << inode_id << ", offset=" << offset
          << ", length=" << length;
  // 1. read from memory cache
  uint64_t actual_read_len = 0;
  std::vector<ReadRequest> mem_cache_miss_request;
  ReadFromMemCache(offset, length, data_buf, &actual_read_len,
                   &mem_cache_miss_request);
  if (mem_cache_miss_request.empty()) {
    return actual_read_len;
  }

  // 2. read from localcache and remote cluster
  std::shared_ptr<InodeWrapper> inode_wrapper;
  auto inode_manager = s3ClientAdaptor_->GetInodeCacheManager();
  if (DINGOFS_ERROR::OK != inode_manager->GetInode(inode_id, inode_wrapper)) {
    LOG(ERROR) << "get inodeId=" << inode_id << " fail";
    return -1;
  }

  // in time warmup
  if (s3ClientAdaptor_->HasDiskCache() && common::FLAGS_in_time_warmup) {
    auto inode_prefetch_manager = s3ClientAdaptor_->GetIntimeWarmUpManager();
    CHECK_NOTNULL(inode_prefetch_manager);
    inode_prefetch_manager->Submit(inode_wrapper);
  }

  uint32_t retry = 0;
  do {
    // generate kv request
    std::vector<S3ReadRequest> kv_requests;
    GenerateKVRequest(inode_wrapper, mem_cache_miss_request, data_buf,
                      &kv_requests);

    // read from kv cluster (localcache -> remote kv cluster -> s3)
    // localcache/remote kv cluster fail will not return error code.
    // Failure to read from s3 will eventually return failure.
    ReadStatus ret =
        ReadKVRequest(kv_requests, data_buf, inode_wrapper->GetLength());
    if (ret == ReadStatus::OK) {
      break;
    }

    if (ret == ReadStatus::S3_NOT_EXIST) {
      retry++;
      if (0 != HandleReadS3NotExist(retry, inode_wrapper)) {
        return -1;
      }
    } else {
      LOG(WARNING) << "read inodeId=" << inode_id
                   << " from s3 failed, ret = " << static_cast<int>(ret);
      // TODO: maybe we should return -1 here
      return static_cast<int>(ret);
    }
  } while (true);

  return actual_read_len;
}

bool FileCacheManager::ReadKVRequestFromLocalCache(const BlockKey& key,
                                                   char* buffer,
                                                   uint64_t offset,
                                                   uint64_t len) {
  {
    auto block_cache = s3ClientAdaptor_->GetBlockCache();
    if (!block_cache->IsCached(key)) {
      return false;
    }

    auto rc = block_cache->Range(key, offset, len, buffer, false);
    if (rc != BCACHE_ERROR::OK) {
      LOG(WARNING) << "Object " << key.Filename() << " not cached in disk.";
      return false;
    }
  }
  return true;
}

bool FileCacheManager::ReadKVRequestFromRemoteCache(const std::string& name,
                                                    char* databuf,
                                                    uint64_t offset,
                                                    uint64_t length) {
  if (!kvClientManager_) {
    return false;
  }

  auto task = std::make_shared<GetKVCacheTask>(name, databuf, offset, length);
  CountDownEvent event(1);
  task->done = [&](const std::shared_ptr<GetKVCacheTask>& task) {
    (void)task;
    event.Signal();
    return;
  };
  kvClientManager_->Get(task);
  event.Wait();

  return task->res;
}

bool FileCacheManager::ReadKVRequestFromS3(const std::string& name,
                                           char* databuf, uint64_t offset,
                                           uint64_t length, BCACHE_ERROR* rc) {
  {
    *rc = s3ClientAdaptor_->GetS3Client()->Range(name, offset, length, databuf);
    if (*rc != BCACHE_ERROR::OK) {
      LOG(ERROR) << "Object " << name << " read from s3 failed" << ", rc=" << rc
                 << ", " << StrErr(*rc);
      return false;
    }
  }

  return true;
}

FileCacheManager::ReadStatus FileCacheManager::ReadKVRequest(
    const std::vector<S3ReadRequest>& kv_requests, char* data_buf,
    uint64_t file_len) {
  absl::BlockingCounter counter(kv_requests.size());
  std::once_flag cancel_flag;
  std::atomic<bool> is_canceled{false};
  std::atomic<BCACHE_ERROR> ret_code{BCACHE_ERROR::OK};

  for (const auto& req : kv_requests) {
    readTaskPool_->Enqueue([&]() {
      auto defer = absl::MakeCleanup([&]() { counter.DecrementCount(); });
      if (is_canceled) {
        LOG(WARNING) << "kv request is canceled " << req.DebugString();
        return;
      }
      ProcessKVRequest(req, data_buf, file_len, cancel_flag, is_canceled,
                       ret_code);
    });
  }

  counter.Wait();

  VLOG(3) << "read  inodeId=" << inode_
          << " kv request end, ret_code : " << ret_code.load()
          << ", is_canceled: " << is_canceled.load();
  return toReadStatus(ret_code.load());
}

void FileCacheManager::ProcessKVRequest(const S3ReadRequest& req,
                                        char* data_buf, uint64_t file_len,
                                        std::once_flag& cancel_flag,
                                        std::atomic<bool>& is_canceled,
                                        std::atomic<BCACHE_ERROR>& ret_code) {
  VLOG(3) << "read inodeId=" << inode_ << " from kv request "
          << req.DebugString();
  uint64_t chunk_index = 0;
  uint64_t chunk_pos = 0;
  uint64_t block_index = 0;
  uint64_t block_pos = 0;
  GetBlockLoc(req.offset, &chunk_index, &chunk_pos, &block_index, &block_pos);

  const uint64_t block_size = s3ClientAdaptor_->GetBlockSize();
  const uint64_t chunk_size = s3ClientAdaptor_->GetChunkSize();

  // prefetch
  if (s3ClientAdaptor_->HasDiskCache() && common::FLAGS_s3_prefetch) {
    PrefetchForBlock(req, file_len, block_size, chunk_size, block_index);
  }

  // read request
  // |--------------------------------|----------------------------------|
  // 0                             blockSize                   2*blockSize
  //                blockPos                   length + blockPos
  //                   |-------------------------|
  //                   |--------------|
  //                    current_read_len
  uint64_t length = req.len;
  uint64_t current_read_len = 0;
  uint64_t read_buf_offset = 0;
  uint64_t object_offset = req.objectOffset;

  while (length > 0) {
    current_read_len =
        length + block_pos > block_size ? block_size - block_pos : length;
    assert(block_pos >= object_offset);
    BlockKey key(req.fsId, req.inodeId, req.chunkId, block_index,
                 req.compaction);
    char* current_buf = data_buf + req.readOffset + read_buf_offset;

    // read from localcache -> remotecache -> s3
    do {
      std::string name = key.Filename();
      std::string store_key = key.StoreKey();
      if (ReadKVRequestFromLocalCache(
              key, current_buf, block_pos - object_offset, current_read_len)) {
        VLOG(9) << "inodeId=" << inode_ << " read " << store_key
                << " from local cache ok";
        break;
      }

      if (ReadKVRequestFromRemoteCache(
              name, current_buf, block_pos - object_offset, current_read_len)) {
        VLOG(9) << "inodeId=" << inode_ << " read " << name
                << " from remote cache ok";
        break;
      }

      BCACHE_ERROR rc = BCACHE_ERROR::OK;
      if (ReadKVRequestFromS3(store_key, current_buf, block_pos - object_offset,
                              current_read_len, &rc)) {
        VLOG(9) << "inodeId=" << inode_ << " read " << store_key
                << " offset: " << (block_pos - object_offset)
                << " len: " << current_read_len << " from s3 ok";
        break;
      }

      LOG(ERROR) << "inodeId=" << inode_ << " read " << name << " fail"
                 << ", rc:" << rc;

      // make sure variable is set only once
      std::call_once(cancel_flag, [&]() {
        is_canceled.store(true);
        ret_code.store(rc);
      });

      return;
    } while (false);

    // update param
    {
      length -= current_read_len;           // Remaining read data length
      read_buf_offset += current_read_len;  // next read offset
      block_index++;
      block_pos = (block_pos + current_read_len) % block_size;
      object_offset = 0;
    }
  }
}

void FileCacheManager::PrefetchForBlock(const S3ReadRequest& req,
                                        uint64_t file_len, uint64_t block_size,
                                        uint64_t chunk_size,
                                        uint64_t start_block_index) {
  uint32_t prefetch_blocks = s3ClientAdaptor_->GetPrefetchBlocks();
  uint32_t object_prefix = s3ClientAdaptor_->GetObjectPrefix();

  std::vector<std::pair<BlockKey, uint64_t>> prefetch_objs;

  uint64_t block_index = start_block_index;
  for (uint32_t i = 0; i < prefetch_blocks; i++) {
    BlockKey key(req.fsId, req.inodeId, req.chunkId, block_index,
                 req.compaction);
    uint64_t max_read_len = (block_index + 1) * block_size;
    uint64_t need_read_len = max_read_len > file_len
                                 ? file_len - (block_index * block_size)
                                 : block_size;

    prefetch_objs.push_back(std::make_pair(key, need_read_len));

    block_index++;
    if (max_read_len > file_len || block_index >= chunk_size / block_size) {
      break;
    }
  }

  PrefetchS3Objs(prefetch_objs);
}

void FileCacheManager::PrefetchS3Objs(
    const std::vector<std::pair<BlockKey, uint64_t>>& prefetch_objs) {
  for (const auto& obj : prefetch_objs) {
    BlockKey key = obj.first;
    std::string name = key.StoreKey();
    uint64_t read_len = obj.second;
    VLOG(3) << "try to prefetch s3 obj inodeId=" << key.ino
            << " block: " << name << ", read len: " << read_len;
    s3ClientAdaptor_->GetBlockCache()->SubmitPreFetch(key, read_len);
  }
}

void FileCacheManager::HandleReadRequest(
    const ReadRequest& request, const S3ChunkInfo& s3ChunkInfo,
    std::vector<ReadRequest>* addReadRequests,
    std::vector<uint64_t>* deletingReq, std::vector<S3ReadRequest>* requests,
    char* dataBuf, uint64_t fsId, uint64_t inodeId) {
  uint64_t block_size = s3ClientAdaptor_->GetBlockSize();
  uint64_t chunk_size = s3ClientAdaptor_->GetChunkSize();

  uint64_t s3_chunk_info_offset = s3ChunkInfo.offset();
  uint64_t s3_chunk_info_len = s3ChunkInfo.len();

  VLOG(9) << "inodeId=" << inodeId
          << " HandleReadRequest request index:" << request.index
          << ", chunkPos:" << request.chunkPos << ", len:" << request.len
          << ", bufOffset:" << request.bufOffset;

  VLOG(9) << "inodeId=" << inodeId
          << " HandleReadRequest s3info chunkid:" << s3ChunkInfo.chunkid()
          << ", offset:" << s3_chunk_info_offset
          << ", len:" << s3_chunk_info_len
          << ", compaction:" << s3ChunkInfo.compaction()
          << ", zero:" << s3ChunkInfo.zero();

  S3ReadRequest s3_request;
  uint64_t file_offset = request.index * chunk_size + request.chunkPos;
  uint64_t length = request.len;
  uint64_t buf_offset = request.bufOffset;

  uint64_t read_offset = 0;
  /*
           -----             read block
                  ------     S3ChunkInfo
  */
  if (file_offset + length <= s3_chunk_info_offset) {
    return;
    /*
         -----              ------------   read block           -
            ------             -----       S3ChunkInfo
    */
  } else if ((s3_chunk_info_offset > file_offset) &&
             (s3_chunk_info_offset < file_offset + length)) {
    ReadRequest split_request;
    split_request.index = request.index;
    split_request.chunkPos = request.chunkPos;
    split_request.len = s3_chunk_info_offset - file_offset;
    split_request.bufOffset = buf_offset;
    addReadRequests->emplace_back(split_request);
    deletingReq->emplace_back(request.chunkPos);
    read_offset += split_request.len;
    /*
         -----                 read block           -
            ------             S3ChunkInfo
    */
    if (file_offset + length <= s3_chunk_info_offset + s3_chunk_info_len) {
      if (s3ChunkInfo.zero()) {
        memset(static_cast<char*>(dataBuf) + buf_offset + read_offset, 0,
               file_offset + length - s3_chunk_info_offset);
      } else {
        s3_request.chunkId = s3ChunkInfo.chunkid();
        s3_request.offset = s3_chunk_info_offset;
        s3_request.len = file_offset + length - s3_chunk_info_offset;
        s3_request.objectOffset =
            s3_chunk_info_offset % chunk_size % block_size;
        s3_request.readOffset = buf_offset + read_offset;
        s3_request.compaction = s3ChunkInfo.compaction();
        s3_request.fsId = fsId;
        s3_request.inodeId = inodeId;
        requests->push_back(s3_request);
      }
      /*
                           ------------   read block           -
                              -----       S3ChunkInfo
      */
    } else {
      if (s3ChunkInfo.zero()) {
        memset(static_cast<char*>(dataBuf) + buf_offset + read_offset, 0,
               s3_chunk_info_len);
      } else {
        s3_request.chunkId = s3ChunkInfo.chunkid();
        s3_request.offset = s3_chunk_info_offset;
        s3_request.len = s3_chunk_info_len;
        s3_request.objectOffset =
            s3_chunk_info_offset % chunk_size % block_size;
        s3_request.readOffset = buf_offset + read_offset;
        s3_request.compaction = s3ChunkInfo.compaction();
        s3_request.fsId = fsId;
        s3_request.inodeId = inodeId;
        requests->push_back(s3_request);
      }

      ReadRequest split_request;

      read_offset += s3_chunk_info_len;
      split_request.index = request.index;
      split_request.chunkPos = request.chunkPos + read_offset;
      split_request.len =
          file_offset + length - (s3_chunk_info_offset + s3_chunk_info_len);
      split_request.bufOffset = buf_offset + read_offset;
      addReadRequests->emplace_back(split_request);
    }
    /*
          ----                      ---------   read block
        ----------                --------      S3ChunkInfo
    */
  } else if ((s3_chunk_info_offset <= file_offset) &&
             (s3_chunk_info_offset + s3_chunk_info_len > file_offset)) {
    deletingReq->emplace_back(request.chunkPos);
    /*
          ----                    read block
        ----------                S3ChunkInfo
    */
    if (file_offset + length <= s3_chunk_info_offset + s3_chunk_info_len) {
      if (s3ChunkInfo.zero()) {
        memset(static_cast<char*>(dataBuf) + buf_offset + read_offset, 0,
               length);
      } else {
        s3_request.chunkId = s3ChunkInfo.chunkid();
        s3_request.offset = file_offset;
        s3_request.len = length;
        if (file_offset / block_size == s3_chunk_info_offset / block_size) {
          s3_request.objectOffset =
              s3_chunk_info_offset % chunk_size % block_size;
        } else {
          s3_request.objectOffset = 0;
        }
        s3_request.readOffset = buf_offset + read_offset;
        s3_request.compaction = s3ChunkInfo.compaction();
        s3_request.fsId = fsId;
        s3_request.inodeId = inodeId;
        requests->push_back(s3_request);
      }
      /*
                                ---------   read block
                              --------      S3ChunkInfo
      */
    } else {
      if (s3ChunkInfo.zero()) {
        memset(static_cast<char*>(dataBuf) + buf_offset + read_offset, 0,
               s3_chunk_info_offset + s3_chunk_info_len - file_offset);
      } else {
        s3_request.chunkId = s3ChunkInfo.chunkid();
        s3_request.offset = file_offset;
        s3_request.len = s3_chunk_info_offset + s3_chunk_info_len - file_offset;
        if (file_offset / block_size == s3_chunk_info_offset / block_size) {
          s3_request.objectOffset =
              s3_chunk_info_offset % chunk_size % block_size;
        } else {
          s3_request.objectOffset = 0;
        }
        s3_request.readOffset = buf_offset + read_offset;
        s3_request.compaction = s3ChunkInfo.compaction();
        s3_request.fsId = fsId;
        s3_request.inodeId = inodeId;
        requests->push_back(s3_request);
      }
      read_offset += s3_chunk_info_offset + s3_chunk_info_len - file_offset;
      ReadRequest split_request;
      split_request.index = request.index;
      split_request.chunkPos = request.chunkPos + s3_chunk_info_offset +
                               s3_chunk_info_len - file_offset;
      split_request.len =
          file_offset + length - (s3_chunk_info_offset + s3_chunk_info_len);
      split_request.bufOffset = buf_offset + read_offset;
      addReadRequests->emplace_back(split_request);
    }
    /*
                -----  read block
        ----           S3ChunkInfo
    do nothing
    */
  } else {
  }
}

void FileCacheManager::GenerateS3Request(ReadRequest request,
                                         const S3ChunkInfoList& s3ChunkInfoList,
                                         char* dataBuf,
                                         std::vector<S3ReadRequest>* requests,
                                         uint64_t fsId, uint64_t inodeId) {
  // first is chunkPos, user read request is split into multiple,
  // and emplace in the readRequests;
  std::map<uint64_t, ReadRequest> read_requests;

  VLOG(9) << "inodeId=" << inodeId
          << " GenerateS3Request start request chunkIndex:" << request.index
          << ", chunkPos:" << request.chunkPos << ", len:" << request.len
          << ", bufOffset:" << request.bufOffset;
  read_requests.emplace(request.chunkPos, request);
  for (int i = s3ChunkInfoList.s3chunks_size() - 1; i >= 0; i--) {
    const S3ChunkInfo& s3_chunk_info = s3ChunkInfoList.s3chunks(i);
    // readRequests is split by current s3ChunkInfo, emplace_back to the
    // addReadRequests
    std::vector<ReadRequest> add_read_requests;
    // if readRequest is split to one or two, old readRequest should be
    // delete,
    std::vector<uint64_t> deleting_req;
    for (auto& read_request : read_requests) {
      HandleReadRequest(read_request.second, s3_chunk_info, &add_read_requests,
                        &deleting_req, requests, dataBuf, fsId, inodeId);
    }

    for (unsigned long& iter : deleting_req) {
      read_requests.erase(iter);
    }

    for (auto& add_read_request : add_read_requests) {
      auto ret =
          read_requests.emplace(add_read_request.chunkPos, add_read_request);
      if (!ret.second) {
        LOG(ERROR) << "read request emplace failed. chunkPos:"
                   << add_read_request.chunkPos
                   << ", len:" << add_read_request.len
                   << ", index:" << add_read_request.index
                   << ",bufOffset:" << add_read_request.bufOffset;
      }
    }

    if (read_requests.empty()) {
      VLOG(6) << "inodeId=" << inodeId << " readRequests has hit s3ChunkInfos.";
      break;
    }
  }

  for (auto& read_request : read_requests) {
    VLOG(9) << "empty buf index:" << read_request.second.index
            << ", chunkPos:" << read_request.second.chunkPos
            << ", len:" << read_request.second.len
            << ", bufOffset:" << read_request.second.bufOffset;
    memset(dataBuf + read_request.second.bufOffset, 0, read_request.second.len);
  }

  auto s3_request_iter = requests->begin();
  for (; s3_request_iter != requests->end(); s3_request_iter++) {
    VLOG(9) << "s3Request chunkid:" << s3_request_iter->chunkId
            << ", offset:" << s3_request_iter->offset
            << ", len:" << s3_request_iter->len
            << ", objectOffset:" << s3_request_iter->objectOffset
            << ", readOffset:" << s3_request_iter->readOffset
            << ", fsid:" << s3_request_iter->fsId
            << ", inodeId=" << s3_request_iter->inodeId
            << ", compaction:" << s3_request_iter->compaction;
  }
}

void FileCacheManager::ReleaseCache() {
  WriteLockGuard write_lock_guard(rwLock_);

  uint64_t chunNum = chunkCacheMap_.size();
  for (auto& chunk : chunkCacheMap_) {
    chunk.second->ReleaseCache();
  }

  chunkCacheMap_.clear();
  g_s3MultiManagerMetric->chunkManagerNum << -1 * chunNum;
}

void FileCacheManager::TruncateCache(uint64_t offset, uint64_t fileSize) {
  uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
  uint64_t chunkIndex = offset / chunkSize;
  uint64_t chunkPos = offset % chunkSize;
  int chunkLen = 0;
  uint64_t truncateLen = fileSize - offset;
  //  Truncate processing according to chunk polling
  while (truncateLen > 0) {
    if (chunkPos + truncateLen > chunkSize) {
      chunkLen = chunkSize - chunkPos;
    } else {
      chunkLen = truncateLen;
    }
    ChunkCacheManagerPtr chunkCacheManager =
        FindOrCreateChunkCacheManager(chunkIndex);
    chunkCacheManager->TruncateCache(chunkPos);
    truncateLen -= chunkLen;
    chunkIndex++;
    chunkPos = (chunkPos + chunkLen) % chunkSize;
  }

  return;
}

DINGOFS_ERROR FileCacheManager::Flush(bool force, bool toS3) {
  (void)toS3;
  // Todo: concurrent flushes within one file
  // instead of multiple file flushes may be better
  DINGOFS_ERROR ret = DINGOFS_ERROR::OK;
  std::map<uint64_t, ChunkCacheManagerPtr> tmp;
  {
    WriteLockGuard write_lock_guard(rwLock_);
    tmp = chunkCacheMap_;
  }

  std::atomic<uint64_t> pendingReq(0);
  dingofs::utils::CountDownEvent cond(1);
  FlushChunkCacheCallBack cb =
      [&](const std::shared_ptr<FlushChunkCacheContext>& context) {
        ret = context->retCode;
        if (context->retCode != DINGOFS_ERROR::OK) {
          LOG(ERROR) << "fileCacheManager Flush error, ret:" << ret
                     << ", inodeId=" << context->inode << ", chunkIndex: "
                     << context->chunkCacheManptr->GetIndex();
          cond.Signal();
          return;
        }

        {
          WriteLockGuard write_lock_guard(rwLock_);
          auto iter1 =
              chunkCacheMap_.find(context->chunkCacheManptr->GetIndex());
          if (iter1 != chunkCacheMap_.end() && iter1->second->IsEmpty() &&
              (iter1->second.use_count() <= 3)) {
            // tmp、chunkCacheMap_ and context->chunkCacheManptr has
            // this ChunkCacheManagerPtr, so count is 3 if count more
            // than 3, this mean someone thread has this
            // ChunkCacheManagerPtr
            VLOG(6) << "ChunkCacheManagerPtr count:"
                    << context->chunkCacheManptr.use_count()
                    << ", inodeId=" << inode_
                    << ", index:" << context->chunkCacheManptr->GetIndex()
                    << " erase iter: " << iter1->first;
            chunkCacheMap_.erase(iter1);
            g_s3MultiManagerMetric->chunkManagerNum << -1;
          }
        }

        if (pendingReq.fetch_sub(1, std::memory_order_seq_cst) == 1) {
          VLOG(9) << "Finish flush pendingReq, inodeId=" << inode_;
          cond.Signal();
        }
      };

  std::vector<std::shared_ptr<FlushChunkCacheContext>> flush_tasks;
  auto iter = tmp.begin();
  VLOG(6) << "flush chunk cache num: " << tmp.size() << ", inodeId=" << inode_;

  for (; iter != tmp.end(); iter++) {
    auto context = std::make_shared<FlushChunkCacheContext>();
    context->inode = inode_;
    context->cb = cb;
    context->force = force;
    context->chunkCacheManptr = iter->second;
    flush_tasks.emplace_back(context);
  }

  pendingReq.fetch_add(flush_tasks.size(), std::memory_order_seq_cst);
  if (pendingReq.load(std::memory_order_seq_cst)) {
    VLOG(6) << "wait for pendingReq, inodeId=" << inode_;
    for (auto& flush_task : flush_tasks) {
      s3ClientAdaptor_->Enqueue(flush_task);
    }
    cond.Wait();
  }

  VLOG(6) << "Finish file cache flush, inodeId=" << inode_;
  return ret;
}

void ChunkCacheManager::ReadChunk(uint64_t index, uint64_t chunkPos,
                                  uint64_t readLen, char* dataBuf,
                                  uint64_t dataBufOffset,
                                  std::vector<ReadRequest>* requests) {
  (void)index;
  std::vector<ReadRequest> cache_miss_write_requests;
  std::vector<ReadRequest> cache_miss_flush_data_request;

  ReadLockGuard read_lock_guard(rwLockChunk_);
  // read by write cache
  ReadByWriteCache(chunkPos, readLen, dataBuf, dataBufOffset,
                   &cache_miss_write_requests);

  // read by flushing data cache
  flushingDataCacheMtx_.lock();
  if (!IsFlushDataEmpty()) {
    // read by flushing data cache
    for (auto request : cache_miss_write_requests) {
      std::vector<ReadRequest> tmp_requests;
      ReadByFlushData(request.chunkPos, request.len, dataBuf, request.bufOffset,
                      &tmp_requests);
      cache_miss_flush_data_request.insert(cache_miss_flush_data_request.end(),
                                           tmp_requests.begin(),
                                           tmp_requests.end());
    }
    flushingDataCacheMtx_.unlock();

    // read by read cache
    for (auto request : cache_miss_flush_data_request) {
      std::vector<ReadRequest> tmp_requests;
      ReadByReadCache(request.chunkPos, request.len, dataBuf, request.bufOffset,
                      &tmp_requests);
      requests->insert(requests->end(), tmp_requests.begin(),
                       tmp_requests.end());
    }
    return;
  }
  flushingDataCacheMtx_.unlock();

  // read by read cache
  for (auto request : cache_miss_write_requests) {
    std::vector<ReadRequest> tmp_requests;
    ReadByReadCache(request.chunkPos, request.len, dataBuf, request.bufOffset,
                    &tmp_requests);
    requests->insert(requests->end(), tmp_requests.begin(), tmp_requests.end());
  }
}

void ChunkCacheManager::ReadByWriteCache(uint64_t chunkPos, uint64_t readLen,
                                         char* dataBuf, uint64_t dataBufOffset,
                                         std::vector<ReadRequest>* requests) {
  ReadLockGuard readLockGuard(rwLockWrite_);

  VLOG(6) << "Try to ReadByWriteCache chunkPos:" << chunkPos
          << ", readLen:" << readLen << ", dataBufOffset:" << dataBufOffset;
  if (dataWCacheMap_.empty()) {
    VLOG(9) << "dataWCacheMap_ is empty";
    ReadRequest request;
    request.index = index_;
    request.len = readLen;
    request.chunkPos = chunkPos;
    request.bufOffset = dataBufOffset;
    requests->emplace_back(request);
    return;
  }

  auto iter = dataWCacheMap_.upper_bound(chunkPos);
  if (iter != dataWCacheMap_.begin()) {
    --iter;
  }

  for (; iter != dataWCacheMap_.end(); iter++) {
    ReadRequest request;
    uint64_t dcChunkPos = iter->second->GetChunkPos();
    uint64_t dcLen = iter->second->GetLen();
    VLOG(6) << "ReadByWriteCache chunkPos:" << chunkPos
            << ", readLen:" << readLen << ", dcChunkPos:" << dcChunkPos
            << ", dcLen:" << dcLen << ", first:" << iter->first;
    assert(iter->first == iter->second->GetChunkPos());
    if (chunkPos + readLen <= dcChunkPos) {
      break;
    } else if ((chunkPos + readLen > dcChunkPos) && (chunkPos < dcChunkPos)) {
      request.len = dcChunkPos - chunkPos;
      request.chunkPos = chunkPos;
      request.index = index_;
      request.bufOffset = dataBufOffset;
      VLOG(6) << "request: chunkIndex:" << index_ << ", chunkPos:" << chunkPos
              << ", len:" << request.len << ", bufOffset:" << dataBufOffset;
      requests->emplace_back(request);
      /*
           -----               ReadData
              ------           DataCache
      */
      if (chunkPos + readLen <= dcChunkPos + dcLen) {
        iter->second->CopyDataCacheToBuf(0, chunkPos + readLen - dcChunkPos,
                                         dataBuf + request.len + dataBufOffset);
        readLen = 0;
        break;
        /*
             -----------         ReadData
                ------           DataCache
        */
      } else {
        iter->second->CopyDataCacheToBuf(0, dcLen,
                                         dataBuf + request.len + dataBufOffset);
        readLen = chunkPos + readLen - (dcChunkPos + dcLen);
        dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
        chunkPos = dcChunkPos + dcLen;
      }
    } else if ((chunkPos >= dcChunkPos) && (chunkPos < dcChunkPos + dcLen)) {
      /*
               ----              ReadData
             ---------           DataCache
      */
      if (chunkPos + readLen <= dcChunkPos + dcLen) {
        iter->second->CopyDataCacheToBuf(chunkPos - dcChunkPos, readLen,
                                         dataBuf + dataBufOffset);
        readLen = 0;
        break;
        /*
                 ----------              ReadData
               ---------                DataCache
        */
      } else {
        iter->second->CopyDataCacheToBuf(chunkPos - dcChunkPos,
                                         dcChunkPos + dcLen - chunkPos,
                                         dataBuf + dataBufOffset);
        readLen = chunkPos + readLen - dcChunkPos - dcLen;
        dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
        chunkPos = dcChunkPos + dcLen;
      }
    } else {
      continue;
    }
  }

  if (readLen > 0) {
    ReadRequest request;
    request.index = index_;
    request.len = readLen;
    request.chunkPos = chunkPos;
    request.bufOffset = dataBufOffset;
    requests->emplace_back(request);
  }
}

void ChunkCacheManager::ReadByReadCache(uint64_t chunkPos, uint64_t readLen,
                                        char* dataBuf, uint64_t dataBufOffset,
                                        std::vector<ReadRequest>* requests) {
  ReadLockGuard readLockGuard(rwLockRead_);

  VLOG(9) << "Try to ReadByReadCache chunkPos:" << chunkPos
          << ", readLen:" << readLen << ", dataBufOffset:" << dataBufOffset;
  if (dataRCacheMap_.empty()) {
    VLOG(9) << "dataRCacheMap_ is empty";
    ReadRequest request;
    request.index = index_;
    request.len = readLen;
    request.chunkPos = chunkPos;
    request.bufOffset = dataBufOffset;
    requests->emplace_back(request);
    return;
  }

  auto iter = dataRCacheMap_.upper_bound(chunkPos);
  if (iter != dataRCacheMap_.begin()) {
    --iter;
  }

  for (; iter != dataRCacheMap_.end(); ++iter) {
    DataCachePtr& dataCache = (*iter->second);
    ReadRequest request;
    uint64_t dcChunkPos = dataCache->GetChunkPos();
    uint64_t dcLen = dataCache->GetLen();

    VLOG(9) << "ReadByReadCache chunkPos:" << chunkPos
            << ", readLen:" << readLen << ", dcChunkPos:" << dcChunkPos
            << ", dcLen:" << dcLen << ", dataBufOffset:" << dataBufOffset;
    if (chunkPos + readLen <= dcChunkPos) {
      break;
    } else if ((chunkPos + readLen > dcChunkPos) && (chunkPos < dcChunkPos)) {
      s3ClientAdaptor_->GetFsCacheManager()->Get(iter->second);
      request.len = dcChunkPos - chunkPos;
      request.chunkPos = chunkPos;
      request.index = index_;
      request.bufOffset = dataBufOffset;
      VLOG(9) << "request: index:" << index_ << ", chunkPos:" << chunkPos
              << ", len:" << request.len << ", bufOffset:" << dataBufOffset;
      requests->emplace_back(request);
      /*
           -----               ReadData
              ------           DataCache
      */
      if (chunkPos + readLen <= dcChunkPos + dcLen) {
        dataCache->CopyDataCacheToBuf(0, chunkPos + readLen - dcChunkPos,
                                      dataBuf + request.len + dataBufOffset);
        readLen = 0;
        break;
        /*
             -----------         ReadData
                ------           DataCache
        */
      } else {
        dataCache->CopyDataCacheToBuf(0, dcLen,
                                      dataBuf + request.len + dataBufOffset);
        readLen = chunkPos + readLen - (dcChunkPos + dcLen);
        dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
        chunkPos = dcChunkPos + dcLen;
      }
    } else if ((chunkPos >= dcChunkPos) && (chunkPos < dcChunkPos + dcLen)) {
      s3ClientAdaptor_->GetFsCacheManager()->Get(iter->second);
      /*
               ----              ReadData
             ---------           DataCache
      */
      if (chunkPos + readLen <= dcChunkPos + dcLen) {
        dataCache->CopyDataCacheToBuf(chunkPos - dcChunkPos, readLen,
                                      dataBuf + dataBufOffset);
        readLen = 0;
        break;
        /*
                 ----------              ReadData
               ---------                DataCache
        */
      } else {
        dataCache->CopyDataCacheToBuf(chunkPos - dcChunkPos,
                                      dcChunkPos + dcLen - chunkPos,
                                      dataBuf + dataBufOffset);
        readLen = chunkPos + readLen - dcChunkPos - dcLen;
        dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
        chunkPos = dcChunkPos + dcLen;
      }
    }
  }

  if (readLen > 0) {
    ReadRequest request;
    request.index = index_;
    request.len = readLen;
    request.chunkPos = chunkPos;
    request.bufOffset = dataBufOffset;
    VLOG(9) << "request: index:" << index_ << ", chunkPos:" << chunkPos
            << ", len:" << request.len << ", bufOffset:" << dataBufOffset;
    requests->emplace_back(request);
  }
  return;
}

void ChunkCacheManager::ReadByFlushData(uint64_t chunkPos, uint64_t readLen,
                                        char* dataBuf, uint64_t dataBufOffset,
                                        std::vector<ReadRequest>* requests) {
  uint64_t dcChunkPos = flushingDataCache_->GetChunkPos();
  uint64_t dcLen = flushingDataCache_->GetLen();
  ReadRequest request;
  VLOG(9) << "Try to ReadByFlushData chunkPos: " << chunkPos
          << ", readLen: " << readLen << ", dcChunkPos: " << dcChunkPos
          << ", dcLen: " << dcLen;
  if (chunkPos + readLen <= dcChunkPos) {
    request.index = index_;
    request.len = readLen;
    request.chunkPos = chunkPos;
    request.bufOffset = dataBufOffset;
    requests->emplace_back(request);
    return;
  } else if ((chunkPos + readLen > dcChunkPos) && (chunkPos < dcChunkPos)) {
    request.len = dcChunkPos - chunkPos;
    request.chunkPos = chunkPos;
    request.index = index_;
    request.bufOffset = dataBufOffset;
    VLOG(6) << "request: index:" << index_ << ", chunkPos:" << chunkPos
            << ", len:" << request.len << ", bufOffset:" << dataBufOffset;
    requests->emplace_back(request);
    /*
         -----               ReadData
            ------           DataCache
    */
    if (chunkPos + readLen <= dcChunkPos + dcLen) {
      flushingDataCache_->CopyDataCacheToBuf(
          0, chunkPos + readLen - dcChunkPos,
          dataBuf + request.len + dataBufOffset);
      readLen = 0;
      return;
      /*
           -----------         ReadData
              ------           DataCache
      */
    } else {
      flushingDataCache_->CopyDataCacheToBuf(
          0, dcLen, dataBuf + request.len + dataBufOffset);
      readLen = chunkPos + readLen - (dcChunkPos + dcLen);
      dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
      chunkPos = dcChunkPos + dcLen;
    }
  } else if ((chunkPos >= dcChunkPos) && (chunkPos < dcChunkPos + dcLen)) {
    /*
             ----              ReadData
           ---------           DataCache
    */
    if (chunkPos + readLen <= dcChunkPos + dcLen) {
      flushingDataCache_->CopyDataCacheToBuf(chunkPos - dcChunkPos, readLen,
                                             dataBuf + dataBufOffset);
      readLen = 0;
      return;
      /*
               ----------              ReadData
             ---------                DataCache
      */
    } else {
      flushingDataCache_->CopyDataCacheToBuf(chunkPos - dcChunkPos,
                                             dcChunkPos + dcLen - chunkPos,
                                             dataBuf + dataBufOffset);
      readLen = chunkPos + readLen - dcChunkPos - dcLen;
      dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
      chunkPos = dcChunkPos + dcLen;
    }
  }

  if (readLen > 0) {
    request.index = index_;
    request.len = readLen;
    request.chunkPos = chunkPos;
    request.bufOffset = dataBufOffset;
    requests->emplace_back(request);
  }
}

DataCachePtr ChunkCacheManager::FindWriteableDataCache(
    uint64_t chunkPos, uint64_t len,
    std::vector<DataCachePtr>* mergeDataCacheVer, uint64_t inodeId) {
  WriteLockGuard writeLockGuard(rwLockWrite_);

  auto iter = dataWCacheMap_.upper_bound(chunkPos);
  if (iter != dataWCacheMap_.begin()) {
    --iter;
  }
  for (; iter != dataWCacheMap_.end(); iter++) {
    VLOG(12) << "FindWriteableDataCache chunkPos:"
             << iter->second->GetChunkPos()
             << ", len:" << iter->second->GetLen() << ", inodeId=" << inodeId
             << ", chunkIndex:" << index_;
    assert(iter->first == iter->second->GetChunkPos());
    if (((chunkPos + len) >= iter->second->GetChunkPos()) &&
        (chunkPos <= iter->second->GetChunkPos() + iter->second->GetLen())) {
      DataCachePtr dataCache = iter->second;
      std::vector<uint64_t> waitDelVec;
      while (1) {
        iter++;
        if (iter == dataWCacheMap_.end()) {
          break;
        }
        if ((chunkPos + len) < iter->second->GetChunkPos()) {
          break;
        }

        mergeDataCacheVer->emplace_back(iter->second);
        waitDelVec.push_back(iter->first);
      }

      std::vector<uint64_t>::iterator iterDel = waitDelVec.begin();
      for (; iterDel != waitDelVec.end(); iterDel++) {
        auto iter = dataWCacheMap_.find(*iterDel);
        VLOG(9) << "delete data cache chunkPos:" << iter->second->GetChunkPos()
                << ", len:" << iter->second->GetLen() << ", inodeId=" << inodeId
                << ", chunkIndex:" << index_;
        s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
        VLOG(9) << "FindWriteableDataCache() DataCacheByteDec1 len:"
                << iter->second->GetLen();
        s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(
            iter->second->GetActualLen());
        dataWCacheMap_.erase(iter);
      }
      return dataCache;
    }
  }
  return nullptr;
}

void ChunkCacheManager::WriteNewDataCache(S3ClientAdaptorImpl* s3ClientAdaptor,
                                          uint32_t chunkPos, uint32_t len,
                                          const char* data) {
  DataCachePtr data_cache =
      std::make_shared<DataCache>(s3ClientAdaptor, this->shared_from_this(),
                                  chunkPos, len, data, kvClientManager_);
  VLOG(9) << "WriteNewDataCache chunkPos:" << chunkPos << ", len:" << len
          << ", new len:" << data_cache->GetLen() << ", chunkIndex:" << index_;
  WriteLockGuard write_lock_guard(rwLockWrite_);
  auto ret = dataWCacheMap_.emplace(chunkPos, data_cache);
  if (!ret.second) {
    LOG(ERROR) << "dataCache emplace failed.";
    return;
  }

  s3ClientAdaptor_->FsSyncSignalAndDataCacheInc();
  s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(
      data_cache->GetActualLen());
}

// TODO: remove read cache becase read performance is degraded compare to cto
void ChunkCacheManager::AddReadDataCache(DataCachePtr dataCache) {
  uint64_t chunkPos = dataCache->GetChunkPos();
  uint64_t len = dataCache->GetLen();
  WriteLockGuard writeLockGuard(rwLockRead_);
  std::vector<uint64_t> deleteKeyVec;
  auto iter = dataRCacheMap_.begin();
  for (; iter != dataRCacheMap_.end(); iter++) {
    if (chunkPos + len <= iter->first) {
      break;
    }
    std::list<DataCachePtr>::iterator dcpIter = iter->second;
    uint64_t dcChunkPos = (*dcpIter)->GetChunkPos();
    uint64_t dcLen = (*dcpIter)->GetLen();
    if ((chunkPos + len > dcChunkPos) && (chunkPos < dcChunkPos + dcLen)) {
      VLOG(9) << "read cache chunkPos:" << chunkPos << ", len:" << len
              << "is overlap with datacache chunkPos:" << dcChunkPos
              << ", len:" << dcLen << ", index:" << index_;
      deleteKeyVec.emplace_back(dcChunkPos);
    }
  }
  for (auto key : deleteKeyVec) {
    auto iter = dataRCacheMap_.find(key);
    std::list<DataCachePtr>::iterator dcpIter = iter->second;
    uint64_t actualLen = (*dcpIter)->GetActualLen();
    if (s3ClientAdaptor_->GetFsCacheManager()->Delete(dcpIter)) {
      g_s3MultiManagerMetric->readDataCacheNum << -1;
      g_s3MultiManagerMetric->readDataCacheByte << -1 * actualLen;
      dataRCacheMap_.erase(iter);
    }
  }
  std::list<DataCachePtr>::iterator outIter;
  bool ret = s3ClientAdaptor_->GetFsCacheManager()->Set(dataCache, &outIter);
  if (ret) {
    g_s3MultiManagerMetric->readDataCacheNum << 1;
    g_s3MultiManagerMetric->readDataCacheByte << dataCache->GetActualLen();
    dataRCacheMap_.emplace(chunkPos, outIter);
  }
}

void ChunkCacheManager::ReleaseReadDataCache(uint64_t key) {
  WriteLockGuard writeLockGuard(rwLockRead_);

  auto iter = dataRCacheMap_.find(key);
  if (iter == dataRCacheMap_.end()) {
    return;
  }
  g_s3MultiManagerMetric->readDataCacheNum << -1;
  g_s3MultiManagerMetric->readDataCacheByte
      << -1 * (*(iter->second))->GetActualLen();
  dataRCacheMap_.erase(iter);
}

void ChunkCacheManager::ReleaseCache() {
  {
    WriteLockGuard writeLockGuard(rwLockWrite_);

    for (auto& dataWCache : dataWCacheMap_) {
      s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
      s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(
          dataWCache.second->GetActualLen());
    }
    dataWCacheMap_.clear();
  }
  WriteLockGuard writeLockGuard(rwLockRead_);
  auto iter = dataRCacheMap_.begin();
  for (; iter != dataRCacheMap_.end(); iter++) {
    if (s3ClientAdaptor_->GetFsCacheManager()->Delete(iter->second)) {
      g_s3MultiManagerMetric->readDataCacheNum << -1;
      g_s3MultiManagerMetric->readDataCacheByte
          << -1 * (*(iter->second))->GetActualLen();
      dataRCacheMap_.erase(iter);
    }
  }
}

void ChunkCacheManager::TruncateCache(uint64_t chunkPos) {
  WriteLockGuard writeLockGuard(rwLockChunk_);

  TruncateWriteCache(chunkPos);
  TruncateReadCache(chunkPos);
}

void ChunkCacheManager::TruncateWriteCache(uint64_t chunkPos) {
  WriteLockGuard writeLockGuard(rwLockWrite_);
  auto rIter = dataWCacheMap_.rbegin();
  for (; rIter != dataWCacheMap_.rend();) {
    uint64_t dcChunkPos = rIter->second->GetChunkPos();
    uint64_t dcLen = rIter->second->GetLen();
    uint64_t dcActualLen = rIter->second->GetActualLen();
    if (dcChunkPos >= chunkPos) {
      s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
      s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(dcActualLen);
      dataWCacheMap_.erase(next(rIter).base());
    } else if ((dcChunkPos < chunkPos) && ((dcChunkPos + dcLen) > chunkPos)) {
      rIter->second->Truncate(chunkPos - dcChunkPos);
      s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(
          dcActualLen - rIter->second->GetActualLen());
      break;
    } else {
      break;
    }
  }
}

void ChunkCacheManager::TruncateReadCache(uint64_t chunkPos) {
  WriteLockGuard writeLockGuard(rwLockRead_);
  auto rIter = dataRCacheMap_.rbegin();
  for (; rIter != dataRCacheMap_.rend();) {
    uint64_t dcChunkPos = (*rIter->second)->GetChunkPos();
    uint64_t dcLen = (*rIter->second)->GetLen();
    uint64_t dcActualLen = (*rIter->second)->GetActualLen();
    if ((dcChunkPos + dcLen) > chunkPos) {
      if (s3ClientAdaptor_->GetFsCacheManager()->Delete(rIter->second)) {
        g_s3MultiManagerMetric->readDataCacheNum << -1;
        g_s3MultiManagerMetric->readDataCacheByte << -1 * dcActualLen;
        dataRCacheMap_.erase(next(rIter).base());
      }
    } else {
      break;
    }
  }
}

void ChunkCacheManager::ReleaseWriteDataCache(const DataCachePtr& dataCache) {
  s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
  VLOG(9) << "chunk flush DataCacheByteDec len:" << dataCache->GetActualLen();
  s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(
      dataCache->GetActualLen());
}

DINGOFS_ERROR ChunkCacheManager::Flush(uint64_t inodeId, bool force,
                                       bool toS3) {
  std::map<uint64_t, DataCachePtr> tmp;
  dingofs::utils::LockGuard lg(flushMtx_);
  DINGOFS_ERROR ret = DINGOFS_ERROR::OK;
  // DataCachePtr dataCache;
  while (1) {
    bool isFlush = false;
    {
      WriteLockGuard writeLockGuard(rwLockChunk_);

      auto iter = dataWCacheMap_.begin();
      while (iter != dataWCacheMap_.end()) {
        if (iter->second->CanFlush(force)) {
          {
            dingofs::utils::LockGuard lg(flushingDataCacheMtx_);
            flushingDataCache_ = std::move(iter->second);
          }
          dataWCacheMap_.erase(iter);
          isFlush = true;
          break;
        } else {
          iter++;
        }
      }
    }
    if (isFlush) {
      VLOG(9) << "Flush datacache chunkPos:"
              << flushingDataCache_->GetChunkPos()
              << ", len:" << flushingDataCache_->GetLen()
              << ", inodeId=" << inodeId << ", chunkIndex:" << index_;
      assert(flushingDataCache_->IsDirty());
      do {
        ret = flushingDataCache_->Flush(inodeId, toS3);
        if (ret == DINGOFS_ERROR::NOTEXIST) {
          LOG(WARNING) << "dataCache flush failed. ret:" << ret
                       << ", index:" << index_ << ", data chunkpos:"
                       << flushingDataCache_->GetChunkPos();
          ReleaseWriteDataCache(flushingDataCache_);
          break;
        } else if (ret == DINGOFS_ERROR::INTERNAL) {
          LOG(WARNING) << "dataCache flush failed. ret:" << ret
                       << ", index:" << index_ << ", data chunkpos:"
                       << flushingDataCache_->GetChunkPos()
                       << ", should retry.";
          ::sleep(3);
          continue;
        }
        VLOG(9) << "ReleaseWriteDataCache chunkPos:"
                << flushingDataCache_->GetChunkPos()
                << ", len:" << flushingDataCache_->GetLen()
                << ", inodeId=" << inodeId << ", chunkIndex:" << index_;
        ReleaseWriteDataCache(flushingDataCache_);
      } while (ret != DINGOFS_ERROR::OK);
      {
        dingofs::utils::LockGuard lg(flushingDataCacheMtx_);
        flushingDataCache_ = nullptr;
      }
    } else {
      VLOG(9) << "can not find flush datacache, inodeId=" << inodeId
              << ", chunkIndex:" << index_;
      break;
    }
  }
  return DINGOFS_ERROR::OK;
}

void ChunkCacheManager::UpdateWriteCacheMap(uint64_t oldChunkPos,
                                            DataCache* pDataCache) {
  auto iter = dataWCacheMap_.find(oldChunkPos);
  DataCachePtr datacache;
  if (iter != dataWCacheMap_.end()) {
    datacache = iter->second;
    dataWCacheMap_.erase(iter);
  } else {
    datacache = pDataCache->shared_from_this();
  }
  auto ret = dataWCacheMap_.emplace(datacache->GetChunkPos(), datacache);
  assert(ret.second);
  (void)ret;
}

void ChunkCacheManager::AddWriteDataCacheForTest(DataCachePtr dataCache) {
  WriteLockGuard writeLockGuard(rwLockWrite_);

  dataWCacheMap_.emplace(dataCache->GetChunkPos(), dataCache);
  s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumInc();
  s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(
      dataCache->GetActualLen());
}

DataCache::DataCache(S3ClientAdaptorImpl* s3ClientAdaptor,
                     ChunkCacheManagerPtr chunkCacheManager, uint64_t chunkPos,
                     uint64_t len, const char* data,
                     std::shared_ptr<KVClientManager> kvClientManager)
    : s3ClientAdaptor_(std::move(s3ClientAdaptor)),
      chunkCacheManager_(chunkCacheManager),
      status_(DataCacheStatus::Dirty),
      inReadCache_(false) {
  uint64_t blockSize = s3ClientAdaptor->GetBlockSize();
  uint32_t pageSize = s3ClientAdaptor->GetPageSize();
  chunkPos_ = chunkPos;
  len_ = len;
  actualChunkPos_ = chunkPos - chunkPos % pageSize;

  uint64_t headZeroLen = chunkPos - actualChunkPos_;
  uint64_t blockIndex = chunkPos / blockSize;
  uint64_t blockPos = chunkPos % blockSize;
  uint64_t pageIndex, pagePos;
  uint64_t n, m, blockLen;
  uint64_t dataOffset = 0;
  uint64_t tailZeroLen = 0;

  while (len > 0) {
    if (blockPos + len > blockSize) {
      n = blockSize - blockPos;
    } else {
      n = len;
    }
    PageDataMap& pdMap = dataMap_[blockIndex];
    blockLen = n;
    pageIndex = blockPos / pageSize;
    pagePos = blockPos % pageSize;
    while (blockLen > 0) {
      if (pagePos + blockLen > pageSize) {
        m = pageSize - pagePos;
      } else {
        m = blockLen;
      }

      PageData* pageData = new PageData();
      pageData->data = DataStream::GetInstance().NewPage();
      memcpy(pageData->data + pagePos, data + dataOffset, m);
      if (pagePos + m < pageSize) {
        tailZeroLen = pageSize - pagePos - m;
      }
      pageData->index = pageIndex;
      assert(pdMap.count(pageIndex) == 0);
      pdMap.emplace(pageIndex, pageData);
      pageIndex++;
      blockLen -= m;
      dataOffset += m;
      pagePos = (pagePos + m) % pageSize;
    }

    blockIndex++;
    len -= n;
    blockPos = (blockPos + n) % blockSize;
  }
  actualLen_ = headZeroLen + len_ + tailZeroLen;
  assert((actualLen_ % pageSize) == 0);
  assert((actualChunkPos_ % pageSize) == 0);
  createTime_ = ::dingofs::utils::TimeUtility::GetTimeofDaySec();

  kvClientManager_ = std::move(kvClientManager);
}

void DataCache::CopyBufToDataCache(uint64_t dataCachePos, uint64_t len,
                                   const char* data) {
  uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
  uint32_t pageSize = s3ClientAdaptor_->GetPageSize();
  uint64_t pos = chunkPos_ + dataCachePos;
  uint64_t blockIndex = pos / blockSize;
  uint64_t blockPos = pos % blockSize;
  uint64_t pageIndex, pagePos;
  uint64_t n, blockLen, m;
  uint64_t dataOffset = 0;
  uint64_t addLen = 0;

  VLOG(9) << "CopyBufToDataCache() dataCachePos:" << dataCachePos
          << ", len:" << len << ", chunkPos_:" << chunkPos_
          << ", len_:" << len_;
  if (dataCachePos + len > len_) {
    len_ = dataCachePos + len;
  }
  while (len > 0) {
    if (blockPos + len > blockSize) {
      n = blockSize - blockPos;
    } else {
      n = len;
    }
    blockLen = n;
    PageDataMap& pdMap = dataMap_[blockIndex];
    PageData* pageData;
    pageIndex = blockPos / pageSize;
    pagePos = blockPos % pageSize;
    while (blockLen > 0) {
      if (pagePos + blockLen > pageSize) {
        m = pageSize - pagePos;
      } else {
        m = blockLen;
      }
      if (pdMap.count(pageIndex)) {
        pageData = pdMap[pageIndex];
      } else {
        pageData = new PageData();
        pageData->data = DataStream::GetInstance().NewPage();
        pageData->index = pageIndex;
        pdMap.emplace(pageIndex, pageData);
        addLen += pageSize;
      }
      memcpy(pageData->data + pagePos, data + dataOffset, m);
      pageIndex++;
      blockLen -= m;
      dataOffset += m;
      pagePos = (pagePos + m) % pageSize;
    }

    blockIndex++;
    len -= n;
    blockPos = (blockPos + n) % blockSize;
  }
  actualLen_ += addLen;
  VLOG(9) << "chunkPos:" << chunkPos_ << ", len:" << len_
          << ", actualChunkPos_:" << actualChunkPos_
          << ", actualLen:" << actualLen_;
}

void DataCache::AddDataBefore(uint64_t len, const char* data) {
  uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
  uint32_t pageSize = s3ClientAdaptor_->GetPageSize();
  uint64_t tmpLen = len;
  uint64_t newChunkPos = chunkPos_ - len;
  uint64_t blockIndex = newChunkPos / blockSize;
  uint64_t blockPos = newChunkPos % blockSize;
  uint64_t pageIndex, pagePos;
  uint64_t n, m, blockLen;
  uint64_t dataOffset = 0;

  VLOG(9) << "AddDataBefore() len:" << len << ", len_:" << len_
          << "chunkPos:" << chunkPos_ << ", actualChunkPos:" << actualChunkPos_
          << ", len:" << len_ << ", actualLen:" << actualLen_;
  while (tmpLen > 0) {
    if (blockPos + tmpLen > blockSize) {
      n = blockSize - blockPos;
    } else {
      n = tmpLen;
    }

    PageDataMap& pdMap = dataMap_[blockIndex];
    blockLen = n;
    PageData* pageData = NULL;
    pageIndex = blockPos / pageSize;
    pagePos = blockPos % pageSize;
    while (blockLen > 0) {
      if (pagePos + blockLen > pageSize) {
        m = pageSize - pagePos;
      } else {
        m = blockLen;
      }

      if (pdMap.count(pageIndex)) {
        pageData = pdMap[pageIndex];
      } else {
        pageData = new PageData();
        pageData->data = DataStream::GetInstance().NewPage();
        pageData->index = pageIndex;
        pdMap.emplace(pageIndex, pageData);
      }
      memcpy(pageData->data + pagePos, data + dataOffset, m);
      pageIndex++;
      blockLen -= m;
      dataOffset += m;
      pagePos = (pagePos + m) % pageSize;
    }
    blockIndex++;
    tmpLen -= n;
    blockPos = (blockPos + n) % blockSize;
  }
  chunkPos_ = newChunkPos;
  actualChunkPos_ = chunkPos_ - chunkPos_ % pageSize;
  len_ += len;
  if ((chunkPos_ + len_ - actualChunkPos_) % pageSize == 0) {
    actualLen_ = chunkPos_ + len_ - actualChunkPos_;
  } else {
    actualLen_ =
        ((chunkPos_ + len_ - actualChunkPos_) / pageSize + 1) * pageSize;
  }
  VLOG(9) << "chunkPos:" << chunkPos_ << ", len:" << len_
          << ", actualChunkPos_:" << actualChunkPos_
          << ", actualLen:" << actualLen_;
}

void DataCache::MergeDataCacheToDataCache(DataCachePtr mergeDataCache,
                                          uint64_t dataOffset, uint64_t len) {
  uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
  uint32_t pageSize = s3ClientAdaptor_->GetPageSize();
  uint64_t maxPageInBlock = blockSize / pageSize;
  uint64_t chunkPos = mergeDataCache->GetChunkPos() + dataOffset;
  assert(chunkPos == (chunkPos_ + len_));
  uint64_t blockIndex = chunkPos / blockSize;
  uint64_t blockPos = chunkPos % blockSize;
  uint64_t pageIndex = blockPos / pageSize;
  uint64_t pagePos = blockPos % pageSize;
  char* data = nullptr;
  PageData* mergePage = nullptr;
  PageDataMap* pdMap = &dataMap_[blockIndex];
  uint64_t n = 0;

  VLOG(9) << "MergeDataCacheToDataCache dataOffset:" << dataOffset
          << ", len:" << len << ",dataCache chunkPos:" << chunkPos_
          << ", len:" << len_
          << " mergeData chunkPos:" << mergeDataCache->GetChunkPos()
          << ", len:" << mergeDataCache->GetLen();
  assert((dataOffset + len) == mergeDataCache->GetLen());
  len_ += len;
  while (len > 0) {
    if (pageIndex == maxPageInBlock) {
      blockIndex++;
      pageIndex = 0;
      pdMap = &dataMap_[blockIndex];
    }
    mergePage = mergeDataCache->GetPageData(blockIndex, pageIndex);
    assert(mergePage);
    if (pdMap->count(pageIndex)) {
      data = (*pdMap)[pageIndex]->data;
      if (pagePos + len > pageSize) {
        n = pageSize - pagePos;
      } else {
        n = len;
      }
      VLOG(9) << "MergeDataCacheToDataCache n:" << n << ", pagePos:" << pagePos;
      memcpy(data + pagePos, mergePage->data + pagePos, n);
      // mergeDataCache->ReleasePageData(blockIndex, pageIndex);
    } else {
      pdMap->emplace(pageIndex, mergePage);
      mergeDataCache->ErasePageData(blockIndex, pageIndex);
      n = pageSize;
      actualLen_ += pageSize;
      VLOG(9) << "MergeDataCacheToDataCache n:" << n;
    }

    if (len >= n) {
      len -= n;
    } else {
      len = 0;
    }
    pageIndex++;
    pagePos = 0;
  }
  VLOG(9) << "MergeDataCacheToDataCache end chunkPos:" << chunkPos_
          << ", len:" << len_ << ", actualChunkPos:" << actualChunkPos_
          << ",  actualLen:" << actualLen_;
  return;
}

void DataCache::Write(uint64_t chunkPos, uint64_t len, const char* data,
                      const std::vector<DataCachePtr>& mergeDataCacheVer) {
  uint64_t addByte = 0;
  uint64_t oldSize = 0;
  VLOG(9) << "DataCache Write() chunkPos:" << chunkPos << ", len:" << len
          << ", dataCache's chunkPos:" << chunkPos_
          << ", actualChunkPos:" << actualChunkPos_
          << ", dataCache's len:" << len_ << ", actualLen:" << actualLen_;
  auto iter = mergeDataCacheVer.begin();
  for (; iter != mergeDataCacheVer.end(); iter++) {
    VLOG(9) << "mergeDataCacheVer chunkPos:" << (*iter)->GetChunkPos()
            << ", len:" << (*iter)->GetLen();
  }
  dingofs::utils::LockGuard lg(mtx_);
  status_.store(DataCacheStatus::Dirty, std::memory_order_release);
  uint64_t oldChunkPos = chunkPos_;
  if (chunkPos <= chunkPos_) {
    /*
        ------       DataCache
     -------         WriteData
    */
    if (chunkPos + len <= chunkPos_ + len_) {
      chunkCacheManager_->rwLockWrite_.WRLock();
      oldSize = actualLen_;
      CopyBufToDataCache(0, chunkPos + len - chunkPos_,
                         data + chunkPos_ - chunkPos);
      AddDataBefore(chunkPos_ - chunkPos, data);
      addByte = actualLen_ - oldSize;
      s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(addByte);
      chunkCacheManager_->UpdateWriteCacheMap(oldChunkPos, this);
      chunkCacheManager_->rwLockWrite_.Unlock();
      return;
    } else {
      std::vector<DataCachePtr>::const_iterator iter =
          mergeDataCacheVer.begin();
      for (; iter != mergeDataCacheVer.end(); iter++) {
        /*
             ------         ------    DataCache
          ---------------------       WriteData
        */
        if (chunkPos + len < (*iter)->GetChunkPos() + (*iter)->GetLen()) {
          chunkCacheManager_->rwLockWrite_.WRLock();
          oldSize = actualLen_;
          CopyBufToDataCache(0, chunkPos + len - chunkPos_,
                             data + chunkPos_ - chunkPos);
          MergeDataCacheToDataCache(
              (*iter), chunkPos + len - (*iter)->GetChunkPos(),
              (*iter)->GetChunkPos() + (*iter)->GetLen() - chunkPos - len);
          AddDataBefore(chunkPos_ - chunkPos, data);
          addByte = actualLen_ - oldSize;
          s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(addByte);
          chunkCacheManager_->UpdateWriteCacheMap(oldChunkPos, this);
          chunkCacheManager_->rwLockWrite_.Unlock();
          return;
        }
      }
      /*
               ------    ------         DataCache
            ---------------------       WriteData
      */
      chunkCacheManager_->rwLockWrite_.WRLock();
      oldSize = actualLen_;
      CopyBufToDataCache(0, chunkPos + len - chunkPos_,
                         data + chunkPos_ - chunkPos);
      AddDataBefore(chunkPos_ - chunkPos, data);
      addByte = actualLen_ - oldSize;
      s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(addByte);
      chunkCacheManager_->UpdateWriteCacheMap(oldChunkPos, this);
      chunkCacheManager_->rwLockWrite_.Unlock();
      return;
    }
  } else {
    /*
        --------       DataCache
         -----         WriteData
    */
    if (chunkPos + len <= chunkPos_ + len_) {
      CopyBufToDataCache(chunkPos - chunkPos_, len, data);
      return;
    } else {
      std::vector<DataCachePtr>::const_iterator iter =
          mergeDataCacheVer.begin();
      for (; iter != mergeDataCacheVer.end(); iter++) {
        /*
             ------         ------    DataCache
                ----------------       WriteData
        */
        if (chunkPos + len < (*iter)->GetChunkPos() + (*iter)->GetLen()) {
          oldSize = actualLen_;

          CopyBufToDataCache(chunkPos - chunkPos_, len, data);
          VLOG(9) << "databuf offset:"
                  << chunkPos + len - (*iter)->GetChunkPos();
          MergeDataCacheToDataCache(
              (*iter), chunkPos + len - (*iter)->GetChunkPos(),
              (*iter)->GetChunkPos() + (*iter)->GetLen() - chunkPos - len);
          addByte = actualLen_ - oldSize;
          s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(addByte);
          return;
        }
      }
      /*
               ------         ------         DataCache
                  --------------------       WriteData
      */
      oldSize = actualLen_;
      CopyBufToDataCache(chunkPos - chunkPos_, len, data);
      addByte = actualLen_ - oldSize;
      s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(addByte);
    }
  }
  return;
}

void DataCache::Truncate(uint64_t size) {
  uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
  uint32_t pageSize = s3ClientAdaptor_->GetPageSize();
  assert(size <= len_);

  dingofs::utils::LockGuard lg(mtx_);
  uint64_t truncatePos = chunkPos_ + size;
  uint64_t truncateLen = len_ - size;
  uint64_t blockIndex = truncatePos / blockSize;
  uint64_t pageIndex = truncatePos % blockSize / pageSize;
  uint64_t blockPos = truncatePos % blockSize;
  int n, m, blockLen;
  while (truncateLen > 0) {
    if (blockPos + truncateLen > blockSize) {
      n = blockSize - blockPos;
    } else {
      n = truncateLen;
    }
    PageDataMap& pdMap = dataMap_[blockIndex];
    blockLen = n;
    pageIndex = blockPos / pageSize;
    uint64_t pagePos = blockPos % pageSize;
    PageData* pageData = nullptr;
    while (blockLen > 0) {
      if (pagePos + blockLen > pageSize) {
        m = pageSize - pagePos;
      } else {
        m = blockLen;
      }

      if (pagePos == 0) {
        if (pdMap.count(pageIndex)) {
          pageData = pdMap[pageIndex];
          DataStream::GetInstance().FreePage(pageData->data);
          pdMap.erase(pageIndex);
          actualLen_ -= pageSize;
        }
      } else {
        if (pdMap.count(pageIndex)) {
          pageData = pdMap[pageIndex];
          memset(pageData->data + pagePos, 0, m);
        }
      }
      pageIndex++;
      blockLen -= m;
      pagePos = (pagePos + m) % pageSize;
    }
    if (pdMap.empty()) {
      dataMap_.erase(blockIndex);
    }
    blockIndex++;
    truncateLen -= n;
    blockPos = (blockPos + n) % blockSize;
  }

  len_ = size;
  uint64_t tmpActualLen;
  if ((chunkPos_ + len_ - actualChunkPos_) % pageSize == 0) {
    tmpActualLen = chunkPos_ + len_ - actualChunkPos_;
  } else {
    tmpActualLen =
        ((chunkPos_ + len_ - actualChunkPos_) / pageSize + 1) * pageSize;
  }
  assert(tmpActualLen == actualLen_);
  (void)tmpActualLen;
  return;
}

void DataCache::Release() {
  chunkCacheManager_->ReleaseReadDataCache(chunkPos_);
}

void DataCache::CopyDataCacheToBuf(uint64_t offset, uint64_t len, char* data) {
  assert(offset + len <= len_);
  uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
  uint32_t pageSize = s3ClientAdaptor_->GetPageSize();
  uint64_t newChunkPos = chunkPos_ + offset;
  uint64_t blockIndex = newChunkPos / blockSize;
  uint64_t blockPos = newChunkPos % blockSize;
  uint64_t pagePos, pageIndex;
  uint64_t n, m, blockLen;
  uint64_t dataOffset = 0;

  VLOG(9) << "CopyDataCacheToBuf start Offset:" << offset
          << ", newChunkPos:" << newChunkPos << ", len:" << len;

  while (len > 0) {
    if (blockPos + len > blockSize) {
      n = blockSize - blockPos;
    } else {
      n = len;
    }
    blockLen = n;
    PageDataMap& pdMap = dataMap_[blockIndex];
    PageData* pageData = NULL;
    pageIndex = blockPos / pageSize;
    pagePos = blockPos % pageSize;
    while (blockLen > 0) {
      if (pagePos + blockLen > pageSize) {
        m = pageSize - pagePos;
      } else {
        m = blockLen;
      }

      assert(pdMap.count(pageIndex));
      pageData = pdMap[pageIndex];
      memcpy(data + dataOffset, pageData->data + pagePos, m);
      pageIndex++;
      blockLen -= m;
      dataOffset += m;
      pagePos = (pagePos + m) % pageSize;
    }

    blockIndex++;
    len -= n;
    blockPos = (blockPos + n) % blockSize;
  }
  VLOG(9) << "CopyDataCacheToBuf end.";
}

DINGOFS_ERROR DataCache::Flush(uint64_t inodeId, bool toS3) {
  VLOG(9) << "DataCache Flush. chunkPos=" << chunkPos_ << ", len=" << len_
          << ", chunkIndex=" << chunkCacheManager_->GetIndex()
          << ", inodeId=" << inodeId;

  // generate flush task
  std::vector<FlushBlock> s3Tasks;
  std::vector<std::shared_ptr<SetKVCacheTask>> kvCacheTasks;
  char* data = reinterpret_cast<char*>(memalign(IO_ALIGNED_BLOCK_SIZE, len_));
  if (!data) {
    LOG(ERROR) << "new data failed.";
    return DINGOFS_ERROR::INTERNAL;
  }
  CopyDataCacheToBuf(0, len_, data);
  uint64_t writeOffset = 0;
  uint64_t chunkId = 0;
  DINGOFS_ERROR ret = PrepareFlushTasks(inodeId, data, &s3Tasks, &kvCacheTasks,
                                        &chunkId, &writeOffset);
  if (DINGOFS_ERROR::OK != ret) {
    free(data);
    return ret;
  }

  // exec flush task
  FlushTaskExecute(toS3, s3Tasks, kvCacheTasks);
  free(data);

  // inode ship to flush
  std::shared_ptr<InodeWrapper> inodeWrapper;
  ret =
      s3ClientAdaptor_->GetInodeCacheManager()->GetInode(inodeId, inodeWrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(WARNING) << "get inode fail, ret:" << ret;
    status_.store(DataCacheStatus::Dirty, std::memory_order_release);
    return ret;
  }

  S3ChunkInfo info;
  uint64_t chunkIndex = chunkCacheManager_->GetIndex();
  uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
  int64_t offset = chunkIndex * chunkSize + chunkPos_;
  PrepareS3ChunkInfo(chunkId, offset, writeOffset, &info);
  inodeWrapper->AppendS3ChunkInfo(chunkIndex, info);
  s3ClientAdaptor_->GetInodeCacheManager()->ShipToFlush(inodeWrapper);

  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR DataCache::PrepareFlushTasks(
    uint64_t inodeId, char* data, std::vector<FlushBlock>* s3Tasks,
    std::vector<std::shared_ptr<SetKVCacheTask>>* kvCacheTasks,
    uint64_t* chunkId, uint64_t* writeOffset) {
  // allocate chunkid
  uint32_t fsId = s3ClientAdaptor_->GetFsId();
  FSStatusCode ret = s3ClientAdaptor_->AllocS3ChunkId(fsId, 1, chunkId);
  if (ret != FSStatusCode::OK) {
    LOG(ERROR) << "alloc s3 chunkid fail. ret:" << ret;
    return DINGOFS_ERROR::INTERNAL;
  }

  // generate flush task
  uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
  uint32_t objectPrefix = s3ClientAdaptor_->GetObjectPrefix();
  uint64_t blockPos = chunkPos_ % blockSize;
  uint64_t blockIndex = chunkPos_ / blockSize;
  uint64_t remainLen = len_;
  while (remainLen > 0) {
    uint64_t curentLen =
        blockPos + remainLen > blockSize ? blockSize - blockPos : remainLen;

    // generate flush to disk or s3 task
    BlockKey key(fsId, inodeId, *chunkId, blockIndex, 0);
    auto context = std::make_shared<PutObjectAsyncContext>();
    context->key = key.StoreKey();
    context->buffer = data + (*writeOffset);
    context->bufferSize = curentLen;
    context->startTime = butil::cpuwide_time_us();
    s3Tasks->emplace_back(FlushBlock(key, context));

    // generate flush to kvcache task
    if (kvClientManager_) {
      auto task = std::make_shared<SetKVCacheTask>();
      task->key = key.Filename();
      task->value = data + (*writeOffset);
      task->length = curentLen;
      kvCacheTasks->emplace_back(task);
    }

    remainLen -= curentLen;
    blockIndex++;
    (*writeOffset) += curentLen;
    blockPos = (blockPos + curentLen) % blockSize;
  }

  return DINGOFS_ERROR::OK;
}

void DataCache::FlushTaskExecute(
    bool to_s3, const std::vector<FlushBlock>& s3Tasks,
    const std::vector<std::shared_ptr<SetKVCacheTask>>& kvCacheTasks) {
  // callback
  std::atomic<uint64_t> s3PendingTaskCal(s3Tasks.size());
  std::atomic<uint64_t> kvPendingTaskCal(kvCacheTasks.size());
  CountDownEvent s3TaskEvent(s3PendingTaskCal);
  CountDownEvent kvTaskEvent(kvPendingTaskCal);

  // success callback
  PutObjectAsyncCallBack callback =
      [&](const std::shared_ptr<PutObjectAsyncContext>& context) {
        // move metrics collection to block_cache.cpp

        // Don't move the if sentence to the front
        // it will cause core dumped because s3Metric_
        // will be destructed before being accessed
        s3TaskEvent.Signal();
      };

  SetKVCacheDone kvdone = [&](const std::shared_ptr<SetKVCacheTask>& task) {
    kvTaskEvent.Signal();
    return;
  };

  // s3task execute
  auto fs = s3ClientAdaptor_->GetFileSystem();
  auto entry_watcher = fs->BorrowMember().entry_watcher;
  auto block_cache = s3ClientAdaptor_->GetBlockCache();
  if (s3PendingTaskCal.load()) {
    for (const auto& fblock : s3Tasks) {
      auto context = fblock.context;
      BlockKey key = fblock.key;
      Block block(context->buffer, context->bufferSize);
      auto from = entry_watcher->ShouldWriteback(key.ino)
                      ? BlockFrom::NOCTO_FLUSH
                      : BlockFrom::CTO_FLUSH;
      BlockContext ctx(from);
      DataStream::GetInstance().EnterFlushSliceQueue(
          [&, key, block, ctx, callback]() {
            for (;;) {
              auto rc = block_cache->Put(key, block, ctx);
              if (rc == BCACHE_ERROR::OK) {
                callback(context);
                break;
              }
            }
          });
    }
  }
  // kvtask execute
  if (kvClientManager_ && kvPendingTaskCal.load()) {
    std::for_each(kvCacheTasks.begin(), kvCacheTasks.end(),
                  [&](const std::shared_ptr<SetKVCacheTask>& task) {
                    task->done = kvdone;
                    kvClientManager_->Set(task);
                  });
    kvTaskEvent.Wait();
  }

  s3TaskEvent.Wait();
}

void DataCache::PrepareS3ChunkInfo(uint64_t chunkId, uint64_t offset,
                                   uint64_t len, S3ChunkInfo* info) {
  info->set_chunkid(chunkId);
  info->set_compaction(0);
  info->set_offset(offset);
  info->set_len(len);
  info->set_size(len);
  info->set_zero(false);
  VLOG(6) << "UpdateInodeChunkInfo chunkId:" << chunkId << ", offset:" << offset
          << ", len:" << len;
}

bool DataCache::CanFlush(bool force) {
  if (force) {
    return true;
  }

  uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
  uint64_t now = ::dingofs::utils::TimeUtility::GetTimeofDaySec();
  uint32_t flushIntervalSec = s3ClientAdaptor_->GetFlushInterval();

  if (len_ == chunkSize) {
    return true;
  } else if (now < (createTime_ + flushIntervalSec)) {
    return false;
  }

  return true;
}

FsCacheManager::ReadCacheReleaseExecutor::ReadCacheReleaseExecutor()
    : running_(true) {
  t_ = std::thread{&FsCacheManager::ReadCacheReleaseExecutor::ReleaseCache,
                   this};
}

void FsCacheManager::ReadCacheReleaseExecutor::ReleaseCache() {
  while (running_) {
    std::list<DataCachePtr> tmp;

    {
      std::unique_lock<std::mutex> lk(mtx_);
      cond_.wait(lk, [&]() { return !retired_.empty() || !running_; });

      if (!running_) {
        return;
      }

      tmp.swap(retired_);
    }

    for (auto& c : tmp) {
      c->Release();
      c.reset();
    }

    VLOG(9) << "released " << tmp.size() << " data caches";
  }
}

void FsCacheManager::ReadCacheReleaseExecutor::Stop() {
  std::lock_guard<std::mutex> lk(mtx_);
  running_ = false;
  cond_.notify_one();
}

FsCacheManager::ReadCacheReleaseExecutor::~ReadCacheReleaseExecutor() {
  Stop();
  t_.join();
  LOG(INFO) << "ReadCacheReleaseExecutor stopped";
}

void FsCacheManager::ReadCacheReleaseExecutor::Release(
    std::list<DataCachePtr>* caches) {
  std::lock_guard<std::mutex> lk(mtx_);
  retired_.splice(retired_.end(), *caches);
  cond_.notify_one();
}

}  // namespace client
}  // namespace dingofs
