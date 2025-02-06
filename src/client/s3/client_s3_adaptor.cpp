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
 * Created Date: 21-5-31
 * Author: huyao
 */

#include "client/s3/client_s3_adaptor.h"

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <utility>

#include "cache/common/errno.h"
#include "client/datastream/data_stream.h"
#include "client/s3/client_s3_cache_manager.h"

namespace dingofs {

namespace client {

using blockcache::BlockCache;
using blockcache::Errno;
using blockcache::S3Client;
using common::S3ClientAdaptorOption;
using datastream::DataStream;
using filesystem::FileSystem;
using stub::rpcclient::MdsClient;
using utils::Thread;

using pb::mds::FSStatusCode;
using pb::metaserver::S3ChunkInfo;
using pb::metaserver::S3ChunkInfoList;

DINGOFS_ERROR
S3ClientAdaptorImpl::Init(const S3ClientAdaptorOption& option,
                          std::shared_ptr<S3Client> client,
                          std::shared_ptr<InodeCacheManager> inodeManager,
                          std::shared_ptr<MdsClient> mdsClient,
                          std::shared_ptr<FsCacheManager> fsCacheManager,
                          std::shared_ptr<FileSystem> filesystem,
                          std::shared_ptr<BlockCache> block_cache,
                          std::shared_ptr<KVClientManager> kvClientManager,
                          bool startBackGround) {
  blockSize_ = option.blockSize;
  chunkSize_ = option.chunkSize;
  pageSize_ = option.pageSize;
  if (chunkSize_ % blockSize_ != 0) {
    LOG(ERROR) << "chunkSize:" << chunkSize_
               << " is not integral multiple for the blockSize:" << blockSize_;
    return DINGOFS_ERROR::INVALIDPARAM;
  }
  prefetchBlocks_ = option.prefetchBlocks;
  prefetchExecQueueNum_ = option.prefetchExecQueueNum;
  memCacheNearfullRatio_ = option.nearfullRatio;
  throttleBaseSleepUs_ = option.baseSleepUs;
  flushIntervalSec_ = option.flushIntervalSec;
  maxReadRetryIntervalMs_ = option.maxReadRetryIntervalMs;
  readRetryIntervalMs_ = option.readRetryIntervalMs;
  objectPrefix_ = option.objectPrefix;
  client_ = client;
  inodeManager_ = inodeManager;
  mdsClient_ = mdsClient;
  fsCacheManager_ = fsCacheManager;
  waitInterval_.Init(option.intervalMs);
  filesystem_ = filesystem;
  block_cache_ = block_cache;
  kvClientManager_ = std::move(kvClientManager);

  // init block cache
  {
    auto rc = block_cache_->Init();
    if (rc != Errno::OK) {
      LOG(ERROR) << "Init bcache cache failed: " << StrErr(rc);
      return DINGOFS_ERROR::INTERNAL;
    }
  }

  if (HasDiskCache()) {
    // init rpc send exec-queue
    downloadTaskQueues_.resize(prefetchExecQueueNum_);
    for (auto& q : downloadTaskQueues_) {
      int rc = bthread::execution_queue_start(
          &q, nullptr, &S3ClientAdaptorImpl::ExecAsyncDownloadTask, this);
      if (rc != 0) {
        LOG(ERROR) << "Init AsyncRpcQueues failed";
        return DINGOFS_ERROR::INTERNAL;
      }
    }
  }
  if (startBackGround) {
    toStop_.store(false, std::memory_order_release);
    bgFlushThread_ = Thread(&S3ClientAdaptorImpl::BackGroundFlush, this);
  }

  LOG(INFO) << "S3ClientAdaptorImpl Init. block size:" << blockSize_
            << ", chunk size: " << chunkSize_
            << ", prefetchBlocks: " << prefetchBlocks_
            << ", prefetchExecQueueNum: " << prefetchExecQueueNum_
            << ", intervalMs: " << option.intervalMs
            << ", flushIntervalSec: " << option.flushIntervalSec
            << ", writeCacheMaxByte: " << option.writeCacheMaxByte
            << ", readCacheMaxByte: " << option.readCacheMaxByte
            << ", readCacheThreads: " << option.readCacheThreads
            << ", nearfullRatio: " << option.nearfullRatio
            << ", baseSleepUs: " << option.baseSleepUs;
  // start chunk flush threads
  return DINGOFS_ERROR::OK;
}

int S3ClientAdaptorImpl::Write(uint64_t inodeId, uint64_t offset,
                               uint64_t length, const char* buf) {
  VLOG(6) << "write start offset:" << offset << ", len:" << length
          << ", fsId:" << fsId_ << ", inodeId=" << inodeId;
  {
    std::lock_guard<std::mutex> lock_guard(ioMtx_);
    // TODO: maybe no need add then dec
    fsCacheManager_->DataCacheByteInc(length);

    // Write stall for memory near full
    while (DataStream::GetInstance().MemoryNearFull()) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }

  FileCacheManagerPtr file_cache_manager =
      fsCacheManager_->FindOrCreateFileCacheManager(fsId_, inodeId);
  int ret = file_cache_manager->Write(offset, length, buf);
  fsCacheManager_->DataCacheByteDec(length);
  VLOG(6) << "write end inodeId=" << inodeId << ", ret: " << ret;
  return ret;
}

int S3ClientAdaptorImpl::Read(uint64_t inode_id, uint64_t offset,
                              uint64_t length, char* buf) {
  VLOG(6) << "read start offset:" << offset << ", len:" << length
          << ", fsId:" << fsId_ << ", inodeId=" << inode_id;

  FileCacheManagerPtr file_cache_manager =
      fsCacheManager_->FindOrCreateFileCacheManager(fsId_, inode_id);
  int ret = file_cache_manager->Read(inode_id, offset, length, buf);
  VLOG(6) << "read end inodeId=" << inode_id << ", ret:" << ret;
  if (ret < 0) {
    return ret;
  }

  VLOG(6) << "read end offset:" << offset << ", len:" << length
          << ", fsId:" << fsId_ << ", inodeId=" << inode_id;
  return ret;
}

DINGOFS_ERROR S3ClientAdaptorImpl::Truncate(InodeWrapper* inodeWrapper,
                                            uint64_t size) {
  const auto* inode = inodeWrapper->GetInodeLocked();
  uint64_t fileSize = inode->length();

  if (size < fileSize) {
    VLOG(6) << "Truncate size:" << size << " less than fileSize:" << fileSize;
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindOrCreateFileCacheManager(fsId_, inode->inodeid());
    fileCacheManager->TruncateCache(size, fileSize);
    return DINGOFS_ERROR::OK;
  } else if (size == fileSize) {
    return DINGOFS_ERROR::OK;
  } else {
    VLOG(6) << "Truncate size:" << size << " more than fileSize" << fileSize;
    uint64_t offset = fileSize;
    uint64_t len = size - fileSize;
    uint64_t index = offset / chunkSize_;
    uint64_t chunkPos = offset % chunkSize_;
    uint64_t n = 0;
    uint64_t beginChunkId;
    FSStatusCode ret;
    uint64_t fsId = inode->fsid();
    uint32_t chunkIdNum = len / chunkSize_ + 1;
    ret = AllocS3ChunkId(fsId, chunkIdNum, &beginChunkId);
    if (ret != FSStatusCode::OK) {
      LOG(ERROR) << "Truncate alloc s3 chunkid fail. ret:" << ret;
      return DINGOFS_ERROR::INTERNAL;
    }
    uint64_t chunkId = beginChunkId;
    while (len > 0) {
      if (chunkPos + len > chunkSize_) {
        n = chunkSize_ - chunkPos;
      } else {
        n = len;
      }
      assert(chunkId <= (beginChunkId + chunkIdNum - 1));
      S3ChunkInfo* tmp;
      auto* s3ChunkInfoMap = inodeWrapper->GetChunkInfoMap();
      auto s3chunkInfoListIter = s3ChunkInfoMap->find(index);
      if (s3chunkInfoListIter == s3ChunkInfoMap->end()) {
        S3ChunkInfoList s3chunkInfoList;
        tmp = s3chunkInfoList.add_s3chunks();
        tmp->set_chunkid(chunkId);
        tmp->set_offset(offset);
        tmp->set_len(n);
        tmp->set_size(n);
        tmp->set_zero(true);
        s3ChunkInfoMap->insert({index, s3chunkInfoList});
      } else {
        S3ChunkInfoList& s3chunkInfoList = s3chunkInfoListIter->second;
        tmp = s3chunkInfoList.add_s3chunks();
        tmp->set_chunkid(chunkId);
        tmp->set_offset(offset);
        tmp->set_len(n);
        tmp->set_size(n);
        tmp->set_zero(true);
      }
      len -= n;
      index++;
      chunkPos = (chunkPos + n) % chunkSize_;
      offset += n;
      chunkId++;
    }
    return DINGOFS_ERROR::OK;
  }
}

void S3ClientAdaptorImpl::ReleaseCache(uint64_t inodeId) {
  FileCacheManagerPtr fileCacheManager =
      fsCacheManager_->FindFileCacheManager(inodeId);
  if (!fileCacheManager) {
    return;
  }
  VLOG(9) << "ReleaseCache inode:" << inodeId;
  fileCacheManager->ReleaseCache();
  fsCacheManager_->ReleaseFileCacheManager(inodeId);
}

DINGOFS_ERROR S3ClientAdaptorImpl::Flush(uint64_t inode_id) {
  FileCacheManagerPtr file_cache_manager =
      fsCacheManager_->FindFileCacheManager(inode_id);
  if (!file_cache_manager) {
    return DINGOFS_ERROR::OK;
  }
  VLOG(6) << "Flush data of inodeId=" << inode_id;
  return file_cache_manager->Flush(true, false);
}

DINGOFS_ERROR S3ClientAdaptorImpl::FsSync() {
  return fsCacheManager_->FsSync(true);
}

FSStatusCode S3ClientAdaptorImpl::AllocS3ChunkId(uint32_t fsId, uint32_t idNum,
                                                 uint64_t* chunkId) {
  return mdsClient_->AllocS3ChunkId(fsId, idNum, chunkId);
}

void S3ClientAdaptorImpl::BackGroundFlush() {
  while (!toStop_.load(std::memory_order_acquire)) {
    {
      std::unique_lock<std::mutex> lck(mtx_);
      if (fsCacheManager_->GetDataCacheNum() == 0) {
        VLOG(3) << "BackGroundFlush has no write cache, so wait";
        cond_.wait(lck);
      }
    }

    if (DataStream::GetInstance().MemoryNearFull()) {
      VLOG(3) << "BackGroundFlush radically, write cache num is: "
              << fsCacheManager_->GetDataCacheNum();
      fsCacheManager_->FsSync(true);

    } else {
      waitInterval_.WaitForNextExcution();
      VLOG(6) << "BackGroundFlush, write cache num is:"
              << fsCacheManager_->GetDataCacheNum();
      fsCacheManager_->FsSync(false);
      VLOG(6) << "background fssync end";
    }
  }
}

int S3ClientAdaptorImpl::Stop() {
  LOG(INFO) << "start Stopping S3ClientAdaptor.";
  waitInterval_.StopWait();
  toStop_.store(true, std::memory_order_release);
  FsSyncSignal();
  if (bgFlushThread_.joinable()) {
    bgFlushThread_.join();
  }
  if (HasDiskCache()) {
    for (auto& q : downloadTaskQueues_) {
      bthread::execution_queue_stop(q);
      bthread::execution_queue_join(q);
    }
  }
  block_cache_->Shutdown();
  return 0;
}

int S3ClientAdaptorImpl::ExecAsyncDownloadTask(
    void* meta,
    bthread::TaskIterator<AsyncDownloadTask>& iter) {  // NOLINT
  (void)meta;
  if (iter.is_queue_stopped()) {
    return 0;
  }

  for (; iter; ++iter) {
    auto& task = *iter;
    task();
  }

  return 0;
}

DINGOFS_ERROR S3ClientAdaptorImpl::FlushAllCache(uint64_t inodeId) {
  VLOG(6) << "FlushAllCache, inodeId=" << inodeId;
  FileCacheManagerPtr fileCacheManager =
      fsCacheManager_->FindFileCacheManager(inodeId);
  if (!fileCacheManager) {
    return DINGOFS_ERROR::OK;
  }

  // force flush data in memory to s3
  VLOG(6) << "FlushAllCache, flush memory data of inodeId=" << inodeId;
  DINGOFS_ERROR ret = fileCacheManager->Flush(true, false);
  if (ret != DINGOFS_ERROR::OK) {
    return ret;
  }

  // force flush data in diskcache to s3
  if (!kvClientManager_ && HasDiskCache()) {
    VLOG(6) << "FlushAllCache, wait inodeId=" << inodeId
            << " related chunk upload to s3";

    auto rc = block_cache_->Flush(inodeId);
    if (rc != Errno::OK) {
      return DINGOFS_ERROR::INTERNAL;
    }

    VLOG(6) << "FlushAllCache, inodeId=" << inodeId
            << " related chunk upload to s3 done";
  }

  return ret;
}

void S3ClientAdaptorImpl::Enqueue(
    std::shared_ptr<FlushChunkCacheContext> context) {
  auto task = [this, context]() { this->FlushChunkClosure(context); };
  DataStream::GetInstance().EnterFlushChunkQueue(task);
}

int S3ClientAdaptorImpl::FlushChunkClosure(
    std::shared_ptr<FlushChunkCacheContext> context) {
  VLOG(9) << "FlushChunkCacheClosure start: inodeId=" << context->inode;
  DINGOFS_ERROR ret =
      context->chunkCacheManptr->Flush(context->inode, context->force);
  // set the returned value
  // it is need in FlushChunkCacheCallBack
  context->retCode = ret;
  context->cb(context);
  VLOG(9) << "FlushChunkCacheClosure end: inodeId=" << context->inode
          << ", ret:" << ret;
  return 0;
}

}  // namespace client
}  // namespace dingofs
