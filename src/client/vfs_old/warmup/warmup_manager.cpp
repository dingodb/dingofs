/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-01-31
 * Author: chengyi01
 */

#include "client/vfs_old/warmup/warmup_manager.h"

#include <glog/logging.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <list>
#include <memory>
#include <utility>

#include "base/filepath/filepath.h"
#include "client/blockcache/cache_store.h"
#include "client/blockcache/s3_client.h"
#include "client/vfs_old/common/common.h"
#include "client/vfs_old/inode_wrapper.h"
#include "client/vfs_old/kvclient/kvclient_manager.h"
#include "client/vfs/vfs_meta.h"
#include "stub/metric/metric.h"
#include "utils/concurrent/concurrent.h"
#include "utils/string_util.h"

namespace dingofs {
namespace client {
namespace warmup {

using aws::GetObjectAsyncCallBack;
using aws::GetObjectAsyncContext;
using base::filepath::PathSplit;
using blockcache::BCACHE_ERROR;
using blockcache::Block;
using blockcache::BlockKey;
using blockcache::S3ClientImpl;
using common::FuseClientOption;
using common::WarmupStorageType;
using stub::metric::MetricGuard;
using stub::metric::S3Metric;
using utils::ReadLockGuard;
using utils::WriteLockGuard;

using pb::metaserver::Dentry;
using pb::metaserver::FsFileType;

#define WARMUP_CHECKINTERVAL_US (1000 * 1000)

bool WarmupManagerS3Impl::AddWarmupFilelist(fuse_ino_t key,
                                            WarmupStorageType type) {
  if (!mounted_.load(std::memory_order_acquire)) {
    LOG(ERROR) << "not mounted";
    return false;
  }
  // add warmup Progress
  if (AddWarmupProcess(key, type)) {
    VLOG(9) << "add warmup list task:" << key;
    WriteLockGuard lock(warmupFilelistDequeMutex_);
    auto iter = FindWarmupFilelistByKeyLocked(key);
    if (iter == warmupFilelistDeque_.end()) {
      std::shared_ptr<InodeWrapper> inode_wrapper;
      DINGOFS_ERROR ret = inodeManager_->GetInode(key, inode_wrapper);
      if (ret != DINGOFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << key;
        return false;
      }
      uint64_t len = inode_wrapper->GetLength();
      warmupFilelistDeque_.emplace_back(key, len);
    }
  }  // Skip already added
  return true;
}

bool WarmupManagerS3Impl::AddWarmupFile(fuse_ino_t key, const std::string& path,
                                        WarmupStorageType type) {
  if (!mounted_.load(std::memory_order_acquire)) {
    LOG(ERROR) << "not mounted";
    return false;
  }
  // add warmup Progress
  if (AddWarmupProcess(key, type)) {
    VLOG(9) << "add warmup single task:" << key;
    FetchDentryEnqueue(key, path);
  }
  return true;
}

void WarmupManagerS3Impl::UnInit() {
  bgFetchStop_.store(true, std::memory_order_release);
  if (initbgFetchThread_) {
    bgFetchThread_.join();
  }

  for (auto& task : inode2FetchDentryPool_) {
    task.second->Stop();
  }

  {
    WriteLockGuard lock_dentry(inode2FetchDentryPoolMutex_);
    inode2FetchDentryPool_.clear();
  }

  {
    WriteLockGuard lock_s3_objects(inode2FetchS3ObjectsPoolMutex_);
    for (auto& task : inode2FetchS3ObjectsPool_) {
      task.second->Stop();
    }
    inode2FetchS3ObjectsPool_.clear();
  }

  {
    WriteLockGuard lock_inodes(warmupInodesDequeMutex_);
    warmupInodesDeque_.clear();
  }

  {
    WriteLockGuard lock_file_list(warmupFilelistDequeMutex_);
    warmupFilelistDeque_.clear();
  }

  WarmupManager::UnInit();
}

void WarmupManagerS3Impl::Init(const FuseClientOption& option) {
  WarmupManager::Init(option);
  bgFetchStop_.store(false, std::memory_order_release);
  bgFetchThread_ = utils::Thread(&WarmupManagerS3Impl::BackGroundFetch, this);
  initbgFetchThread_ = true;
}

void WarmupManagerS3Impl::BackGroundFetch() {
  while (!bgFetchStop_.load(std::memory_order_acquire)) {
    usleep(WARMUP_CHECKINTERVAL_US);
    ScanWarmupFilelist();
    ScanWarmupInodes();
    ScanCleanFetchS3ObjectsPool();
    ScanCleanFetchDentryPool();
    ScanCleanWarmupProgress();
  }
}

void WarmupManagerS3Impl::GetWarmupList(const WarmupFilelist& filelist,
                                        std::vector<std::string>* list) {
  int flags = 0;
  flags &= ~O_DIRECT;

  size_t read_size = 0;
  std::unique_ptr<char[]> data(new char[filelist.GetFileLen() + 1]);
  std::memset(data.get(), 0, filelist.GetFileLen());
  data[filelist.GetFileLen()] = '\n';

  {
    uint64_t fh = 0;
    vfs::Attr attr;
    Status s = vfs_->Open(filelist.GetKey(), flags, &fh, &attr);
    if (!s.ok()) {
      LOG(ERROR) << "Fail open warmup list file, status: " << s.ToString()
                 << ", key = " << filelist.GetKey();
      return;
    }

    s = vfs_->Read(filelist.GetKey(), data.get(), filelist.GetFileLen(), 0, fh,
                   &read_size);
    if (!s.ok()) {
      LOG(ERROR) << "Fail read warmup list file, status: " << s.ToString()
                 << ", key = " << filelist.GetKey();
      vfs_->Release(filelist.GetKey(), fh);
      return;
    } else {
      if (read_size != filelist.GetFileLen()) {
        LOG(WARNING) << "read warmup list file size mismatch,  key = "
                     << filelist.GetKey() << ", read_size:" << read_size
                     << ", expect_size:" << filelist.GetFileLen();
      }
      vfs_->Release(filelist.GetKey(), fh);
    }
  }

  std::string file = data.get();
  VLOG(9) << "file is: " << file;
  // remove enter, newline, blank
  std::string blanks("\r\n ");
  file.erase(0, file.find_first_not_of(blanks));
  file.erase(file.find_last_not_of(blanks) + 1);
  VLOG(9) << "after del file is: " << file;
  dingofs::utils::AddSplitStringToResult(file, "\n", list);
}

void WarmupManagerS3Impl::FetchDentryEnqueue(fuse_ino_t key,
                                             const std::string& file) {
  VLOG(9) << "FetchDentryEnqueue start: " << key << " file: " << file;
  auto task = [this, key, file]() { LookPath(key, file); };
  AddFetchDentryTask(key, task);
  VLOG(9) << "FetchDentryEnqueue end: " << key << " file: " << file;
}

void WarmupManagerS3Impl::LookPath(fuse_ino_t key, std::string file) {
  VLOG(9) << "LookPath start key: " << key << " file: " << file;
  std::vector<std::string> split_path;
  // remove enter, newline, blank
  std::string blanks("\r\n ");
  file.erase(0, file.find_first_not_of(blanks));
  file.erase(file.find_last_not_of(blanks) + 1);
  if (file.empty()) {
    VLOG(9) << "empty path";
    return;
  }

  bool is_root = false;
  if (file == "/") {
    split_path.push_back(file);
    is_root = true;
  } else {
    dingofs::utils::AddSplitStringToResult(file, "/", &split_path);
  }

  VLOG(6) << "splitPath size is: " << split_path.size();
  if (split_path.size() == 1 && is_root) {
    VLOG(9) << "i am root";
    auto task = [this, key]() {
      FetchChildDentry(key, fsInfo_->rootinodeid());
    };
    AddFetchDentryTask(key, task);
    return;
  } else if (split_path.size() == 1) {
    VLOG(9) << "parent is root: " << fsInfo_->rootinodeid()
            << ", path is: " << split_path[0];
    auto task = [this, key, split_path]() {
      FetchDentry(key, fsInfo_->rootinodeid(), split_path[0]);
    };
    AddFetchDentryTask(key, task);
    return;
  } else if (split_path.size() > 1) {  // travel path
    VLOG(9) << "traverse path start: " << split_path.size();
    std::string last_name = split_path.back();
    split_path.pop_back();
    fuse_ino_t ino = fsInfo_->rootinodeid();
    for (const auto& iter : split_path) {
      VLOG(9) << "traverse path: " << iter << "ino is: " << ino;
      Dentry dentry;
      std::string path_name = iter;
      DINGOFS_ERROR ret = dentryManager_->GetDentry(ino, path_name, &dentry);
      if (ret != DINGOFS_ERROR::OK) {
        if (ret != DINGOFS_ERROR::NOTEXIST) {
          LOG(WARNING) << "dentryManager_ get dentry fail, ret = " << ret
                       << ", parent inodeid = " << ino << ", name = " << file;
        }
        VLOG(9) << "FetchDentry error: " << ret;
        return;
      }
      ino = dentry.inodeid();
    }
    auto task = [this, key, ino, last_name]() {
      FetchDentry(key, ino, last_name);
    };
    AddFetchDentryTask(key, task);
    VLOG(9) << "ino is: " << ino << " lastname is: " << last_name;
    return;
  } else {
    VLOG(3) << "unknown path";
  }
  VLOG(9) << "LookPath start end: " << key << " file: " << file;
}

void WarmupManagerS3Impl::FetchDentry(fuse_ino_t key, fuse_ino_t ino,
                                      const std::string& file) {
  VLOG(9) << "FetchDentry start: " << file << ", ino: " << ino
          << " key: " << key;
  Dentry dentry;
  DINGOFS_ERROR ret = dentryManager_->GetDentry(ino, file, &dentry);
  if (ret != DINGOFS_ERROR::OK) {
    if (ret != DINGOFS_ERROR::NOTEXIST) {
      LOG(WARNING) << "dentryManager_ get dentry fail, ret = " << ret
                   << ", parent inodeid = " << ino << ", name = " << file;
    } else {
      LOG(ERROR) << "FetchDentry key: " << key << " file: " << file
                 << " errorCode: " << ret;
    }
    return;
  }

  if (FsFileType::TYPE_S3 == dentry.type()) {
    WriteLockGuard lock(warmupInodesDequeMutex_);
    auto iter_deque = FindWarmupInodesByKeyLocked(key);
    if (iter_deque == warmupInodesDeque_.end()) {
      warmupInodesDeque_.emplace_back(key,
                                      std::set<fuse_ino_t>{dentry.inodeid()});
    } else {
      iter_deque->AddFileInode(dentry.inodeid());
    }
    return;
  } else if (FsFileType::TYPE_DIRECTORY == dentry.type()) {
    auto task = [this, key, dentry]() {
      FetchChildDentry(key, dentry.inodeid());
    };
    AddFetchDentryTask(key, task);
    VLOG(9) << "FetchDentry: " << dentry.inodeid();
    return;

  } else if (FsFileType::TYPE_SYM_LINK == dentry.type()) {
    // skip links
  } else {
    VLOG(3) << "unkown, file: " << file << ", ino: " << ino;
    return;
  }
  VLOG(9) << "FetchDentry end: " << file << ", ino: " << ino;
}

void WarmupManagerS3Impl::FetchChildDentry(fuse_ino_t key, fuse_ino_t ino) {
  VLOG(9) << "FetchChildDentry start: key:" << key << " inode: " << ino;
  std::list<Dentry> dentry_list;
  auto limit = option_.listDentryLimit;
  DINGOFS_ERROR ret = dentryManager_->ListDentry(ino, &dentry_list, limit);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "dentryManager_ ListDentry fail, ret = " << ret
               << ", parent = " << ino;
    return;
  }

  for (const auto& dentry : dentry_list) {
    VLOG(9) << "FetchChildDentry: key:" << key << " dentry: " << dentry.name();
    if (FsFileType::TYPE_S3 == dentry.type()) {
      WriteLockGuard lock(warmupInodesDequeMutex_);
      auto iter_deque = FindWarmupInodesByKeyLocked(key);
      if (iter_deque == warmupInodesDeque_.end()) {
        warmupInodesDeque_.emplace_back(key,
                                        std::set<fuse_ino_t>{dentry.inodeid()});
      } else {
        iter_deque->AddFileInode(dentry.inodeid());
      }
      VLOG(9) << "FetchChildDentry: " << dentry.inodeid();
    } else if (FsFileType::TYPE_DIRECTORY == dentry.type()) {
      auto task = [this, key, dentry]() {
        FetchChildDentry(key, dentry.inodeid());
      };
      AddFetchDentryTask(key, task);
      VLOG(9) << "FetchChildDentry: " << dentry.inodeid();
    } else if (FsFileType::TYPE_SYM_LINK == dentry.type()) {  // need todo
    } else {
      VLOG(9) << "unknown type";
    }
  }
  VLOG(9) << "FetchChildDentry end: key:" << key << " inode: " << ino;
}

void WarmupManagerS3Impl::FetchDataEnqueue(fuse_ino_t key, fuse_ino_t ino) {
  VLOG(9) << "FetchDataEnqueue start: key:" << key << " inode: " << ino;
  auto task = [key, ino, this]() {
    std::shared_ptr<InodeWrapper> inode_wrapper;
    DINGOFS_ERROR ret = inodeManager_->GetInode(ino, inode_wrapper);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                 << ", inodeid = " << ino;
      return;
    }

    S3ChunkInfoMapType s3_chunk_info_map;
    {
      ::dingofs::utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();
      s3_chunk_info_map = *inode_wrapper->GetChunkInfoMap();
    }
    if (s3_chunk_info_map.empty()) {
      return;
    }
    TravelChunks(key, ino, s3_chunk_info_map);
  };

  AddFetchS3objectsTask(key, task);
  VLOG(9) << "FetchDataEnqueue end: key:" << key << " inode: " << ino;
}

void WarmupManagerS3Impl::TravelChunks(
    fuse_ino_t key, fuse_ino_t ino,
    const S3ChunkInfoMapType& s3_chunk_info_map) {
  VLOG(9) << "travel chunk start: " << ino
          << ", size: " << s3_chunk_info_map.size();
  for (auto const& info_iter : s3_chunk_info_map) {
    VLOG(9) << "travel chunk: " << info_iter.first;
    std::list<std::pair<BlockKey, uint64_t>> prefetch_objs;
    TravelChunk(ino, info_iter.second, &prefetch_objs);
    {
      ReadLockGuard lock(inode2ProgressMutex_);
      auto iter = FindWarmupProgressByKeyLocked(key);
      if (iter != inode2Progress_.end()) {
        iter->second.AddTotal(prefetch_objs.size());
      } else {
        LOG(ERROR) << "no such warmup progress: " << key;
      }
    }
    auto task = [this, key, prefetch_objs]() {
      WarmUpAllObjs(key, prefetch_objs);
    };
    AddFetchS3objectsTask(key, task);
  }
  VLOG(9) << "travel chunks end";
}

void WarmupManagerS3Impl::TravelChunk(
    fuse_ino_t ino, const pb::metaserver::S3ChunkInfoList& chunk_info,
    ObjectListType* prefetch_objs) {
  uint64_t block_size = s3Adaptor_->GetBlockSize();
  uint64_t chunk_size = s3Adaptor_->GetChunkSize();

  uint64_t offset, len, chunkid, compaction;
  for (const auto& chunkinfo : chunk_info.s3chunks()) {
    auto fs_id = fsInfo_->fsid();
    chunkid = chunkinfo.chunkid();
    compaction = chunkinfo.compaction();
    offset = chunkinfo.offset();
    len = chunkinfo.len();
    // the offset in the chunk
    uint64_t chunk_pos = offset % chunk_size;
    // the first blockIndex
    uint64_t block_index_begin = chunk_pos / block_size;

    if (len < block_size) {  // just one block
      BlockKey key(fs_id, ino, chunkid, block_index_begin, compaction);
      prefetch_objs->push_back(std::make_pair(key, len));
    } else {
      // the offset in the block
      uint64_t block_pos = chunk_pos % block_size;

      // firstly, let's get the size in the first block
      // then, subtract the length in the first block
      // to obtain the remaining length
      // lastly, We need to judge the last block is full or not
      uint64_t first_block_size =
          (block_pos != 0) ? block_size - block_pos : block_size;
      uint64_t left_size = len - first_block_size;
      uint32_t block_counts = (left_size % block_size == 0)
                                  ? (left_size / block_size + 1)
                                  : (left_size / block_size + 1 + 1);
      // so we can get the last blockIndex
      // because the bolck Index is cumulative
      uint64_t block_index_end = block_index_begin + block_counts - 1;

      // the size of the last block
      uint64_t last_block_size = left_size % block_size;
      // whether the first block or the last block is full or not
      bool first_block_full = (block_pos == 0);
      bool last_block_full = (last_block_size == 0);
      // the start and end block Index that need travel
      uint64_t travel_start_index, travel_end_index;
      // if the block is full, the size is needed download
      // of the obj is blockSize. Otherwise, the value is special.
      if (!first_block_full) {
        travel_start_index = block_index_begin + 1;
        BlockKey key(fs_id, ino, chunkid, block_index_begin, compaction);
        prefetch_objs->push_back(std::make_pair(key, first_block_size));
      } else {
        travel_start_index = block_index_begin;
      }
      if (!last_block_full) {
        // block index is greater than or equal to 0
        travel_end_index = (block_index_end == block_index_begin)
                               ? block_index_end
                               : block_index_end - 1;
        BlockKey key(fs_id, ino, chunkid, block_index_end, compaction);
        // there is no need to care about the order
        // in which objects are downloaded
        prefetch_objs->push_back(std::make_pair(key, last_block_size));
      } else {
        travel_end_index = block_index_end;
      }
      VLOG(9) << "travel obj, ino: " << ino << ", chunkid: " << chunkid
              << ", blockCounts: " << block_counts
              << ", compaction: " << compaction << ", blockSize: " << block_size
              << ", chunkSize: " << chunk_size << ", offset: " << offset
              << ", blockIndexBegin: " << block_index_begin
              << ", blockIndexEnd: " << block_index_end << ", len: " << len
              << ", firstBlockSize: " << first_block_size
              << ", lastBlockSize: " << last_block_size
              << ", blockPos: " << block_pos << ", chunkPos: " << chunk_pos;
      for (auto block_index = travel_start_index;
           block_index <= travel_end_index; block_index++) {
        BlockKey key(fs_id, ino, chunkid, block_index, compaction);
        prefetch_objs->push_back(std::make_pair(key, block_size));
      }
    }
  }
}

// TODO(hzwuhongsong): These logics are very similar to other place,
// try to merge it
void WarmupManagerS3Impl::WarmUpAllObjs(
    fuse_ino_t ino,
    const std::list<std::pair<BlockKey, uint64_t>>& prefetch_objs) {
  std::atomic<uint64_t> pending_req(0);
  dingofs::utils::CountDownEvent cond(1);
  uint64_t start = butil::cpuwide_time_us();
  // callback function
  GetObjectAsyncCallBack cb =
      [&](const aws::S3Adapter* adapter,
          const std::shared_ptr<GetObjectAsyncContext>& context) {
        (void)adapter;
        // metrics for async get data from s3
        MetricGuard guard(&context->retCode, &S3Metric::GetInstance().read_s3,
                          context->len, start);
        if (bgFetchStop_.load(std::memory_order_acquire)) {
          VLOG(9) << "need stop warmup";
          cond.Signal();
          return;
        }
        if (context->retCode == 0) {
          VLOG(9) << "Get Object success: " << context->key;
          PutObjectToCache(ino, context);
          CollectMetrics(&warmupS3Metric_.warmupS3Cached, context->len, start);
          warmupS3Metric_.warmupS3CacheSize << context->len;
          if (pending_req.fetch_sub(1, std::memory_order_seq_cst) == 1) {
            VLOG(6) << "pendingReq is over";
            cond.Signal();
          }
          return;
        }
        warmupS3Metric_.warmupS3Cached.eps.count << 1;
        if (++context->retry >= option_.downloadMaxRetryTimes) {
          if (pending_req.fetch_sub(1, std::memory_order_seq_cst) == 1) {
            VLOG(6) << "pendingReq is over";
            cond.Signal();
          }
          VLOG(9) << "Up to max retry times, "
                  << "download object failed, key: " << context->key;
          delete[] context->buf;
          return;
        }

        LOG(WARNING) << "Get Object failed, key: " << context->key
                     << ", offset: " << context->offset;
        S3ClientImpl::GetInstance()->AsyncGet(context);
      };

  pending_req.fetch_add(prefetch_objs.size(), std::memory_order_seq_cst);
  if (pending_req.load(std::memory_order_seq_cst)) {
    VLOG(9) << "wait for pendingReq";
    for (auto iter : prefetch_objs) {
      BlockKey bkey = iter.first;
      std::string name = bkey.StoreKey();
      uint64_t read_len = iter.second;
      VLOG(9) << "download start: " << name;
      {
        ReadLockGuard lock(inode2ProgressMutex_);
        auto iter_progress = FindWarmupProgressByKeyLocked(ino);
        if (iter_progress->second.GetStorageType() ==
                dingofs::client::common::WarmupStorageType::
                    kWarmupStorageTypeDisk &&
            s3Adaptor_->GetBlockCache()->IsCached(bkey)) {
          // storage in disk and has cached
          pending_req.fetch_sub(1);
          continue;
        }
      }

      char* cache_s3 = new char[read_len];
      memset(cache_s3, 0, read_len);
      auto context = std::make_shared<GetObjectAsyncContext>();
      context->key = name;
      context->buf = cache_s3;
      context->offset = 0;
      context->len = read_len;
      context->cb = cb;
      context->retry = 0;
      S3ClientImpl::GetInstance()->AsyncGet(context);
    }

    if (pending_req.load()) cond.Wait();
  }
}

bool WarmupManagerS3Impl::ProgressDone(fuse_ino_t key) {
  bool ret;
  {
    ReadLockGuard lock_list(warmupFilelistDequeMutex_);
    ret = FindWarmupFilelistByKeyLocked(key) == warmupFilelistDeque_.end();
  }

  {
    ReadLockGuard lock_dentry(inode2FetchDentryPoolMutex_);
    ret = ret &&
          (FindFetchDentryPoolByKeyLocked(key) == inode2FetchDentryPool_.end());
  }

  {
    ReadLockGuard lock_inodes(warmupInodesDequeMutex_);
    ret = ret && (FindWarmupInodesByKeyLocked(key) == warmupInodesDeque_.end());
  }

  {
    ReadLockGuard lock_s3_objects(inode2FetchS3ObjectsPoolMutex_);
    ret = ret && (FindFetchS3ObjectsPoolByKeyLocked(key) ==
                  inode2FetchS3ObjectsPool_.end());
  }
  return ret;
}

void WarmupManagerS3Impl::ScanCleanFetchDentryPool() {
  // clean inode2FetchDentryPool_
  WriteLockGuard lock(inode2FetchDentryPoolMutex_);
  for (auto iter = inode2FetchDentryPool_.begin();
       iter != inode2FetchDentryPool_.end();) {
    std::deque<WarmupInodes>::iterator iter_inode;
    if (iter->second->QueueSize() == 0) {
      VLOG(9) << "remove FetchDentry task: " << iter->first;
      iter->second->Stop();
      iter = inode2FetchDentryPool_.erase(iter);
    } else {
      ++iter;
    }
  }
}

void WarmupManagerS3Impl::ScanCleanFetchS3ObjectsPool() {
  // clean inode2FetchS3ObjectsPool_
  WriteLockGuard lock(inode2FetchS3ObjectsPoolMutex_);
  for (auto iter = inode2FetchS3ObjectsPool_.begin();
       iter != inode2FetchS3ObjectsPool_.end();) {
    if (iter->second->QueueSize() == 0) {
      VLOG(9) << "remove FetchS3object task: " << iter->first;
      iter->second->Stop();
      iter = inode2FetchS3ObjectsPool_.erase(iter);
    } else {
      ++iter;
    }
  }
}

void WarmupManagerS3Impl::ScanCleanWarmupProgress() {
  // clean done warmupProgress
  ReadLockGuard lock(inode2ProgressMutex_);
  for (auto iter = inode2Progress_.begin(); iter != inode2Progress_.end();) {
    if (ProgressDone(iter->first)) {
      VLOG(9) << "warmup key: " << iter->first << " done!";
      iter = inode2Progress_.erase(iter);
    } else {
      ++iter;
    }
  }
}

void WarmupManagerS3Impl::ScanWarmupInodes() {
  // file need warmup
  WriteLockGuard lock(warmupInodesDequeMutex_);
  if (!warmupInodesDeque_.empty()) {
    WarmupInodes inodes = warmupInodesDeque_.front();
    for (auto const& iter : inodes.GetReadAheadFiles()) {
      VLOG(9) << "BackGroundFetch: key: " << inodes.GetKey()
              << " inode:" << iter;
      FetchDataEnqueue(inodes.GetKey(), iter);
    }
    warmupInodesDeque_.pop_front();
  }
}

void WarmupManagerS3Impl::ScanWarmupFilelist() {
  // Use a write lock to ensure that all parsing tasks are added.
  WriteLockGuard lock(warmupFilelistDequeMutex_);
  if (!warmupFilelistDeque_.empty()) {
    WarmupFilelist warmup_filelist = warmupFilelistDeque_.front();
    VLOG(9) << "warmup ino: " << warmup_filelist.GetKey()
            << " len is: " << warmup_filelist.GetFileLen();

    std::vector<std::string> warmuplist;
    GetWarmupList(warmup_filelist, &warmuplist);
    for (const auto& file_path : warmuplist) {
      FetchDentryEnqueue(warmup_filelist.GetKey(), file_path);
    }
    warmupFilelistDeque_.pop_front();
  }
}

void WarmupManagerS3Impl::AddFetchDentryTask(fuse_ino_t key,
                                             std::function<void()> task) {
  VLOG(9) << "add fetchDentry task: " << key;
  if (!bgFetchStop_.load(std::memory_order_acquire)) {
    WriteLockGuard lock(inode2FetchDentryPoolMutex_);
    auto iter = inode2FetchDentryPool_.find(key);
    if (iter == inode2FetchDentryPool_.end()) {
      std::unique_ptr<ThreadPool> tp = absl::make_unique<ThreadPool>();
      tp->Start(option_.warmupThreadsNum);
      iter = inode2FetchDentryPool_.emplace(key, std::move(tp)).first;
    }
    if (!iter->second->Enqueue(task)) {
      LOG(ERROR) << "key:" << key
                 << " fetch dentry thread pool has been stoped!";
    }
    VLOG(9) << "add fetchDentry task: " << key << " finished";
  }
}

void WarmupManagerS3Impl::AddFetchS3objectsTask(fuse_ino_t key,
                                                std::function<void()> task) {
  VLOG(9) << "add fetchS3Objects task: " << key;
  if (!bgFetchStop_.load(std::memory_order_acquire)) {
    WriteLockGuard lock(inode2FetchS3ObjectsPoolMutex_);
    auto iter = inode2FetchS3ObjectsPool_.find(key);
    if (iter == inode2FetchS3ObjectsPool_.end()) {
      std::unique_ptr<ThreadPool> tp = absl::make_unique<ThreadPool>();
      tp->Start(option_.warmupThreadsNum);
      iter = inode2FetchS3ObjectsPool_.emplace(key, std::move(tp)).first;
    }
    if (!iter->second->Enqueue(task)) {
      LOG(ERROR) << "key:" << key
                 << " fetch s3 objects thread pool has been stoped!";
    }
    VLOG(9) << "add fetchS3Objects task: " << key << " finished";
  }
}

void WarmupManagerS3Impl::PutObjectToCache(
    fuse_ino_t ino, const std::shared_ptr<GetObjectAsyncContext>& context) {
  ReadLockGuard lock(inode2ProgressMutex_);
  auto iter = FindWarmupProgressByKeyLocked(ino);
  if (iter == inode2Progress_.end()) {
    VLOG(9) << "no this warmup task progress: " << ino;
    return;
  }
  // update progress
  iter->second.FinishedPlusOne();
  switch (iter->second.GetStorageType()) {
    case dingofs::client::common::WarmupStorageType::kWarmupStorageTypeDisk: {
      BlockKey key;
      Block block(context->buf, context->len);
      auto items = PathSplit(context->key);
      CHECK_GT(items.size(), 0);
      CHECK(key.ParseFilename(items.back()));
      auto block_cache = s3Adaptor_->GetBlockCache();
      auto rc = block_cache->Cache(key, block);
      if (rc != BCACHE_ERROR::OK) {
        // cache failed,add error count
        iter->second.ErrorsPlusOne();
        LOG_EVERY_SECOND(INFO) << "Cache block (" << key.Filename() << ")"
                               << " failed: " << StrErr(rc);
      }
    }
      delete[] context->buf;
      break;
    case dingofs::client::common::WarmupStorageType::kWarmupStorageTypeKvClient:
      if (kvClientManager_ != nullptr) {
        kvClientManager_->Set(std::make_shared<SetKVCacheTask>(
            context->key, context->buf, context->len,
            [context](const std::shared_ptr<SetKVCacheTask>&) {
              delete[] context->buf;
            }));
      }
      break;
    default:
      LOG_EVERY_N(ERROR, 1000) << "unsupported warmup storage type";
  }
}

void WarmupManager::CollectMetrics(stub::metric::InterfaceMetric* interface,
                                   int count, uint64_t start) {
  interface->bps.count << count;
  interface->qps.count << 1;
  interface->latency << (butil::cpuwide_time_us() - start);
}

}  // namespace warmup
}  // namespace client
}  // namespace dingofs
