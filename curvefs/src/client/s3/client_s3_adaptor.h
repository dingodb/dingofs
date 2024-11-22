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
 * Project: curve
 * Created Date: 21-5-31
 * Author: huyao
 */
#ifndef CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_
#define CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_

#include <bthread/execution_queue.h>

#include <memory>
#include <string>
#include <vector>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/blockcache/block_cache.h"
#include "curvefs/src/client/blockcache/s3_client.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/filesystem/error.h"
#include "curvefs/src/client/filesystem/filesystem.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/metric/client_metric.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/s3/client_s3_cache_manager.h"
#include "curvefs/src/utils/wait_interval.h"

namespace curvefs {
namespace client {

using ::curvefs::client::blockcache::BlockCache;
using ::curvefs::client::blockcache::S3Client;
using ::curvefs::client::blockcache::StoreType;
using curvefs::client::common::DiskCacheType;
using curvefs::client::common::S3ClientAdaptorOption;
using ::curvefs::client::filesystem::FileSystem;
using curvefs::client::metric::InterfaceMetric;
using curvefs::client::metric::S3Metric;
using curvefs::metaserver::Inode;
using curvefs::metaserver::S3ChunkInfo;
using curvefs::metaserver::S3ChunkInfoList;
using ::curvefs::utils::TaskThreadPool;
using ::curvefs::utils::Thread;
using rpcclient::MdsClient;

class DiskCacheManagerImpl;
class ChunkCacheManager;
struct FlushChunkCacheContext;

class S3ClientAdaptor {
 public:
  S3ClientAdaptor() = default;
  virtual ~S3ClientAdaptor() = default;
  /**
   * @brief Initailize s3 client
   * @param[in] options the options for s3 client
   */
  virtual CURVEFS_ERROR Init(const S3ClientAdaptorOption& option,
                             std::shared_ptr<S3Client> client,
                             std::shared_ptr<InodeCacheManager> inodeManager,
                             std::shared_ptr<MdsClient> mdsClient,
                             std::shared_ptr<FsCacheManager> fsCacheManager,
                             std::shared_ptr<FileSystem> filesystem,
                             std::shared_ptr<BlockCache> block_cache,
                             std::shared_ptr<KVClientManager> kvClientManager,
                             bool startBackGround = false) = 0;
  /**
   * @brief write data to s3
   * @param[in] options the options for s3 client
   */
  virtual int Write(uint64_t inodeId, uint64_t offset, uint64_t length,
                    const char* buf) = 0;
  virtual int Read(uint64_t inodeId, uint64_t offset, uint64_t length,
                   char* buf) = 0;
  virtual CURVEFS_ERROR Truncate(InodeWrapper* inodeWrapper, uint64_t size) = 0;
  virtual void ReleaseCache(uint64_t inodeId) = 0;
  virtual CURVEFS_ERROR Flush(uint64_t inodeId) = 0;
  virtual CURVEFS_ERROR FlushAllCache(uint64_t inodeId) = 0;
  virtual CURVEFS_ERROR FsSync() = 0;
  virtual int Stop() = 0;
  virtual FSStatusCode AllocS3ChunkId(uint32_t fsId, uint32_t idNum,
                                      uint64_t* chunkId) = 0;
  virtual void SetFsId(uint32_t fsId) = 0;
  virtual std::shared_ptr<S3Client> GetS3Client() = 0;
  virtual uint64_t GetBlockSize() = 0;
  virtual uint64_t GetChunkSize() = 0;
  virtual uint32_t GetObjectPrefix() = 0;
  virtual std::shared_ptr<BlockCache> GetBlockCache() = 0;
  virtual bool HasDiskCache() = 0;
};

using FlushChunkCacheCallBack =
    std::function<void(const std::shared_ptr<FlushChunkCacheContext>&)>;

struct FlushChunkCacheContext {
  uint64_t inode;
  ChunkCacheManagerPtr chunkCacheManptr;
  bool force;
  FlushChunkCacheCallBack cb;
  CURVEFS_ERROR retCode;
};

// client use s3 internal interface
class S3ClientAdaptorImpl : public S3ClientAdaptor {
 public:
  S3ClientAdaptorImpl() = default;
  ~S3ClientAdaptorImpl() override { LOG(INFO) << "delete S3ClientAdaptorImpl"; }
  /**
   * @brief Initailize s3 client
   * @param[in] options the options for s3 client
   */
  CURVEFS_ERROR
  Init(const S3ClientAdaptorOption& option, std::shared_ptr<S3Client> client,
       std::shared_ptr<InodeCacheManager> inodeManager,
       std::shared_ptr<MdsClient> mdsClient,
       std::shared_ptr<FsCacheManager> fsCacheManager,
       std::shared_ptr<FileSystem> filesystem,
       std::shared_ptr<BlockCache> block_cache,
       std::shared_ptr<KVClientManager> kvClientManager,
       bool startBackGround = false) override;
  /**
   * @brief write data to s3
   * @param[in] options the options for s3 client
   */
  int Write(uint64_t inodeId, uint64_t offset, uint64_t length,
            const char* buf) override;

  int Read(uint64_t inode_id, uint64_t offset, uint64_t length,
           char* buf) override;

  CURVEFS_ERROR Truncate(InodeWrapper* inodeWrapper, uint64_t size) override;
  void ReleaseCache(uint64_t inodeId) override;
  CURVEFS_ERROR Flush(uint64_t inode_id) override;
  CURVEFS_ERROR FlushAllCache(uint64_t inodeId) override;
  CURVEFS_ERROR FsSync() override;
  int Stop() override;

  uint64_t GetBlockSize() override { return blockSize_; }
  uint64_t GetChunkSize() override { return chunkSize_; }
  uint32_t GetObjectPrefix() override { return objectPrefix_; }

  std::shared_ptr<FsCacheManager> GetFsCacheManager() {
    return fsCacheManager_;
  }
  uint32_t GetFlushInterval() const { return flushIntervalSec_; }
  std::shared_ptr<S3Client> GetS3Client() override { return client_; }
  uint32_t GetPrefetchBlocks() const { return prefetchBlocks_; }

  bool HasDiskCache() override {
    return block_cache_->GetStoreType() == StoreType::DISK;
  }

  std::shared_ptr<InodeCacheManager> GetInodeCacheManager() {
    return inodeManager_;
  }

  std::shared_ptr<FileSystem> GetFileSystem() { return filesystem_; }

  std::shared_ptr<BlockCache> GetBlockCache() override { return block_cache_; }

  FSStatusCode AllocS3ChunkId(uint32_t fsId, uint32_t idNum,
                              uint64_t* chunkId) override;

  void FsSyncSignal() {
    std::lock_guard<std::mutex> lk(mtx_);
    VLOG(3) << "fs sync signal";
    cond_.notify_one();
  }

  void FsSyncSignalAndDataCacheInc() {
    std::lock_guard<std::mutex> lk(mtx_);
    fsCacheManager_->DataCacheNumInc();
    VLOG(3) << "fs sync signal";
    cond_.notify_one();
  }

  void SetFsId(uint32_t fs_id) override { fsId_ = fs_id; }
  uint32_t GetFsId() const { return fsId_; }
  uint32_t GetPageSize() const { return pageSize_; }

  uint32_t GetMaxReadRetryIntervalMs() const { return maxReadRetryIntervalMs_; }

  uint32_t GetReadRetryIntervalMs() const { return readRetryIntervalMs_; }

 private:
  void BackGroundFlush();

  using AsyncDownloadTask = std::function<void()>;

  static int ExecAsyncDownloadTask(
      void* meta, bthread::TaskIterator<AsyncDownloadTask>& iter);  // NOLINT

 public:
  void PushAsyncTask(const AsyncDownloadTask& task) {
    static thread_local unsigned int seed = time(nullptr);

    int idx = rand_r(&seed) % downloadTaskQueues_.size();
    int rc = bthread::execution_queue_execute(downloadTaskQueues_[idx], task);

    if (CURVE_UNLIKELY(rc != 0)) {
      task();
    }
  }

  void Enqueue(std::shared_ptr<FlushChunkCacheContext> context);

 private:
  std::shared_ptr<S3Client> client_;
  uint64_t blockSize_;
  uint64_t chunkSize_;
  uint32_t prefetchBlocks_;
  uint32_t prefetchExecQueueNum_;
  std::string allocateServerEps_;
  uint32_t flushIntervalSec_;
  uint32_t memCacheNearfullRatio_;
  uint32_t throttleBaseSleepUs_;
  uint32_t maxReadRetryIntervalMs_;
  uint32_t readRetryIntervalMs_;
  uint32_t objectPrefix_;
  Thread bgFlushThread_;
  std::atomic<bool> toStop_;
  std::mutex mtx_;
  std::mutex ioMtx_;
  std::condition_variable cond_;
  curvefs::utils::WaitInterval waitInterval_;
  std::shared_ptr<FsCacheManager> fsCacheManager_;
  std::shared_ptr<InodeCacheManager> inodeManager_;
  std::shared_ptr<FileSystem> filesystem_;
  std::shared_ptr<BlockCache> block_cache_;
  std::shared_ptr<MdsClient> mdsClient_;
  uint32_t fsId_;
  std::string fsName_;
  std::vector<bthread::ExecutionQueueId<AsyncDownloadTask>> downloadTaskQueues_;
  uint32_t pageSize_;

  int FlushChunkClosure(std::shared_ptr<FlushChunkCacheContext> context);

  std::shared_ptr<KVClientManager> kvClientManager_ = nullptr;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_
