/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2024-08-20
 * Author: Jingli Chen (Wine93)
 */

#include <map>

#include "curvefs/src/client/bcache/error.h"
#include "curvefs/src/client/bcache/block_cache.h"

namespace curvefs {
namespace client {
namespace bcache {

DiskCacheManager::DiskCacheManager(uint64_t capacity,
                                   double spaceFreeRatio,
                                   std::shared_ptr<DiskCacheLayout> layout)
    : cacheDir_(cacheDir),
      capacity_(capacity),
      spaceFreeRatio_(spaceFreeRatio)
      handler_(handler),
      used_(0),
      stageFull_(false),
      cacheFull_(false),
      fs_(std::make_unique<LocalFileSystem>()) {
    mq_ = std::make_shared<MessageQueueType>("disk_cache", 10);
    mq_->Subscribe([&](const std::shared_ptr<DeleteBatcher>& batcher){
        for (const auto& key : batcher.keys) {
            HandleEvict(key);
        }
    });
}

BCACHE_ERROR DiskCacheManager::Start() {
    mq_->Start();
    if (!running_.exchange(true)) {
        thread_ = std::thread(&DiskCacheManager::CheckFreeSpace, this);
        LOG(INFO) << "DiskCache manager thread start success.";
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCacheManager::Stop() {
    mq_->Stop();
    if (running_.exchange(false)) {
        LOG(INFO) << "Stop disk cache manager thread...";
        sleeper_.interrupt();
        thread_.join();
        LOG(INFO) << "Disk cache manager thread stopped";
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCacheManager::Add(const std::string& key,
                                   size_t size,
                                   TimeSpec atime) {
    keys_[key] = CacheItem{ size, atime };
}

BCACHE_ERROR DiskCacheManager::Exists(const std::string& key) {
    if (keys_.find(key) != keys_.end()) {
        return BCACHE_ERROR::OK;
    }
    return BCACHE_ERROR::NOT_FOUND;
}

BCACHE_ERROR DiskCacheManager::Remove(const std::string& key) {
    keys.erase(key);
    return BCACHE_ERROR::OK;
}

void DiskCacheManager::CleanupFull() {
    double watermark = 1.0 - spaceFreeRatio_;
    uint64_t goalBytes = GetUsageBytes() * 0.95;
    uint64_t goalFiles = GetUsageFiles() * 0.95;
    goalBytes = std::min(goalBytes, totalBytes * watermark);
    goalFiles = std::min(goalFiles, totalFiles * watermark);

    auto batcher = std::make_shared<DeleteBacther>();
    for (const auto& it : keys) {  // FIXME(Wine93)
        keys.erase(it);
        AddUsedBytes(-it.second.size);
        AddFilesBytes(-1);
        batcher->Insert(it.first);
        if (GetUsedBytes() < goalBytes && GetUsedFiles() < goalFiles) {
            break;
        }
    }
    mq_->Publish(batcher);
}

void DiskCacheManager::CheckFreeSpace() {
    LocalFileSystem::StatFS statfs;
    bool running = true;
    while (running) {
        auto rc = fs_->StatFS(cachDir_, &statfs);
        if (rc != BCACHE_ERROR::OK) {
            LOG(ERROR) << "Check free space failed."
            running = sleeper_.wait_for(std::chrono::seconds(1));
            continue;
        }

        double br = statfs.freeBytesRatio;
        double fr = statfs.filesBytesRatio;
        bool cacheFull = br < spaceFreeRatio_ || fr <  spaceFreeRatio_;
        bool stageFull = (br < spaceFreeRatio_ / 2) ||
                         (fr < spaceFreeRatio_ / 2);
        cacheFull_.store(cacheFull, std::memory_order_release);
        stageFull_.store(stageFull, std::memory_order_release);
        if (cacheFull) {
            LOG(WARN) << std::fixed << std::setprecision(2)
                      << "Disk usage is so high, dir=" << layout_->GetRootDir()
                      << ", watermark=" << (1.0 - spaceFreeRatio_) * 100 << "%"
                      << ", bytes usage=" << (1.0 - br) * 100 << "%"
                      << ", files usage=" << (1.0 - fr) * 100 << "%.";
            CleanupFull();
        }
        running = sleeper_.wait_for(std::chrono::seconds(1));
    }
}

void DiskCacheManager::HandleEvict(const BlockKey& key) {
    std::string filepath = layout_->GetCachePath(key);
    auto rc = fs_->RemoveFile(filepath);
    if (rc != BCACHE_ERROR::OK) {

    }
}

inline void DiskCacheManager::AddUsedBytes(uint64_t bytes) {
    usedBytes_.fetch_add(bytes, std::memory_order_release);
}

inline void DiskCacheManager::AddUsedFiles(uint64_t files) {
    usedFiles_.fetch_add(files, std::memory_order_release);
}

inline uint64_t DiskCacheManager::GetUsedBytes() {
    usedBytes_.load(std::memory_order_acquire);
}

inline uint64_t DiskCacheManager::GetUsedFiles() {
    usedFiles_.load(std::memory_order_acquire);
}

inline bool DiskCacheManager::StageFull() const {
    return stageFull_.load(std::memory_order_acquire);
}

inline bool DiskCacheManager::CacheFull() const {
    return cacheFull_.load(std::memory_order_acquire);
}

}  // namespace bcache
}  // namespace client
}  // namespace curvefs
