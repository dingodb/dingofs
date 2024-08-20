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

#ifndef CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_MANAGER_H_
#define CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_MANAGER_H_

#include <string>

#include "curvefs/src/base/time/time.h"
#include "curvefs/src/base/queue/message_queue.h"
#include "curvefs/src/client/bcache/cache_store.h"

namespace curvefs {
namespace client {
namespace bcache {

struct DeletBatcher {
    void Insert(const BlockKey& key) {
        keys.emplace_back(key);
    }
    std::vector<BlockKeys> keys;
};

// manage cache capacity
class DiskCacheManager {
 public:
    using MessageType = std::shared_ptr<DeleteBatcher>;
    using MessageQueueType = MessageQueue<MessageType>;

    struct CacheItem {
        size_t size;
        TimeSpec atime;
    };

    struct DeletBatcher {
        void Insert(const BlockKey& key) {
            keys.emplace_back(key);
        }
        std::vector<BlockKeys> keys;
    };

 public:
    DiskCacheManager(std::string cacheDir,
                     uint64_t capacity,
                     double spaceFreeRatio,
                     std::shared_ptr<LocalFileSystem> fs);

    BCACHE_ERROR Start();

    BCACHE_ERROR Stop();

    BCACHE_ERROR Add(const BlockKey& key, size_t size, TimeSpec atime);

    BCACHE_ERROR Exists(const BlockKey& key);

    BCACHE_ERROR Remove(const BlockKey& key);

    bool StageFull() const;

    bool CacheFull() const;

 private:
    void CleanupFull();

    void CheckFreeSpace();

    void HandleEvict(const BlockKey& key);

    BCACHE_ERROR GetFreeRatio(double* bytesFreeRatio,
                              double* filesFreeRatio);

    // atomic wrapper with memory order
    void AddUsedBytes(uint64_t bytes);

    void AddUsedFiles(uint64_t files);

    uint64_t GetUsedBytes();

    uint64_t GetUsedFiles();

 private:
    std::string cacheDir_;
    uint64_t capacity_;
    double spaceFreeRatio_;
    EvictHandler handler_;
    std::atomic<uint64_t> usedBytes_;
    std::atomic<uint64_t> usedFiles_;
    std::atomic<bool> stageFull_;
    std::atomic<bool> cacheFull_;
    std::atomic<bool> running_;
    std::thread thread_;
    InterruptibleSleeper sleeper_;
    std::shared_ptr<LocalFileSystem> fs_;
    std::shared_ptr<MessageQueueType> mq_;
    std::shared_ptr<DiskCacheMetric> metric_;
    std::unordered_map<BlockKey, CacheItem> keys_;  // TODO: lru
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_MANAGER_H_