/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-09-23
 * Author: YangFan (fansehep)
 */

#ifndef DINGOFS_SRC_CLIENT_KVCLIENT_KVCLIENT_MANAGER_H_
#define DINGOFS_SRC_CLIENT_KVCLIENT_KVCLIENT_MANAGER_H_

#include <bthread/condition_variable.h>

#include <memory>
#include <string>
#include <utility>

#include "client/vfs_legacy/kvclient/kvclient.h"
#include "metrics/client/vfs_legacy/kv_client.h"
#include "options/client/vfs_legacy/vfs_legacy_option.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace client {

class KVClientManager;
class SetKVCacheTask;
class GetKVCacheTask;

using SetKVCacheDone =
    std::function<void(const std::shared_ptr<SetKVCacheTask>&)>;
using GetKVCacheDone =
    std::function<void(const std::shared_ptr<GetKVCacheTask>&)>;

struct SetKVCacheTask {
  std::string key;
  const char* value;
  uint64_t length;
  SetKVCacheDone done;
  SetKVCacheTask() = default;
  SetKVCacheTask(
      const std::string& k, const char* val, const uint64_t len,
      SetKVCacheDone done = [](const std::shared_ptr<SetKVCacheTask>&) {})
      : key(k), value(val), length(len), done(std::move(done)) {}
};

struct GetKVCacheTask {
  const std::string& key;
  char* value;
  uint64_t offset;
  uint64_t length;
  bool res;
  GetKVCacheDone done;
  GetKVCacheTask(const std::string& k, char* v, uint64_t off, uint64_t len)
      : key(k), value(v), offset(off), length(len), res(false) {
    done = [](const std::shared_ptr<GetKVCacheTask>&) {};
  }
};

class KVClientManager {
 public:
  KVClientManager() = default;
  ~KVClientManager() { Uninit(); }

  bool Init(const KVClientManagerOpt& config,
            const std::shared_ptr<KVClient>& kvclient);

  /**
   * It will get a db client and set the key value asynchronusly.
   * The set task will push threadpool, you'd better
   * don't get the key immediately.
   */
  void Set(std::shared_ptr<SetKVCacheTask> task);

  void Get(std::shared_ptr<GetKVCacheTask> task);

  metrics::client::vfs_legacy::KVClientMetric* GetClientMetricForTesting() {
    return &kvClientMetric_;
  }

 private:
  void Uninit();

  utils::TaskThreadPool<bthread::Mutex, bthread::ConditionVariable> threadPool_;
  std::shared_ptr<KVClient> client_;
  metrics::client::vfs_legacy::KVClientMetric kvClientMetric_;
};

}  // namespace client
}  // namespace dingofs
#endif  // DINGOFS_SRC_CLIENT_KVCLIENT_KVCLIENT_MANAGER_H_
