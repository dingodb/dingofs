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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef DINGOFS_SRC_CLIENT_DENTRY_CACHE_MANAGER_H_
#define DINGOFS_SRC_CLIENT_DENTRY_CACHE_MANAGER_H_

#include <cstdint>
#include <list>
#include <memory>
#include <string>

#include "client/filesystem/error.h"
#include "stub/rpcclient/metaserver_client.h"
#include "utils/concurrent/generic_name_lock.h"

namespace dingofs {
namespace client {

static const char* kDentryKeyDelimiter = ":";

class DentryCacheManager {
 public:
  DentryCacheManager() : fsId_(0) {}
  virtual ~DentryCacheManager() {}

  void SetFsId(uint32_t fsId) { fsId_ = fsId; }

  virtual filesystem::DINGOFS_ERROR GetDentry(uint64_t parent,
                                              const std::string& name,
                                              pb::metaserver::Dentry* out) = 0;

  virtual filesystem::DINGOFS_ERROR CreateDentry(
      const pb::metaserver::Dentry& dentry) = 0;

  virtual filesystem::DINGOFS_ERROR DeleteDentry(
      uint64_t parent, const std::string& name,
      pb::metaserver::FsFileType type) = 0;

  virtual filesystem::DINGOFS_ERROR ListDentry(
      uint64_t parent, std::list<pb::metaserver::Dentry>* dentryList,
      uint32_t limit, bool onlyDir = false, uint32_t nlink = 0) = 0;

 protected:
  uint32_t fsId_;
};

class DentryCacheManagerImpl : public DentryCacheManager {
 public:
  DentryCacheManagerImpl()
      : metaClient_(std::make_shared<stub::rpcclient::MetaServerClientImpl>()) {
  }

  explicit DentryCacheManagerImpl(
      const std::shared_ptr<stub::rpcclient::MetaServerClient>& metaClient)
      : metaClient_(metaClient) {}

  filesystem::DINGOFS_ERROR GetDentry(uint64_t parent, const std::string& name,
                                      pb::metaserver::Dentry* out) override;

  filesystem::DINGOFS_ERROR CreateDentry(
      const pb::metaserver::Dentry& dentry) override;

  filesystem::DINGOFS_ERROR DeleteDentry(
      uint64_t parent, const std::string& name,
      pb::metaserver::FsFileType type) override;

  filesystem::DINGOFS_ERROR ListDentry(
      uint64_t parent, std::list<pb::metaserver::Dentry>* dentryList,
      uint32_t limit, bool dirOnly = false, uint32_t nlink = 0) override;

  std::string GetDentryCacheKey(uint64_t parent, const std::string& name) {
    return std::to_string(parent) + kDentryKeyDelimiter + name;
  }

 private:
  std::shared_ptr<stub::rpcclient::MetaServerClient> metaClient_;

  dingofs::utils::GenericNameLock<utils::Mutex> nameLock_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_DENTRY_CACHE_MANAGER_H_
