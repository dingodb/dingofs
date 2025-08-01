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
 * Project: Dingofs
 * Created Date: 2023-03-29
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_TEST_CLIENT_FILESYSTEM_HELPER_BUILDER_H_
#define DINGOFS_TEST_CLIENT_FILESYSTEM_HELPER_BUILDER_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>

#include "client/vfs_legacy/filesystem/filesystem.h"
#include "client/vfs_legacy/mock_dentry_cache_mamager.h"
#include "client/vfs_legacy/mock_inode_cache_manager.h"
#include "client/vfs_legacy/mock_metaserver_client.h"
#include "options/client/vfs_legacy/vfs_legacy_option.h"
#include "stub/rpcclient/mock_mds_client.h"

namespace dingofs {
namespace client {
namespace filesystem {

using dingofs::stub::rpcclient::MockMdsClient;
using dingofs::stub::rpcclient::MockMetaServerClient;

const uint32_t kMockFsId = 100;
static const char* kMockFsName = "dingofs";

class DeferSyncBuilder {
 public:
  using Callback = std::function<void(DeferSyncOption* option)>;

  static DeferSyncOption DefaultOption() {
    return DeferSyncOption{
      delay : 3,
      deferDirMtime : false,
    };
  }

 public:
  DeferSyncBuilder()
      : option_(DefaultOption()),
        dentryManager_(std::make_shared<MockDentryCacheManager>()),
        inodeManager_(std::make_shared<MockInodeCacheManager>()) {}

  DeferSyncBuilder SetOption(Callback callback) {
    callback(&option_);
    return *this;
  }

  std::shared_ptr<DeferSync> Build() {
    return std::make_shared<DeferSync>(option_);
  }

  std::shared_ptr<MockDentryCacheManager> GetDentryManager() {
    return dentryManager_;
  }

  std::shared_ptr<MockInodeCacheManager> GetInodeManager() {
    return inodeManager_;
  }

 private:
  DeferSyncOption option_;
  std::shared_ptr<MockDentryCacheManager> dentryManager_;
  std::shared_ptr<MockInodeCacheManager> inodeManager_;
};

class DirCacheBuilder {
 public:
  using Callback = std::function<void(DirCacheOption* option)>;

  static DirCacheOption DefaultOption() {
    return DirCacheOption{
      lruSize : 5000000,
    };
  }

 public:
  DirCacheBuilder() : option_(DefaultOption()) {}

  DirCacheBuilder SetOption(Callback callback) {
    callback(&option_);
    return *this;
  }

  std::shared_ptr<DirCache> Build() {
    return std::make_shared<DirCache>(option_);
  }

 private:
  DirCacheOption option_;
};

class OpenFilesBuilder {
 public:
  using Callback = std::function<void(OpenFilesOption* option)>;

  static OpenFilesOption DefaultOption() {
    return OpenFilesOption{
      lruSize : 65535,
      deferSyncSecond : 3,
    };
  }

 public:
  OpenFilesBuilder()
      : option_(DefaultOption()), deferSync_(DeferSyncBuilder().Build()) {}

  OpenFilesBuilder SetOption(Callback callback) {
    callback(&option_);
    return *this;
  }

  std::shared_ptr<OpenFiles> Build() {
    return std::make_shared<OpenFiles>(option_, deferSync_);
  }

 private:
  std::shared_ptr<DeferSync> deferSync_;
  OpenFilesOption option_;
};

class RPCClientBuilder {
 public:
  using Callback = std::function<void(RPCOption* option)>;

  static RPCOption DefaultOption() {
    return RPCOption{listDentryLimit : 65535};
  }

  RPCClientBuilder()
      : option_(DefaultOption()),
        dentryManager_(std::make_shared<MockDentryCacheManager>()),
        inodeManager_(std::make_shared<MockInodeCacheManager>()),
        meta_client_(std::make_shared<MockMetaServerClient>()) {}

  RPCClientBuilder SetOption(Callback callback) {
    callback(&option_);
    return *this;
  }

  std::shared_ptr<RPCClient> Build() {
    ExternalMember member(dentryManager_, inodeManager_, meta_client_,
                          mds_client_);
    return std::make_shared<RPCClient>(option_, member);
  }

  std::shared_ptr<MockDentryCacheManager> GetDentryManager() {
    return dentryManager_;
  }

  std::shared_ptr<MockInodeCacheManager> GetInodeManager() {
    return inodeManager_;
  }

 private:
  RPCOption option_;
  std::shared_ptr<MockDentryCacheManager> dentryManager_;
  std::shared_ptr<MockInodeCacheManager> inodeManager_;
  std::shared_ptr<MockMetaServerClient> meta_client_;
  std::shared_ptr<MockMdsClient> mds_client_;
};

// build filesystem which you want
class FileSystemBuilder {
 public:
  using Callback = std::function<void(FileSystemOption* option)>;

  FileSystemOption DefaultOption() {
    auto option = FileSystemOption();
    auto kernelCacheOption = KernelCacheOption{
      entryTimeoutSec : 3600,
      dirEntryTimeoutSec : 3600,
      attrTimeoutSec : 3600,
      dirAttrTimeoutSec : 3600,
    };
    auto lookupCacheOption = LookupCacheOption{
      lruSize : 100000,
      negativeTimeoutSec : 0,
    };
    auto attrWatcherOption = AttrWatcherOption{
      lruSize : 5000000,
    };

    option.cto = true;
    option.disableXAttr = true;
    option.maxNameLength = 255;
    option.blockSize = 0x10000u;
    option.kernelCacheOption = kernelCacheOption;
    option.lookupCacheOption = lookupCacheOption;
    option.dirCacheOption = DirCacheBuilder::DefaultOption();
    option.openFilesOption = OpenFilesBuilder::DefaultOption();
    option.attrWatcherOption = attrWatcherOption;
    option.rpcOption = RPCClientBuilder::DefaultOption();
    option.deferSyncOption = DeferSyncBuilder::DefaultOption();
    return option;
  }

  FileSystemBuilder()
      : option_(DefaultOption()),
        dentryManager_(std::make_shared<MockDentryCacheManager>()),
        inodeManager_(std::make_shared<MockInodeCacheManager>()),
        meta_client_(std::make_shared<MockMetaServerClient>()) {}

  FileSystemBuilder SetOption(Callback callback) {
    callback(&option_);
    return *this;
  }

  std::shared_ptr<FileSystem> Build() {
    auto member = ExternalMember(dentryManager_, inodeManager_, meta_client_,
                                 mds_client_);
    return std::make_shared<FileSystem>(kMockFsId, kMockFsName, option_,
                                        member);
  }

  std::shared_ptr<MockDentryCacheManager> GetDentryManager() {
    return dentryManager_;
  }

  std::shared_ptr<MockInodeCacheManager> GetInodeManager() {
    return inodeManager_;
  }

 private:
  FileSystemOption option_;
  std::shared_ptr<MockDentryCacheManager> dentryManager_;
  std::shared_ptr<MockInodeCacheManager> inodeManager_;
  std::shared_ptr<MockMetaServerClient> meta_client_;
  std::shared_ptr<MockMdsClient> mds_client_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_FILESYSTEM_HELPER_BUILDER_H_
