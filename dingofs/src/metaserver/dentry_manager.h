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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef DINGOFS_SRC_METASERVER_DENTRY_MANAGER_H_
#define DINGOFS_SRC_METASERVER_DENTRY_MANAGER_H_

#include <memory>
#include <string>
#include <vector>

#include "proto/metaserver.pb.h"
#include "metaserver/dentry_storage.h"
#include "metaserver/transaction.h"

namespace dingofs {
namespace metaserver {
class DentryManager {
 public:
  DentryManager(std::shared_ptr<DentryStorage> dentryStorage,
                std::shared_ptr<TxManager> txManger);

  pb::metaserver::MetaStatusCode CreateDentry(
      const pb::metaserver::Dentry& dentry);

  // only invoked from snapshot loadding
  pb::metaserver::MetaStatusCode CreateDentry(
      const pb::metaserver::DentryVec& vec, bool merge);

  pb::metaserver::MetaStatusCode DeleteDentry(
      const pb::metaserver::Dentry& dentry);

  pb::metaserver::MetaStatusCode GetDentry(pb::metaserver::Dentry* dentry);

  pb::metaserver::MetaStatusCode ListDentry(
      const pb::metaserver::Dentry& dentry,
      std::vector<pb::metaserver::Dentry>* dentrys, uint32_t limit,
      bool onlyDir = false);

  void ClearDentry();

  pb::metaserver::MetaStatusCode HandleRenameTx(
      const std::vector<pb::metaserver::Dentry>& dentrys);

 private:
  void Log4Dentry(const std::string& request,
                  const pb::metaserver::Dentry& dentry);
  void Log4Code(const std::string& request, pb::metaserver::MetaStatusCode rc);

  std::shared_ptr<DentryStorage> dentryStorage_;
  std::shared_ptr<TxManager> txManager_;
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_DENTRY_MANAGER_H_
