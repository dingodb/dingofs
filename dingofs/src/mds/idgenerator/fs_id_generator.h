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
 * Created Date: Wed Jul 28 10:51:34 CST 2021
 * Author: wuhanqing
 */

#ifndef DINGOFS_SRC_MDS_IDGENERATOR_FS_ID_GENERATOR_H_
#define DINGOFS_SRC_MDS_IDGENERATOR_FS_ID_GENERATOR_H_

#include <memory>

#include "mds/idgenerator/etcd_id_generator.h"
#include "mds/common/storage_key.h"

namespace dingofs {
namespace mds {

class FsIdGenerator {
 public:
  explicit FsIdGenerator(
      const std::shared_ptr<dingofs::kvstorage::KVStorageClient>& client)
      : generator_(new dingofs::idgenerator::EtcdIdGenerator(
            client, FS_ID_KEY_PREFIX, FS_ID_INIT, FS_ID_ALLOCATE_BUNDLE)) {}

  bool GenFsId(uint64_t* id) { return generator_->GenID(id); }

 private:
  static constexpr uint64_t FS_ID_INIT = 0;
  static constexpr uint64_t FS_ID_ALLOCATE_BUNDLE = 100;

 private:
  std::unique_ptr<dingofs::idgenerator::EtcdIdGenerator> generator_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_IDGENERATOR_FS_ID_GENERATOR_H_
