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
 * Date: Thursday Jun 09 10:34:04 CST 2022
 * Author: wuhanqing
 */

#ifndef DINGOFS_SRC_METASERVER_STORAGE_ROCKSDB_OPTIONS_H_
#define DINGOFS_SRC_METASERVER_STORAGE_ROCKSDB_OPTIONS_H_

#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace dingofs {
namespace utils {
class Configuration;
}  // namespace utils
}  // namespace dingofs

namespace dingofs {
namespace metaserver {
namespace storage {

// Parse rocksdb related options from conf
void ParseRocksdbOptions(dingofs::utils::Configuration* conf);

void InitRocksdbOptions(
    rocksdb::DBOptions* options,
    std::vector<rocksdb::ColumnFamilyDescriptor>* columnFamilies,
    bool createIfMissing = true, bool errorIfExists = false);

}  // namespace storage
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_STORAGE_ROCKSDB_OPTIONS_H_
