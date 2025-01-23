
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
 * Project: Dingofs
 * Date: 2022-02-28
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_TEST_METASERVER_STORAGE_STORAGE_TEST_H_
#define DINGOFS_TEST_METASERVER_STORAGE_STORAGE_TEST_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "dingofs/metaserver.pb.h"
#include "metaserver/dentry_storage.h"
#include "metaserver/storage/storage.h"

namespace dingofs {
namespace metaserver {
namespace storage {

using pb::metaserver::Dentry;

Dentry Value(const std::string& name);

void TestHGet(std::shared_ptr<KVStorage> kvStorage);
void TestHSet(std::shared_ptr<KVStorage> kvStorage);
void TestHDel(std::shared_ptr<KVStorage> kvStorage);
void TestHGetAll(std::shared_ptr<KVStorage> kvStorage);
void TestHSize(std::shared_ptr<KVStorage> kvStorage);
void TestHClear(std::shared_ptr<KVStorage> kvStorage);

void TestSGet(std::shared_ptr<KVStorage> kvStorage);
void TestSSet(std::shared_ptr<KVStorage> kvStorage);
void TestSDel(std::shared_ptr<KVStorage> kvStorage);
void TestSSeek(std::shared_ptr<KVStorage> kvStorage);
void TestSGetAll(std::shared_ptr<KVStorage> kvStorage);
void TestSSize(std::shared_ptr<KVStorage> kvStorage);
void TestSClear(std::shared_ptr<KVStorage> kvStorage);

void TestMixOperator(std::shared_ptr<KVStorage> kvStorage);
void TestTransaction(std::shared_ptr<KVStorage> kvStorage);

}  // namespace storage
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_TEST_METASERVER_STORAGE_STORAGE_TEST_H_
