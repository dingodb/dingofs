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
 * Date: 2021-12-28
 * Author: xuchaojie
 */

#ifndef DINGOFS_TEST_METASERVER_TEST_HELPER_H_
#define DINGOFS_TEST_METASERVER_TEST_HELPER_H_

#include "proto/metaserver.pb.h"

namespace dingofs {
namespace metaserver {

using pb::metaserver::Inode;
using pb::metaserver::UpdateInodeRequest;

UpdateInodeRequest MakeUpdateInodeRequestFromInode(const Inode& inode,
                                                   uint32_t poolId = 0,
                                                   uint32_t copysetId = 0,
                                                   uint32_t partitionId = 0);

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_TEST_METASERVER_TEST_HELPER_H_
