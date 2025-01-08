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
 * Date: Wednesday Dec 01 19:30:09 CST 2021
 * Author: wuhanqing
 */

#ifndef DINGOFS_TEST_METASERVER_COPYSET_MOCK_MOCK_COPYSET_NODE_MANAGER_H_
#define DINGOFS_TEST_METASERVER_COPYSET_MOCK_MOCK_COPYSET_NODE_MANAGER_H_

#include <gmock/gmock.h>

#include "metaserver/copyset/copyset_node_manager.h"

namespace dingofs {
namespace metaserver {
namespace copyset {

class MockCopysetNodeManager : public CopysetNodeManager {
 public:
  MOCK_METHOD2(GetCopysetNode, CopysetNode*(PoolId, CopysetId));
  MOCK_METHOD2(PurgeCopysetNode, bool(PoolId, CopysetId));
  MOCK_CONST_METHOD0(IsLoadFinished, bool());
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_TEST_METASERVER_COPYSET_MOCK_MOCK_COPYSET_NODE_MANAGER_H_
