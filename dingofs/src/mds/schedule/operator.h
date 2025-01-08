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
 * @Project: dingo
 * @Date: 2021-11-8 11:01:48
 * @Author: chenwei
 */

#ifndef DINGOFS_SRC_MDS_SCHEDULE_OPERATOR_H_
#define DINGOFS_SRC_MDS_SCHEDULE_OPERATOR_H_

#include "mds/common/mds_define.h"
#include "mds/schedule/operatorStep.h"
#include "mds/schedule/operatorTemplate.h"
#include "mds/schedule/topoAdapter.h"
#include "mds/topology/topology_item.h"

namespace dingofs {
namespace mds {
namespace schedule {
using Operator = OperatorT<MetaServerIdType, CopySetInfo, CopySetConf>;
}  // namespace schedule
}  // namespace mds
}  // namespace dingofs
#endif  // DINGOFS_SRC_MDS_SCHEDULE_OPERATOR_H_
