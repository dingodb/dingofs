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
 * Date: Sun 22 Aug 2021 10:40:42 AM CST
 * Author: wuhanqing
 */

#ifndef DINGOFS_TEST_METASERVER_COPYSET_MOCK_MOCK_COPYSET_SERVICE_H_
#define DINGOFS_TEST_METASERVER_COPYSET_MOCK_MOCK_COPYSET_SERVICE_H_

#include <gmock/gmock.h>

#include "proto/copyset.pb.h"

namespace dingofs {
namespace metaserver {
namespace copyset {

class MockCopysetService : public pb::metaserver::copyset::CopysetService {
 public:
  MOCK_METHOD4(
      CreateCopysetNode,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::pb::metaserver::copyset::CreateCopysetRequest*
               request,
           ::dingofs::pb::metaserver::copyset::CreateCopysetResponse* response,
           ::google::protobuf::Closure* done));

  MOCK_METHOD4(
      GetCopysetStatus,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::pb::metaserver::copyset::CopysetStatusRequest*
               request,
           ::dingofs::pb::metaserver::copyset::CopysetStatusResponse* response,
           ::google::protobuf::Closure* done));
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_TEST_METASERVER_COPYSET_MOCK_MOCK_COPYSET_SERVICE_H_
