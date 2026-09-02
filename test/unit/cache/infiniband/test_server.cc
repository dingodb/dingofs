/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2026-09-02
 * Author: AI
 */

#include <brpc/controller.h>
#include <gtest/gtest.h>

#include <string>

#include "cache/infiniband/server.h"
#include "cache/infiniband/service.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class InfinibandServiceImplTest : public ::testing::Test {
 protected:
  // Sync() with an unparsable cm meta never reaches the listener, so it is
  // safe to call without RDMA hardware whether the service runs or not.
  std::string Sync(InfinibandServiceImpl* service) {
    brpc::Controller cntl;
    pb::infiniband::SyncRequest request;
    request.mutable_cm_meta()->set_gid("too-short");
    pb::infiniband::SyncResponse response;
    service->Sync(&cntl, &request, &response, nullptr);
    return cntl.Failed() ? cntl.ErrorText() : "";
  }

  Listener listener_;
  ServiceHub hub_;
};

TEST_F(InfinibandServiceImplTest, SyncRefusedUnlessRunning) {
  InfinibandServiceImpl service(&listener_, &hub_);
  EXPECT_NE(Sync(&service).find("not running"), std::string::npos);

  service.Start();
  EXPECT_NE(Sync(&service).find("invalid gid size"), std::string::npos);

  service.Shutdown();
  EXPECT_NE(Sync(&service).find("not running"), std::string::npos);
}

TEST_F(InfinibandServiceImplTest, StartAndShutdown) {
  InfinibandServiceImpl service(&listener_, &hub_);

  {  // idempotent
    service.Start();
    service.Start();
    service.Shutdown();
    service.Shutdown();
  }

  {  // restartable
    service.Start();
    EXPECT_NE(Sync(&service).find("invalid gid size"), std::string::npos);
    service.Shutdown();
    EXPECT_NE(Sync(&service).find("not running"), std::string::npos);
  }

  {  // never started
    InfinibandServiceImpl idle(&listener_, &hub_);
    idle.Shutdown();
  }
}

TEST_F(InfinibandServiceImplTest, DestructorShutsDown) {
  InfinibandServiceImpl service(&listener_, &hub_);
  service.Start();
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
