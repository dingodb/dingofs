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
 * Created Date: 2026-04-20
 * Author: AI
 */

#include "cache/infiniband/infiniband.h"

#include <gtest/gtest.h>
#include <infiniband/verbs.h>

namespace dingofs {
namespace cache {
namespace infiniband {

class DeviceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    devices_ = ibv_get_device_list(&num_devices_);
    has_hardware_ = (devices_ != nullptr) && (num_devices_ > 0);
  }

  void TearDown() override {
    if (devices_ != nullptr) {
      ibv_free_device_list(devices_);
    }
  }

  ibv_device** devices_{nullptr};
  int num_devices_{0};
  bool has_hardware_ = false;
};

TEST_F(DeviceTest, Open) {
  if (!has_hardware_) {
    GTEST_SKIP() << "no infiniband device available";
  }

  auto device = Device::Open(ibv_get_device_name(devices_[0]));
  EXPECT_NE(device, nullptr);
}

TEST_F(DeviceTest, AllocProtectDomain) {
  if (!has_hardware_) {
    GTEST_SKIP() << "no infiniband device available";
  }

  auto device = Device::Open(ibv_get_device_name(devices_[0]));
  ASSERT_NE(device, nullptr);

  auto pd = ProtectDomain::Alloc(device.get());
  EXPECT_NE(pd, nullptr);
}

TEST_F(DeviceTest, QueryActivePortIfAvailable) {
  if (!has_hardware_) {
    GTEST_SKIP() << "no infiniband device available";
  }

  auto device = Device::Open(ibv_get_device_name(devices_[0]));
  ASSERT_NE(device, nullptr);

  ibv_device_attr attr;
  ASSERT_EQ(ibv_query_device(device->GetIbContext(), &attr), 0);
  for (uint8_t port_num = 1; port_num <= attr.phys_port_cnt; ++port_num) {
    auto port = Port::Query(device.get(), port_num);
    if (port != nullptr && port->GetPortState() == PortState::kActive) {
      EXPECT_NE(port->GetLinkLayer(), LinkLayer::kUnspecified);
      return;
    }
  }
  GTEST_SKIP() << "no active infiniband port available";
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
