/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-04
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include "utils/message_queue.h"

namespace dingofs {
namespace base {
namespace queue {

class MessageQueueTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(MessageQueueTest, Basic) {
  auto mq = std::make_shared<utils::MessageQueue<int>>("test", 10);

  std::vector<int> receive;
  mq->Subscribe(
      [&receive](const int& number) { receive.emplace_back(number); });
  mq->Start();

  mq->Publish(1);
  mq->Publish(2);
  mq->Publish(3);
  mq->Stop();

  std::vector<int> expected{1, 2, 3};
  ASSERT_EQ(receive, expected);
}

TEST_F(MessageQueueTest, PublishAfterStop) {
  auto mq = std::make_shared<MessageQueue<int>>("test", 10);

  std::vector<int> receive;
  mq->Subscribe(
      [&receive](const int& number) { receive.emplace_back(number); });
  mq->Start();

  mq->Publish(1);
  mq->Publish(2);
  mq->Publish(3);
  mq->Stop();

  // The message queue will not consume any more messages after it has stopped
  mq->Publish(4);

  std::vector<int> expected{1, 2, 3};
  ASSERT_EQ(receive, expected);
}

}  // namespace queue
}  // namespace base
}  // namespace dingofs
