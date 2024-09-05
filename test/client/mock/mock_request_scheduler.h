/**
 * Project: curve
 * Date: Tue May 19 14:07:57 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 NetEase
 */

#ifndef TEST_CLIENT_MOCK_MOCK_REQUEST_SCHEDULER_H_
#define TEST_CLIENT_MOCK_MOCK_REQUEST_SCHEDULER_H_

#include "gmock/gmock.h"
#include "src/client/request_scheduler.h"

namespace curve {
namespace client {

class MockRequestScheduler : public RequestScheduler {
 public:
  MOCK_METHOD1(ReSchedule, int(RequestContext* ctx));
};

}  // namespace client
}  // namespace curve

#endif  // TEST_CLIENT_MOCK_MOCK_REQUEST_SCHEDULER_H_
