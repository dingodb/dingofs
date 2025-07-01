// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gmock/gmock.h>

#include "utils/executor/executor.h"

namespace dingofs {

class MockExecutor : public Executor {
 public:
  MOCK_METHOD(bool, Start, (), (override));
  MOCK_METHOD(bool, Stop, (), (override));
  MOCK_METHOD(bool, Execute, (std::function<void()> func), (override));
  MOCK_METHOD(bool, Schedule, (std::function<void()> func, int delay_ms),
              (override));
  MOCK_METHOD(int, ThreadNum, (), (const override));
  MOCK_METHOD(int, TaskNum, (), (const override));
  MOCK_METHOD(std::string, Name, (), (const override));
};

}  // namespace dingofs