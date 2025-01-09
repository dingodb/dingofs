
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

#include "client/filesystem/dir_parent_watcher.h"
#include "gmock/gmock.h"

namespace dingofs {
namespace client {
namespace filesystem {

class MockDirParentWatcher : public DirParentWatcher {
 public:
  MOCK_METHOD(void, Remeber, (Ino ino, Ino parent), (override));
  MOCK_METHOD(void, Forget, (Ino ino), (override));
  MOCK_METHOD(DINGOFS_ERROR, GetParent, (Ino ino, Ino& parent), (override));
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs