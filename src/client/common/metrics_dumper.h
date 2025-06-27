// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_SRC_CLIENT_COMMON_METRICS_DUMPER_H_
#define DINGOFS_SRC_CLIENT_COMMON_METRICS_DUMPER_H_

#include <brpc/server.h>
#include <bvar/bvar.h>

#include <sstream>

namespace dingofs {
namespace client {

class MetricsDumper : public bvar::Dumper {
 private:
  std::vector<std::pair<std::string, std::string> > metricList_;

 public:
  bool dump(const std::string& name,
            const butil::StringPiece& description) override {
    metricList_.push_back(std::make_pair(name, description.as_string()));
    return true;
  }

  std::string Contents() const {
    std::ostringstream oss;
    for (const auto& metric : metricList_) {
      oss << metric.first << " : " << metric.second << "\n";
    }
    return oss.str();
  }
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_COMMON_DUMMYSERVER_H_
