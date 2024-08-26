/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"){}
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
 * Project: curve
 * Created Date: Thur Oct Fri 29 2021
 * Author: lixiaocui
 */

#ifndef SRC_COMMON_DUMMYSERVER_H_
#define SRC_COMMON_DUMMYSERVER_H_

#include <sstream>
#include <mutex>
#include <brpc/server.h>
#include <bvar/bvar.h>

namespace curve {
namespace common {

bool StartBrpcDummyserver(uint32_t dummyServerStartPort,
                          uint32_t dummyServerEndPort, uint32_t *listenPort) {
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        while (dummyServerStartPort < dummyServerEndPort) {
            int ret = brpc::StartDummyServerAt(dummyServerStartPort);
            if (ret >= 0) {
                LOG(INFO) << "Start dummy server success, listen port = "
                          << dummyServerStartPort;
                *listenPort = dummyServerStartPort;
                break;
            }

            ++dummyServerStartPort;
        }
    });

    if (dummyServerStartPort >= dummyServerEndPort) {
        LOG(ERROR) << "Start dummy server failed!";
        return false;
    }

    return true;
}

class MetricsDumper : public bvar::Dumper {
private:
    std::vector<std::pair<std::string, std::string> > _metricList;
public:
    bool dump(const std::string& name,
              const butil::StringPiece& description) {
        _metricList.push_back(std::make_pair(name, description.as_string()));
        return true;
    }

    std::string contents() const {
        std::ostringstream oss;
        for (auto& metric : _metricList) {
            oss << metric.first << " : " << metric.second << "\n";
        }
        return oss.str();
    }  
};



}  // namespace common
}  // namespace curve


#endif  // SRC_COMMON_DUMMYSERVER_H_
