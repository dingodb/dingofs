# Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The .proto contract layer: how a verb becomes an opcode (proto_binding),
# how a payload is encoded (codec), and the typed bindings over it --
# Stub on the calling side, ProtoService on the serving side.
add_library(blockcache_protocol
    proto_binding.cc
)

target_compile_features(blockcache_protocol PRIVATE cxx_std_20)

target_link_libraries(blockcache_protocol
    blockcache_net
    protobuf::libprotobuf
)

blockcache_tune(blockcache_protocol)
