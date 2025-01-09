#!/usr/bin/env bash

g_metaserver_listen_addr="127.0.0.1:6801"

SetFsQuota() {
    curl "http://${g_metaserver_listen_addr}/MetaServerService/SetFsQuota" \
        -d '{"poolId":1,"copysetId":1,"fsId":1,"quota":{"maxBytes":4096,"maxInodes":1000}}' \
        -s | json_pp
}

GetFsQuota() {
    curl "http://${g_metaserver_listen_addr}/MetaServerService/GetFsQuota" \
        -d '{"poolId":1,"copysetId":1,"fsId":1}' \
        -s | json_pp
}

FlushFsUsage() {
    curl "http://${g_metaserver_listen_addr}/MetaServerService/FlushFsUsage" \
        -d '{"poolId":1,"copysetId":1,"fsId":1,"usage":{"bytes":4096,"inodes":10000}}' \
        -s | json_pp
}

SetDirQuota() {
    curl "http://${g_metaserver_listen_addr}/MetaServerService/SetDirQuota" \
        -d '{"poolId":1,"copysetId":1,"fsId":2,"dirInodeId":200,"quota":{"maxBytes":1,"maxInodes":1}}' \
        -s | json_pp
}

GetDirQuota() {
    curl "http://${g_metaserver_listen_addr}/MetaServerService/GetDirQuota" \
        -d '{"poolId":1,"copysetId":1,"fsId":2,"dirInodeId":100}' \
        -s | json_pp
}

LoadDirQuotas() {
    curl "http://${g_metaserver_listen_addr}/MetaServerService/LoadDirQuotas" \
        -d '{"poolId":1,"copysetId":1,"fsId":2}' \
        -s | json_pp
}

FlushDirUsages() {
    curl "http://${g_metaserver_listen_addr}/MetaServerService/FlushDirUsages" \
        -d '{"poolId":1,"copysetId":1,"fsId":2,"usages":[{"key":100,"value":{"bytes":4096,"inodes":1}}, {"key":200,"value":{"bytes":8192,"inodes":2}}]}' \
        -s | json_pp
}

main() {
    SetFsQuota
    GetFsQuota
    FlushFsUsage
    SetDirQuota
    GetDirQuota
    LoadDirQuotas
    FlushDirUsages
}

main
