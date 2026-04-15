#!/bin/bash
# 脚本名: stat_rpc_latency_full.sh
# 功能: 统计 TxnBatchGetRpc, TxnGetRpc, TxnPrewriteRpc, TxnScanRpc 的出现次数
#       以及各 RPC 的 total_time_us, total_phase_time_us, raft_commit_time_us 
#       分别超过 10ms 和 100ms 的个数
# 用法: ./stat_rpc_latency_full.sh [logfile1] [logfile2] ...  若不指定文件，则统计当前目录下所有 *.log 文件

if [ $# -eq 0 ]; then
    files=(*.log)
else
    files=("$@")
fi

if [ ${#files[@]} -eq 0 ] || [ ! -f "${files[0]}" ]; then
    echo "未找到任何日志文件，请检查文件路径或通配符。" >&2
    exit 1
fi

awk '
BEGIN {
    split("TxnBatchGetRpc TxnGetRpc TxnPrewriteRpc TxnCommitRpc TxnScanRpc", rpc_list, " ")
    for (i in rpc_list) {
        name = rpc_list[i]
        total[name] = 0
        total_gt10[name] = 0
        total_gt100[name] = 0
        phase_gt10[name] = 0
        phase_gt100[name] = 0
        raft_gt10[name] = 0
        raft_gt100[name] = 0
    }
}

{
    for (i in rpc_list) {
        rpc = rpc_list[i]
        if (index($0, rpc)) {
            total[rpc]++

            # total_time_us(数字)
            if (match($0, /total_time_us\(([0-9]+)\)/, arr)) {
                t = arr[1] + 0
                if (t > 10000) total_gt10[rpc]++
                if (t > 100000) total_gt100[rpc]++
            }

            # total_phase_time_us(数字)
            if (match($0, /total_phase_time_us\(([0-9]+)\)/, arr)) {
                t = arr[1] + 0
                if (t > 10000) phase_gt10[rpc]++
                if (t > 100000) phase_gt100[rpc]++
            }

            # raft_commit_time_us(数字)
            if (match($0, /raft_commit_time_us\(([0-9]+)\)/, arr)) {
                t = arr[1] + 0
                if (t > 10000) raft_gt10[rpc]++
                if (t > 100000) raft_gt100[rpc]++
            }

            break  # 一行只匹配第一个 RPC 类型
        }
    }
}

END {
    printf "%-20s %10s %15s %15s %15s %15s %15s %15s\n", "RPC Type", "Total", "total>10ms", "total>100ms", "phase>10ms", "phase>100ms", "raft>10ms", "raft>100ms"
    printf "%-20s %10s %15s %15s %15s %15s %15s %15s\n", "--------", "-----", "----------", "-----------", "----------", "-----------", "---------", "----------"
    for (i in rpc_list) {
        rpc = rpc_list[i]
        printf "%-20s %10d %15d %15d %15d %15d %15d %15d\n", rpc, total[rpc], total_gt10[rpc], total_gt100[rpc], phase_gt10[rpc], phase_gt100[rpc], raft_gt10[rpc], raft_gt100[rpc]
    }
}' "${files[@]}"
