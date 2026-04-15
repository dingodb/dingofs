!/usr/bin/env python3
"""
统计日志中 RPC phase 的性能指标，并增加 raft_commit_time_us 统计（仅 TxnPrewriteRpc / TxnCommitRpc）
用法: python3 stat_phase_metrics.py [--csv|--markdown] logfile1 logfile2 ...
默认输出美观的对齐表格，添加 --csv 输出 CSV 格式，添加 --markdown 输出 Markdown 表格（相同 RPC+Phase 合并单元格）
"""

import re
import sys
import os
from collections import defaultdict

# 需要统计的 RPC 类型
TARGET_RPCS = {'TxnGetRpc', 'TxnBatchGetRpc', 'TxnPrewriteRpc', 'TxnCommitRpc', 'TxnScanRpc'}

# phase 行的正则表达式：匹配 "name(数字 数字 数字 数字 数字 数字)"
PHASE_PATTERN = re.compile(
    r'(\S+?)\((\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\)'
)

# 指标名称（按输出顺序）
METRICS = [
    'time_us',
    'skip_version',
    'io_time_us',
    'miss_block_count',
    'internal_skipped_count',
    'internal_delete_skipped_count'
]

# 需要统计 raft_commit_time_us 的 RPC（这些 RPC 会额外产生一个 'raft_commit' Phase）
RAFT_COMMIT_RPCS = {'TxnPrewriteRpc', 'TxnCommitRpc'}

def compute_stats(values):
    """计算一组数值的 min, avg, max, p50, p90, p99"""
    if not values:
        return [None] * 6
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    avg = sum(sorted_vals) / n
    min_val = sorted_vals[0]
    max_val = sorted_vals[-1]
    def percentile(p):
        idx = int((n - 1) * p)
        return sorted_vals[idx]
    p50 = percentile(0.5)
    p90 = percentile(0.9)
    p99 = percentile(0.99)
    return [min_val, avg, max_val, p50, p90, p99]

def print_table(rows, headers):
    """打印对齐的表格（原样式）"""
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))
    sep = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
    header_line = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"
    print(sep)
    print(header_line)
    print(sep)
    for row in rows:
        line = "| " + " | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)) + " |"
        print(line)
    print(sep)

def print_markdown(rows, headers):
    """打印 Markdown 表格，相同 RPC 和 Phase 合并单元格（留空）"""
    merged_rows = []
    prev_rpc = None
    prev_phase = None
    for row in rows:
        rpc, phase = row[0], row[1]
        if rpc == prev_rpc and phase == prev_phase:
            new_row = ['', ''] + row[2:]
        else:
            new_row = row[:]
        merged_rows.append(new_row)
        prev_rpc, prev_phase = rpc, phase

    header_line = '| ' + ' | '.join(headers) + ' |'
    sep_line = '|' + '|'.join(['---' for _ in headers]) + '|'
    print(header_line)
    print(sep_line)
    for row in merged_rows:
        str_row = [str(cell) if cell is not None else '' for cell in row]
        print('| ' + ' | '.join(str_row) + ' |')

def metric_sort_key(metric):
    """返回 metric 在 METRICS 中的索引，用于自定义排序"""
    try:
        return METRICS.index(metric)
    except ValueError:
        return len(METRICS)  # 未知指标放最后

def main():
    output_format = 'table'   # 'table', 'csv', 'markdown'
    files = []
    for arg in sys.argv[1:]:
        if arg == '--csv':
            output_format = 'csv'
        elif arg == '--markdown':
            output_format = 'markdown'
        else:
            files.append(arg)

    if not files:
        print("用法: python3 stat_phase_metrics.py [--csv|--markdown] <logfile1> [logfile2 ...]")
        sys.exit(1)

    # 统一存储所有指标: data[(rpc, phase, metric)] = list of values
    data = defaultdict(list)

    for logfile in files:
        if not os.path.isfile(logfile):
            print(f"警告: 文件不存在 {logfile}", file=sys.stderr)
            continue
        with open(logfile, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                # 匹配 RPC 类型
                rpc_match = re.search(r'StoreService\.(Txn\w+Rpc)', line)
                if not rpc_match:
                    continue
                rpc = rpc_match.group(1)
                if rpc not in TARGET_RPCS:
                    continue

                # 提取 raft_commit_time_us（针对指定 RPC），存入 data 作为一个特殊 phase
                if rpc in RAFT_COMMIT_RPCS:
                    raft_match = re.search(r'raft_commit_time_us\((\d+)\)', line)
                    if raft_match:
                        raft_val = int(raft_match.group(1))
                        key = (rpc, 'raft_commit', 'time_us')
                        data[key].append(raft_val)

                # 提取所有 phase 的六个指标
                for match in PHASE_PATTERN.finditer(line):
                    phase = match.group(1)
                    nums = [int(match.group(i)) for i in range(2, 8)]
                    for metric_idx, metric_name in enumerate(METRICS):
                        key = (rpc, phase, metric_name)
                        data[key].append(nums[metric_idx])

    if not data:
        print("未找到任何匹配的日志行", file=sys.stderr)
        sys.exit(1)

    headers = ["RPC", "Phase", "Metric", "Count", "Min", "Avg", "Max", "P50", "P90", "P99"]
    rows = []

    # 构建所有行
    for (rpc, phase, metric), vals in data.items():
        min_val, avg, max_val, p50, p90, p99 = compute_stats(vals)
        # 如果所有值都是 None（比如空列表），跳过
        if min_val is None:
            continue
        avg_str = f"{avg:.2f}"
        rows.append([rpc, phase, metric, len(vals), min_val, avg_str, max_val, p50, p90, p99])

    # 排序：先按 RPC，再按 Phase（字母序），最后按 Metric 在 METRICS 中的顺序
    rows.sort(key=lambda x: (x[0], x[1], metric_sort_key(x[2])))

    if output_format == 'csv':
        print(",".join(headers))
        for row in rows:
            print(",".join(str(cell) for cell in row))
    elif output_format == 'markdown':
        print_markdown(rows, headers)
    else:
        print_table(rows, headers)

if __name__ == '__main__':
    main()
