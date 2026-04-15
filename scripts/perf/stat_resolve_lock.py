#!/usr/bin/env python3
"""
统计日志中 resolve_lock 的耗时分布（支持 glob 通配符）
用法: python3 stat_resolve_lock.py "pattern1" "pattern2" ...
示例: python3 stat_resolve_lock.py "mds.info.log.20260410-*"
"""

import re
import sys
import glob
import os

LOG_PATTERN = re.compile(
    r'trace total_time_us\(\d+\) read\([\d\s]+\) prewrite\([\d\s]+\) commit\([\d\s]+\) resolve_lock\((\d+)\) sleep\([\d\s]+\)'
)

def main():
    if len(sys.argv) < 2:
        print("用法: python3 stat_resolve_lock.py <pattern1> [pattern2 ...]")
        sys.exit(1)

    file_patterns = sys.argv[1:]
    files = []
    for pattern in file_patterns:
        files.extend(glob.glob(pattern))
    if not files:
        print("错误: 没有匹配到任何文件", file=sys.stderr)
        sys.exit(1)

    total = 0
    gt0 = 0
    gt10ms = 0
    gt100ms = 0

    for logfile in files:
        if not os.path.isfile(logfile):
            continue
        with open(logfile, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                match = LOG_PATTERN.search(line)
                if not match:
                    continue
                total += 1
                val = int(match.group(1))
                if val > 0:
                    gt0 += 1
                if val > 10000:
                    gt10ms += 1
                if val > 100000:
                    gt100ms += 1

    print(f"总事务个数: {total}")
    print(f"resolve_lock > 0 us 的个数: {gt0}")
    print(f"resolve_lock > 10 ms (10000 us) 的个数: {gt10ms}")
    print(f"resolve_lock > 100 ms (100000 us) 的个数: {gt100ms}")

if __name__ == '__main__':
    main()
