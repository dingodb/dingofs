import os
import shutil
import stat
import random
import string
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import queue
import argparse


def generate_random_string(length=10):
    """生成随机字符串"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def write_file(file_path):
    """写文件"""
    with open(file_path, 'a', encoding='utf-8') as f:
        index = 0
        while True:
          print(f"Writing {index}...")
          f.write(generate_random_string(4096) + '\n')
          sleep_time = random.uniform(0.1, 3.1)  # 模拟写文件的时间
          time.sleep(sleep_time)
          index += 1

def main():
    print("start test......")

    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Test script for creating and deleting nested directory structures.")
    parser.add_argument('--path', type=str, default='test_structure', help='Base path for the nested structure')
    args = parser.parse_args()

    
    try:
        file_path = args.path

        write_file(file_path)
        
        print("\ntest finish!")
        
    finally:
        pass

if __name__ == "__main__":
    main()