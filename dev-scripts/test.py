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


def create_blank_file(file_path):
    """创建一个空白文件"""
    with open(file_path, 'w', encoding='utf-8') as f:
        pass
    return file_path

def create_directory(dir_path):
    """创建一个目录"""
    os.makedirs(dir_path, exist_ok=True)
    return dir_path



# 递归创建目录和文件，每层创建10个子目录和1000个文件
def create_nested_structure(base_path, depth, num_dirs=10, num_files=1000):
    """递归创建目录和文件结构"""
    if depth == 0:
        return
    
    for i in range(num_dirs):
        dir_name = f"dir_{i:05d}"
        dir_path = os.path.join(base_path, dir_name)
        create_directory(dir_path)
        
        # 在当前目录下创建文件
        for j in range(num_files):

            file_name = f"file_{j:010d}"
            file_path = os.path.join(dir_path, file_name)
            create_blank_file(file_path)
        
        # 递归创建更深层的目录结构
        create_nested_structure(dir_path, depth - 1, num_dirs, num_files)
    
    return base_path

# 递归删除目录和文件
def delete_nested_structure(base_path):
    """递归删除目录和文件结构"""
    if not os.path.exists(base_path):
        return
    
    for item in os.listdir(base_path):
        item_path = os.path.join(base_path, item)
        if os.path.isdir(item_path):
            delete_nested_structure(item_path)
        else:
            os.remove(item_path)
    
    os.rmdir(base_path)

# 多个线程同时调用create_nested_structure函数去创建目录和文件
def threaded_create_structure(base_path, depth, num_threads=5, num_dirs=10, num_files=1000):
    """使用多线程创建目录和文件结构"""
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            thread_base_path = f"{base_path}/thread_{i}"
            create_directory(thread_base_path)  # 确保线程的基础路径存在
            futures.append(executor.submit(create_nested_structure, thread_base_path, depth, num_dirs, num_files))
        
        for future in futures:
            future.result()  # 等待所有线程完成

def main():
    print("start test......")

    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Test script for creating and deleting nested directory structures.")
    parser.add_argument('--path', type=str, default='test_structure', help='Base path for the nested structure')
    parser.add_argument('--depth', type=int, default=5, help='Depth of the nested structure')
    parser.add_argument('--threads', type=int, default=5, help='Number of threads to use for creating structure')
    args = parser.parse_args()

    
    try:
        # 运行各种测试
        base_path = args.path
        depth = args.depth
        num_threads = args.threads
        print(f"Creating nested structure at {base_path} with depth {depth}...")

        # create_nested_structure(base_path, depth, num_dirs=5, num_files=1000)
        threaded_create_structure(base_path, depth, num_threads, num_dirs=5, num_files=1000)
        
        print("\ntest finish!")
        
    finally:
        # delete_nested_structure(base_path)
        # print("\ncleanup completed!")
        pass

if __name__ == "__main__":
    main()