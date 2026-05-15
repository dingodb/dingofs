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


def create_file(file_path, file_size=0):
    """创建一个文件，可指定大小（字节），内容为随机字节"""
    with open(file_path, 'wb') as f:
        if file_size > 0:
            chunk_size = 1024 * 1024  # 1MB
            remaining = file_size
            while remaining > 0:
                write_size = min(chunk_size, remaining)
                f.write(os.urandom(write_size))
                remaining -= write_size
    return file_path

def create_directory(dir_path):
    """创建一个目录"""
    os.makedirs(dir_path, exist_ok=True)
    return dir_path



# 递归创建目录和文件，每层创建10个子目录和1000个文件
def create_nested_structure(base_path, depth, num_dirs=10, num_files=1000, file_size=0):
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
            create_file(file_path, file_size)
        
        # 递归创建更深层的目录结构
        create_nested_structure(dir_path, depth - 1, num_dirs, num_files, file_size)
    
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
def threaded_create_structure(base_path, depth, num_threads=5, num_dirs=10, num_files=1000, file_size=0):
    """使用多线程创建目录和文件结构"""
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            thread_base_path = f"{base_path}/thread_{i}"
            create_directory(thread_base_path)  # 确保线程的基础路径存在
            futures.append(executor.submit(create_nested_structure, thread_base_path, depth, num_dirs, num_files, file_size))
        
        for future in futures:
            future.result()  # 等待所有线程完成

def parse_size(size_str):
    """解析文件大小字符串，支持 K/M/G 后缀（如 4K, 1M, 2G），返回字节数"""
    s = str(size_str).strip().upper()
    if not s:
        return 0
    units = {'K': 1024, 'M': 1024 ** 2, 'G': 1024 ** 3, 'T': 1024 ** 4}
    if s[-1] in units:
        return int(float(s[:-1]) * units[s[-1]])
    return int(s)

def main():
    print("start test......")

    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Test script for creating and deleting nested directory structures.")
    parser.add_argument('--path', type=str, default='test_structure', help='Base path for the nested structure')
    parser.add_argument('--depth', type=int, default=5, help='Depth of the nested structure')
    parser.add_argument('--threads', type=int, default=5, help='Number of threads to use for creating structure')
    parser.add_argument('--dirs', type=int, default=5, help='Number of subdirectories per level')
    parser.add_argument('--files', type=int, default=1000, help='Number of files per directory')
    parser.add_argument('--file-size', type=str, default='0', help='Size of each file, supports K/M/G suffix (e.g. 4K, 1M, 2G)')
    args = parser.parse_args()

    
    try:
        # 运行各种测试
        base_path = args.path
        depth = args.depth
        num_threads = args.threads
        num_dirs = args.dirs
        num_files = args.files
        file_size = parse_size(args.file_size)
        print(f"Creating nested structure at {base_path} with depth {depth}, file_size {file_size} bytes...")

        threaded_create_structure(base_path, depth, num_threads, num_dirs=num_dirs, num_files=num_files, file_size=file_size)
        
        print("\ntest finish!")
        
    finally:
        # delete_nested_structure(base_path)
        # print("\ncleanup completed!")
        pass

if __name__ == "__main__":
    main()