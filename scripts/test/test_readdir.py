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

mount_point_1 = "/home/dengzihui/mount-test/dengzh_c101_01-1"
mount_point_2 = "/home/dengzihui/mount-test/dengzh_c101_01-2"

def mkdir(name):
  path = os.path.join(mount_point_1, name)
  os.mkdir(path)
  print(f"mkdir {path} done")

def readdir(name):
  visible = name in os.listdir(mount_point_2)
  print(f"readdir {name} visible: {visible}")

def main():
  print("start test......")


for i in range(100):
    # random num use timestamp (ms) to avoid name conflict
    name = "test_dir_" + str(int(time.time() * 1000) + random.randint(1, 1000))
    mkdir(name)
    time.sleep(2)
    readdir(name)

if __name__ == "__main__":
  main()