#!/usr/bin/env python3
# coding=utf-8

from cProfile import label
import os
import time
import json
import configparser
import subprocess
import re
import argparse

DingoFS_TOOL = "dingo"
HOSTNAME_PORT_REGEX = r"[^\"\ ]\S*:\d+"
IP_PORT_REGEX = r"[0-9]+(?:\.[0-9]+){3}:\d+"

targetPath=None

def loadConf():
    global targetPath
    conf=configparser.ConfigParser()
    conf.read("target.ini")
    targetPath=conf.get("path", "target_path")

def runDingofsToolCommand(command):
    cmd = [DingoFS_TOOL]+command
    output = None
    try:
        output = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT, timeout=5)
    except subprocess.TimeoutExpired as e:
        return -1, output
    except subprocess.CalledProcessError as e:
        return 0, e.output
    return 0, output

def loadMdsServer():
    ret, output = runDingofsToolCommand(["status", "mds", "--format=json"])
    label = lablesValue(None, "mds")
    
    if ret != 0:
        return unitValue(label, [])
    
    try:
        data = json.loads(output)
        result = data.get("result", {})
        mds_list = result.get("mdses", [])
        
        mdsServers = []
        for mdsInfo in mds_list:
            location = mdsInfo.get("location", {})
            host = location.get("host")
            port = location.get("port")
            if host and port:
                mdsServers.append(ipPort2Addr(host, port))
                
    except json.JSONDecodeError:
        mdsServers = []
    
    return unitValue(label, mdsServers)

def loadClient():
    ret, output = runDingofsToolCommand(["list", "mountpoint", "--format=json"])
    label = lablesValue(None, "client")
    
    if ret != 0:
        return unitValue(label, [])
    
    try:
        data = json.loads(output)
        fsInfos = data.get("result", {}).get("fsInfos", [])

        clients = []
        for fsinfo in fsInfos:
            mountPoints = fsinfo.get("mountPoints", [])
            for mountpoint in mountPoints:
                hostname = mountpoint.get("hostname")
                port = mountpoint.get("port")
                if hostname and port:
                    clients.append(ipPort2Addr(hostname, port))

    except json.JSONDecodeError:
        clients = []
    
    return unitValue(label, clients)

def loadRemoteCacheServer():
    ret, output = runDingofsToolCommand(["list", "cachemember", "--format=json"])
    label = lablesValue(None, "remotecache")
    
    if ret != 0:
        return unitValue(label, [])
    
    try:
        data = json.loads(output)
        members = data.get("result", {}).get("members", [])

        cacheServers = []
        for cacheMember in members:
            ip = cacheMember.get("ip")
            port = cacheMember.get("port")
            if ip and port:
                cacheServers.append(ipPort2Addr(ip, port))
                
    except json.JSONDecodeError:
        cacheServers = []
    
    return unitValue(label, cacheServers)

def ipPort2Addr(ip, port):
    return str(ip) + ":" + str(port)

def lablesValue(hostname, job):
    labels = {}
    if hostname is not None:
        labels["hostname"] = hostname
    if job is not None:
        labels["job"] = job
    return labels

def unitValue(lables, targets):
    unit = {}
    if lables is not None:
        unit["labels"] = lables
    if targets is not None:
        unit["targets"] = targets
    return unit


def refresh(isShow=False):
    targets = []

    # load mds
    mdsServers = loadMdsServer()
    targets.append(mdsServers)
    # load client
    client = loadClient()
    targets.append(client)
    # load cachemember
    cachemember = loadRemoteCacheServer()   
    targets.append(cachemember)

    with open(targetPath+'.new', 'w', 0o777) as fd:
        json.dump(targets, fd, indent=4)
        fd.flush()
        os.fsync(fd.fileno())

    os.rename(targetPath+'.new', targetPath)
    os.chmod(targetPath, 0o777)

    if isShow:
        print(json.dumps(targets, indent=4))    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='generate target for dingofs monitor',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--interval', 
                       type=int, 
                       default=60,
                       help='execute internal(s), default 60s')
    
    parser.add_argument('--count', 
                       type=int,
                       help='execute count, default infinite, always execute')

    parser.add_argument('--show', 
                       type=bool,
                       default=False,
                       help='show target info, default False')
    
    args = parser.parse_args()

    interval = args.interval
    count = args.count
    isShow = args.show

    print("Realtime update target is running, ","interval:", interval, "count:", count, "show:", isShow)

    current_count = 0
    while True:
        current_count += 1
        loadConf()
        refresh(isShow)
        if count is not None and current_count >= count:
            break
        time.sleep(interval)