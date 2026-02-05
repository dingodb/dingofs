#!/usr/bin/env python3
# coding=utf-8

import argparse
import configparser
import json
import os
import re
import subprocess
import time
from cProfile import label

DingoFS_TOOL = "dingo"
HOSTNAME_PORT_REGEX = r"[^\"\ ]\S*:\d+"
IP_PORT_REGEX = r"[0-9]+(?:\.[0-9]+){3}:\d+"

targetPath = None


def loadConf():
    global targetPath
    conf = configparser.ConfigParser()
    conf.read("target.ini")
    targetPath = conf.get("path", "target_path")
    print("target path:", targetPath)


def runDingofsToolCommand(command):
    cmd = [DingoFS_TOOL] + command
    output = None
    try:
        output = subprocess.check_output(
            cmd, text=True, stderr=subprocess.STDOUT, timeout=5
        )
    except subprocess.TimeoutExpired as e:
        return -1, output
    except subprocess.CalledProcessError as e:
        return 0, e.output
    return 0, output


def loadMdsServer(mdsaddr):
    command = ["mds", "status", "--format=json"]
    if mdsaddr:
        command.append(f"--mdsaddr={mdsaddr}")

    ret, output = runDingofsToolCommand(command)
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
        print("mds servers:", mdsServers)

    except json.JSONDecodeError:
        print("load mds server json decode error")
        mdsServers = []

    return unitValue(label, mdsServers)


def loadClient(mdsaddr):
    command = ["fs", "mountpoint", "--format=json"]
    if mdsaddr:
        command.append(f"--mdsaddr={mdsaddr}")

    ret, output = runDingofsToolCommand(command)
    label = lablesValue(None, "client")

    if ret != 0:
        return unitValue(label, [])

    try:
        data = json.loads(output)
        fsInfos = data.get("result", {}).get("fsInfos", [])
        if not fsInfos:
            print("no fsInfos found")
            return unitValue(label, [])
        clients = []
        for fsinfo in fsInfos:
            mountPoints = fsinfo.get("mountPoints", [])
            for mountpoint in mountPoints:
                hostname = mountpoint.get("hostname")
                port = mountpoint.get("port")
                if hostname and port:
                    clients.append(ipPort2Addr(hostname, port))
        print("clients:", clients)
    except json.JSONDecodeError:
        print("load client json decode error")
        clients = []

    return unitValue(label, clients)


def loadRemoteCacheServer(mdsaddr):
    command = ["cache", "member", "list", "--format=json"]
    if mdsaddr:
        command.append(f"--mdsaddr={mdsaddr}")

    ret, output = runDingofsToolCommand(command)
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
        print("remote cache servers:", cacheServers)
    except json.JSONDecodeError:
        print("load remote cache server json decode error")
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


def refresh(mdsaddr, isShow=False):
    targets = []

    # load mds
    mdsServers = loadMdsServer(mdsaddr)
    targets.append(mdsServers)
    # load client
    client = loadClient(mdsaddr)
    targets.append(client)
    # load cachemember
    cachemember = loadRemoteCacheServer(mdsaddr)
    targets.append(cachemember)

    try:
        with open(targetPath + ".new", "w", 0o777) as fd:
            json.dump(targets, fd, indent=4)
            fd.flush()
            os.fsync(fd.fileno())
    except IOError as e:
        print("write target file error:", e)
        return

    os.rename(targetPath + ".new", targetPath)
    os.chmod(targetPath, 0o777)
    print("update target file:", targetPath)

    if isShow:
        print(json.dumps(targets, indent=4))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="generate target for dingofs monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--interval", type=int, default=60, help="execute internal(s), default 60s"
    )

    parser.add_argument(
        "--count", type=int, help="execute count, default infinite, always execute"
    )

    parser.add_argument(
        "--show", type=bool, default=False, help="show target info, default False"
    )

    parser.add_argument("--mdsaddr", type=str, help="mds address, default None")

    args = parser.parse_args()

    interval = args.interval
    count = args.count
    isShow = args.show
    mdsaddr = args.mdsaddr

    print(
        "Realtime update target is running, ",
        "interval:",
        interval,
        "count:",
        count,
        "show:",
        isShow,
        "mdsaddr:",
        mdsaddr,
    )

    current_count = 0
    while True:
        current_count += 1
        loadConf()
        refresh(mdsaddr, isShow)
        if count is not None and current_count >= count:
            break
        time.sleep(interval)
