#!/usr/bin/env python3
# coding=utf-8

from cProfile import label
import os
import time
import json
import configparser
import subprocess
import re

DingoFS_TOOL = "dingo"
HOSTNAME_PORT_REGEX = r"[^\"\ ]\S*:\d+"
IP_PORT_REGEX = r"[0-9]+(?:\.[0-9]+){3}:\d+"

targetPath=None
etcdTargetPath=None

def loadConf():
    global targetPath
    global etcdTargetPath
    conf=configparser.ConfigParser()
    conf.read("target.ini")
    targetPath=conf.get("path", "target_path")
    etcdTargetPath=conf.get("path", "etcd_target_path")

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
    ret, output = runDingofsToolCommand(["status","mds","--format=json"])
    mdsServers = []
    label = lablesValue(None, "mds")
    if ret == 0 :
        data = json.loads(output)
        for mdsInfo in data["result"]:
            # hostname:port:path
            dummyAddr = mdsInfo["dummyAddr"]
            mdsServers.append(dummyAddr)
    return unitValue(label, mdsServers)

def loadMetaServer():
    ret, data = runDingofsToolCommand(["list", "topology","--format=json"])
    if ret == 0:
       jsonData = json.loads(data)
       jsonData = jsonData["result"]   

    metaservers = []
    if jsonData is not None:
        for pool in jsonData["poollist"]:
            for zone in pool["zoneList"]:
                for server in zone["serverList"]:
                    for metaserver in server["metaserverList"]:
                        metaservers.append(metaserver)
    targets = []
    labels = lablesValue(None, "metaserver")
    for server in metaservers:
        targets.append(ipPort2Addr(server["externalIp"], server["externalPort"]))
    targets = list(set(targets))
    return unitValue(labels, targets)

def loadEtcdServer():
    ret, output = runDingofsToolCommand(["status","etcd","--format=json"])
    etcdServers = []
    label = lablesValue(None, "etcd")
    if ret == 0 :
        data = json.loads(output)
        for etcdInfo in data["result"]:
            etcdAddr = etcdInfo["addr"]
            etcdServers.append(etcdAddr)
    return unitValue(label, etcdServers)

def loadClient():
    ret, output = runDingofsToolCommand(["list","mountpoint","--format=json"])
    clients = []
    label = lablesValue(None, "client")
    if ret == 0 :
        data = json.loads(output)
        for fsinfo in data["result"]:
            # hostname:port:path
            mountpoint = str(fsinfo["mountpoint"])
            muontListData=mountpoint.split(":")
            clients.append(muontListData[0] + ":" + muontListData[1])
    return unitValue(label, clients)

def loadType(hostType):
    ret, output = runDingofsToolCommand(["status-%s"%hostType])
    targets = []
    if ret == 0:
        targets = re.findall(IP_PORT_REGEX, str(output))
    labels = lablesValue(None, hostType)
    return unitValue(labels, targets)

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


def refresh():
    targets = []
    etcd_targets = []

    # load mds
    mdsServers = loadMdsServer()
    targets.append(mdsServers)
    # load metaserver
    metaServers = loadMetaServer()
    targets.append(metaServers)
    # load client
    client = loadClient()
    targets.append(client)
    # load etcd
    etcdServers = loadEtcdServer()
    etcd_targets.append(etcdServers)

    with open(targetPath+'.new', 'w', 0o777) as fd:
        json.dump(targets, fd, indent=4)
        fd.flush()
        os.fsync(fd.fileno())

    os.rename(targetPath+'.new', targetPath)
    os.chmod(targetPath, 0o777)

    with open(etcdTargetPath+'.new', 'w', 0o777) as etcd_fd:
        json.dump(etcd_targets, etcd_fd, indent=4)
        etcd_fd.flush()
        os.fsync(etcd_fd.fileno())
        
    os.rename(etcdTargetPath+'.new', etcdTargetPath)
    os.chmod(etcdTargetPath, 0o777)

if __name__ == '__main__':
    while True:
        loadConf()
        refresh()
        # refresh every 30s
        time.sleep(30)
