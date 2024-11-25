# dingo tool usage

A tool for CurveFS

- [dingo tool usage](#dingo-tool-usage)
  - [How to use dingo tool](#how-to-use-dingo-tool)
    - [Install](#install)
    - [Introduction](#introduction)
  - [Command](#command)
    - [version](#version)
    - [check](#check)
      - [check copyset](#check-copyset)
    - [create](#create)
      - [create fs](#create-fs)
      - [create topology](#create-topology)
    - [delete](#delete)
      - [delete fs](#delete-fs)
    - [list](#list)
      - [list copyset](#list-copyset)
      - [list fs](#list-fs)
      - [list mountpoint](#list-mountpoint)
      - [list partition](#list-partition)
      - [list topology](#list-topology)
    - [query](#query)
      - [query copyset](#query-copyset)
      - [query fs](#query-fs)
      - [query inode](#query-inode)
      - [query metaserver](#query-metaserver)
      - [query partition](#query-partition)
    - [status](#status)
      - [status mds](#status-mds)
      - [status metaserver](#status-metaserver)
      - [status etcd](#status-etcd)
      - [status copyset](#status-copyset)
      - [status cluster](#status-cluster)
    - [umount](#umount)
      - [umount fs](#umount-fs)
    - [usage](#usage)
      - [usage inode](#usage-inode)
      - [usage metadata](#usage-metadata)
    - [warmup](#warmup)
      - [warmup add](#warmup-add)
    - [config](#config)
      - [config fs](#config-fs)
      - [config get](#config-get)
      - [config check](#config-check)
    - [quota](#quota)
      - [quota set](#quota-set)
      - [quota get](#quota-get)
      - [quota list](#quota-list)
      - [quota delete](#quota-delete)
      - [quota check](#quota-check)
      
## How to use dingo tool

### Install

install dingo tool

For obtaining binary package, please refer to:
[dingo tool binary compilation guide](https://github.com/dingodb/dingofs/blob/main/INSTALL.md)

```bash
chmod +x dingo
mv dingo /usr/bin/dingo
```

set configure file

```bash
wget https://github.com/dingodb/dingofs/blob/main/tools-v2/pkg/config/dingo.yaml
```
Please modify the `mdsAddr, mdsDummyAddr, etcdAddr` under `dingofs` in the template.yaml file as required

configure file priority
environment variables(CONF=/opt/dingo.yaml) > default (~/.dingo/dingo.yaml)
```bash
mv dingo.yaml ~/.dingo/dingo.yaml
or
export CONF=/opt/dingo.yaml
```

### Introduction

Here's how to use the tool

```bash
dingo COMMAND [options]
```

When you are not sure how to use a command, --help can give you an example of use:

```bash
dingo COMMAND --help
```

For example:

```bash
dingo status mds --help
Usage:  dingo status mds [flags]

get the inode usage of dingofs

Flags:
  -c, --conf string            config file (default is $HOME/.dingo/dingo.yaml or /etc/dingo/dingo.yaml)
  -f, --format string          Output format (json|plain) (default "plain")
  -h, --help                   Print usage
      --httptimeout duration   http timeout (default 500ms)
      --mdsaddr string         mds address, should be like 127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702
      --mdsdummyaddr string    mds dummy address, should be like 127.0.0.1:7700,127.0.0.1:7701,127.0.0.1:7702
      --showerror              display all errors in command

Examples:
$ dingo status mds
```

In addition, this tool reads the configuration from `$HOME/.dingo/dingo.yaml` or `/etc/dingo/dingo.yaml` by default,
and can be specified by `--conf` or `-c`.

## Command

### version

show the version of dingo tool

Usage:

```shell
dingo --version
```

Output:

```shell
dingo v1.2
```

### fs

#### check

##### check copyset

check copysets health in dingofs

Usage:

```shell
dingo check copyset --copysetid 1 --poolid 1
```

Output:

```shell
+------------+-----------+--------+--------+---------+
| COPYSETKEY | COPYSETID | POOLID | STATUS | EXPLAIN |
+------------+-----------+--------+--------+---------+
| 4294967297 |         1 |      1 | ok     |         |
+------------+-----------+--------+--------+---------+
```

#### create

##### create fs

create a fs in dingofs

Usage:

```shell
dingo create fs --fsname test3
```

Output:

```shell
+--------+-------------------------------------------+
| FSNAME |                  RESULT                   |
+--------+-------------------------------------------+
| test3  | fs exist, but s3 info is not inconsistent |
+--------+-------------------------------------------+
```

##### create topology

create dingofs topology

Usage:

```shell
dingo create topology --clustermap topology.json
```

Output:

```shell
+-------------------+--------+-----------+--------+
|       NAME        |  TYPE  | OPERATION | PARENT |
+-------------------+--------+-----------+--------+
| pool2             | pool   | del       |        |
+-------------------+--------+           +--------+
| zone4             | zone   |           | pool2  |
+-------------------+--------+           +--------+
| **.***.***.**_3_0 | server |           | zone4  |
+-------------------+--------+-----------+--------+
```

#### delete

##### delete fs

delete a fs from dingofs

Usage:

```shell
dingo delete fs --fsname test1
WARNING:Are you sure to delete fs test1?
please input [test1] to confirm: test1
```

Output:

```shell
+--------+-------------------------------------+
| FSNAME |               RESULT                |
+--------+-------------------------------------+
| test1  | delete fs failed!, error is FS_BUSY |
+--------+-------------------------------------+
```

#### list

##### list copyset

list all copyset info of the dingofs

Usage:

```shell
dingo list copyset
```

Output:

```shell
+------------+-----------+--------+-------+--------------------------------+------------+
|    KEY     | COPYSETID | POOLID | EPOCH |           LEADERPEER           | PEERNUMBER |
+------------+-----------+--------+-------+--------------------------------+------------+
| 4294967302 | 6         | 1      | 2     | id:1                           | 3          |
|            |           |        |       | address:"**.***.***.**:6801:0" |            |
+------------+-----------+        +-------+                                +------------+
| 4294967303 | 7         |        | 1     |                                | 3          |
|            |           |        |       |                                |            |
+------------+-----------+        +-------+                                +------------+
| 4294967304 | 8         |        | 1     |                                | 3          |
|            |           |        |       |                                |            |
+------------+-----------+        +-------+                                +------------+
| 4294967307 | 11        |        | 1     |                                | 3          |
|            |           |        |       |                                |            |
+------------+-----------+        +-------+--------------------------------+------------+
| 4294967297 | 1         |        | 1     | id:2                           | 3          |
|            |           |        |       | address:"**.***.***.**:6802:0" |            |
+------------+-----------+        +-------+                                +------------+
| 4294967301 | 5         |        | 1     |                                | 3          |
|            |           |        |       |                                |            |
+------------+-----------+        +-------+                                +------------+
| 4294967308 | 12        |        | 1     |                                | 3          |
|            |           |        |       |                                |            |
+------------+-----------+        +-------+--------------------------------+------------+
| 4294967298 | 2         |        | 1     | id:3                           | 3          |
|            |           |        |       | address:"**.***.***.**:6800:0" |            |
+------------+-----------+        +-------+                                +------------+
| 4294967299 | 3         |        | 1     |                                | 3          |
|            |           |        |       |                                |            |
+------------+-----------+        +-------+                                +------------+
| 4294967300 | 4         |        | 1     |                                | 3          |
|            |           |        |       |                                |            |
+------------+-----------+        +-------+                                +------------+
| 4294967305 | 9         |        | 1     |                                | 3          |
|            |           |        |       |                                |            |
+------------+-----------+        +-------+                                +------------+
| 4294967306 | 10        |        | 1     |                                | 3          |
|            |           |        |       |                                |            |
+------------+-----------+--------+-------+--------------------------------+------------+
```

##### list fs

list all fs info in the dingofs

Usage:

```shell
dingo list fs
```

Output:

```shell
+----+-------+--------+--------------+-----------+---------+----------+-----------+----------+
| ID | NAME  | STATUS |   CAPACITY   | BLOCKSIZE | FSTYPE  | SUMINDIR |   OWNER   | MOUNTNUM |
+----+-------+--------+--------------+-----------+---------+----------+-----------+----------+
| 2  | test1 | INITED | 107374182400 | 1048576   | TYPE_S3 | false    | anonymous | 1        |
+----+-------+--------+--------------+-----------+         +----------+           +----------+
| 3  | test3 | INITED | 107374182400 | 1048576   |         | false    |           | 0        |
+----+-------+--------+--------------+-----------+---------+----------+-----------+----------+
```

##### list mountpoint

list all mountpoint of the dingofs

Usage:

```shell
dingo list mountpoint
```

Output:

```shell
+------+--------+-------------------------------------------------------------------+
| FSID | FSNAME |                            MOUNTPOINT                             |
+------+--------+-------------------------------------------------------------------+
| 2    | test1  | siku-QiTianM420-N000:9002:/dingofs/client/mnt/home/siku/temp/mnt1 |
+      +        +-------------------------------------------------------------------+
|      |        | siku-QiTianM420-N000:9003:/dingofs/client/mnt/home/siku/temp/mnt2 |
+------+--------+-------------------------------------------------------------------+
```

##### list partition

list partition in dingofs by fsid

Usage:

```shell
dingo list partition
```

Output:

```shell
+-------------+------+--------+-----------+----------+----------+-----------+
| PARTITIONID | FSID | POOLID | COPYSETID |  START   |   END    |  STATUS   |
+-------------+------+--------+-----------+----------+----------+-----------+
| 14          | 2    | 1      | 10        | 1048676  | 2097351  | READWRITE |
+-------------+      +        +           +----------+----------+-----------+
| 20          |      |        |           | 7340732  | 8389407  | READWRITE |
+-------------+      +        +-----------+----------+----------+-----------+
| 13          |      |        | 11        | 0        | 1048675  | READWRITE |
+-------------+      +        +           +----------+----------+-----------+
| 16          |      |        |           | 3146028  | 4194703  | READWRITE |
+-------------+      +        +           +----------+----------+-----------+
| 22          |      |        |           | 9438084  | 10486759 | READWRITE |
+-------------+      +        +-----------+----------+----------+-----------+
| 21          |      |        | 5         | 8389408  | 9438083  | READWRITE |
+-------------+      +        +-----------+----------+----------+-----------+
| 23          |      |        | 7         | 10486760 | 11535435 | READWRITE |
+-------------+      +        +           +----------+----------+-----------+
| 24          |      |        |           | 11535436 | 12584111 | READWRITE |
+-------------+      +        +-----------+----------+----------+-----------+
| 15          |      |        | 8         | 2097352  | 3146027  | READWRITE |
+-------------+      +        +           +----------+----------+-----------+
| 18          |      |        |           | 5243380  | 6292055  | READWRITE |
+-------------+      +        +-----------+----------+----------+-----------+
| 17          |      |        | 9         | 4194704  | 5243379  | READWRITE |
+-------------+      +        +           +----------+----------+-----------+
| 19          |      |        |           | 6292056  | 7340731  | READWRITE |
+-------------+------+        +-----------+----------+----------+-----------+
| 26          | 3    |        | 2         | 1048676  | 2097351  | READWRITE |
+-------------+      +        +           +----------+----------+-----------+
| 30          |      |        |           | 5243380  | 6292055  | READWRITE |
+-------------+      +        +-----------+----------+----------+-----------+
| 34          |      |        | 3         | 9438084  | 10486759 | READWRITE |
+-------------+      +        +-----------+----------+----------+-----------+
| 29          |      |        | 4         | 4194704  | 5243379  | READWRITE |
+-------------+      +        +           +----------+----------+-----------+
| 32          |      |        |           | 7340732  | 8389407  | READWRITE |
+-------------+      +        +-----------+----------+----------+-----------+
| 35          |      |        | 5         | 10486760 | 11535435 | READWRITE |
+-------------+      +        +           +----------+----------+-----------+
| 27          |      |        |           | 2097352  | 3146027  | READWRITE |
+-------------+      +        +           +----------+----------+-----------+
| 33          |      |        |           | 8389408  | 9438083  | READWRITE |
+-------------+      +        +-----------+----------+----------+-----------+
| 25          |      |        | 6         | 0        | 1048675  | READWRITE |
+-------------+      +        +           +----------+----------+-----------+
| 36          |      |        |           | 11535436 | 12584111 | READWRITE |
+-------------+      +        +-----------+----------+----------+-----------+
| 28          |      |        | 8         | 3146028  | 4194703  | READWRITE |
+-------------+      +        +-----------+----------+----------+-----------+
| 31          |      |        | 9         | 6292056  | 7340731  | READWRITE |
+-------------+------+--------+-----------+----------+----------+-----------+
```

##### list topology

list the topology of the dingofs

Usage:

```shell
dingo list topology
```

Output:

```shell
+----+------------+--------------------+------------+-----------------------+
| ID |    TYPE    |        NAME        | CHILDTYPE  |       CHILDLIST       |
+----+------------+--------------------+------------+-----------------------+
| 1  | pool       | pool1              | zone       | zone3 zone2 zone1     |
+----+------------+--------------------+------------+-----------------------+
| 3  | zone       | zone3              | server     | **.***.***.**_2_0     |
+----+            +--------------------+            +-----------------------+
| 2  |            | zone2              |            | **.***.***.**_1_0     |
+----+            +--------------------+            +-----------------------+
| 1  |            | zone1              |            | **.***.***.**_0_0     |
+----+------------+--------------------+------------+-----------------------+
| 3  | server     | **.***.***.**_2_0  | metaserver | dingofs-metaserver.2  |
+----+            +--------------------+            +-----------------------+
| 2  |            | **.***.***.**_1_0  |            | dingofs-metaserver.1  |
+----+            +--------------------+            +-----------------------+
| 1  |            | **.***.***.**_0_0  |            | dingofs-metaserver.3  |
+----+------------+--------------------+------------+-----------------------+
| 3  | metaserver | dingofs-metaserver |            |                       |
+----+            +--------------------+------------+-----------------------+
| 2  |            | dingofs-metaserver |            |                       |
+----+            +--------------------+------------+-----------------------+
| 1  |            | dingofs-metaserver |            |                       |
+----+------------+--------------------+------------+-----------------------+
```

#### query

##### query copyset

query copysets in dingofs

Usage:

```shell
dingo query copyset --copysetid 1 --poolid 1
```

Output:

```shell
+------------+-----------+--------+--------------------------------------+-------+
| copysetKey | copysetId | poolId |              leaderPeer              | epoch |
+------------+-----------+--------+--------------------------------------+-------+
| 4294967297 |     1     |   1    | id:2  address:"**.***.***.**:6802:0" |   1   |
+------------+-----------+--------+--------------------------------------+-------+
```

##### query fs

query fs in dingofs by fsname or fsid

Usage:

```shell
dingo query fs --fsname test1
```

Output:

```shell
+----+-------+--------+--------------+-----------+---------+----------+-----------+----------+
| id | name  | status |   capacity   | blocksize | fsType  | sumInDir |   owner   | mountNum |
+----+-------+--------+--------------+-----------+---------+----------+-----------+----------+
| 2  | test1 | INITED | 107374182400 |  1048576  | TYPE_S3 |  false   | anonymous |    2     |
+----+-------+--------+--------------+-----------+---------+----------+-----------+----------+
```

##### query inode

query the inode of fs

Usage:

```shell
dingo query inode --fsid 2 --inodeid 5243380
```

Output:

```shell
+-------+----------+-----------+---------+-------+--------+
| fs id | inode id |  length   |  type   | nlink | parent |
+-------+----------+-----------+---------+-------+--------+
|   2   | 5243380  | 352321536 | TYPE_S3 |   1   |  [1]   |
+-------+----------+-----------+---------+-------+--------+
```

##### query metaserver

query metaserver in dingofs by metaserverid or metaserveraddr

Usage:

```shell
dingo query metaserver --metaserveraddr **.***.***.**:6801,**.***.***.**:6802
```

Output:

```shell
+----+--------------------+--------------------+--------------------+-------------+
| id |      hostname      |    internalAddr    |    externalAddr    | onlineState |
+----+--------------------+--------------------+--------------------+-------------+
| 1  | dingofs-metaserver | **.***.***.**:6801 | **.***.***.**:6801 |   ONLINE    |
| 2  | dingofs-metaserver | **.***.***.**:6802 | **.***.***.**:6802 |   ONLINE    |
+----+--------------------+--------------------+--------------------+-------------+
```

##### query partition

query the copyset of partition

Usage:

```shell
dingo query partition --partitionid 14
```

Output:

```shell
+----+--------+-----------+--------+----------------------+
| id | poolId | copysetId | peerId |       peerAddr       |
+----+--------+-----------+--------+----------------------+
| 14 |   1    |    10     |   1    | **.***.***.**:6801:0 |
| 14 |   1    |    10     |   2    | **.***.***.**:6802:0 |
| 14 |   1    |    10     |   3    | **.***.***.**:6800:0 |
+----+--------+-----------+--------+----------------------+
```

#### status

##### status mds

get status of mds

Usage:

```shell
dingo status mds
```

Output:

```shell
+--------------------+--------------------+----------------+----------+
|        addr        |     dummyAddr      |    version     |  status  |
+--------------------+--------------------+----------------+----------+
| **.***.***.**:6700 | **.***.***.**:7700 | 8fc48476+debug | follower |
| **.***.***.**:6701 | **.***.***.**:7701 | 8fc48476+debug | follower |
| **.***.***.**:6702 | **.***.***.**:7702 | 8fc48476+debug |  leader  |
+--------------------+--------------------+----------------+----------+
```

##### status metaserver

get status of metaserver

Usage:

```shell
dingo status metaserver
```

Output:

```shell
+--------------------+--------------------+----------------+--------+
|    externalAddr    |    internalAddr    |    version     | status |
+--------------------+--------------------+----------------+--------+
| **.***.***.**:6800 | **.***.***.**:6800 | 8fc48476+debug | online |
| **.***.***.**:6802 | **.***.***.**:6802 | 8fc48476+debug | online |
| **.***.***.**:6801 | **.***.***.**:6801 | 8fc48476+debug | online |
+--------------------+--------------------+----------------+--------+
```

##### status etcd

get status of etcd

Usage:

```shell
dingo status etcd
```

Output:

```shell
+---------------------+---------+----------+
|        addr         | version |  status  |
+---------------------+---------+----------+
| **.***.***.**:23790 | 3.4.10  | follower |
| **.***.***.**:23791 | 3.4.10  | follower |
| **.***.***.**:23792 | 3.4.10  |  leader  |
+---------------------+---------+----------+
```

##### status copyset

get status of copyset

Usage:

```shell
dingo status copyset
```

Output:

```shell
+------------+-----------+--------+--------+---------+
| copysetKey | copysetId | poolId | status | explain |
+------------+-----------+--------+--------+---------+
| 4294967297 |     1     |   1    |   ok   |         |
| 4294967298 |     2     |   1    |   ok   |         |
| 4294967299 |     3     |   1    |   ok   |         |
| 4294967300 |     4     |   1    |   ok   |         |
| 4294967301 |     5     |   1    |   ok   |         |
| 4294967302 |     6     |   1    |   ok   |         |
| 4294967303 |     7     |   1    |   ok   |         |
| 4294967304 |     8     |   1    |   ok   |         |
| 4294967305 |     9     |   1    |   ok   |         |
| 4294967306 |    10     |   1    |   ok   |         |
| 4294967307 |    11     |   1    |   ok   |         |
| 4294967308 |    12     |   1    |   ok   |         |
+------------+-----------+--------+--------+---------+
```

##### status cluster

get status of cluster

Usage:

```shell
dingo status cluster
```

Output:

```shell
etcd:
+---------------------+---------+----------+
|        addr         | version |  status  |
+---------------------+---------+----------+
| **.***.***.**:23790 | 3.4.10  | follower |
| **.***.***.**:23791 | 3.4.10  | follower |
| **.***.***.**:23792 | 3.4.10  |  leader  |
+---------------------+---------+----------+

mds:
+--------------------+--------------------+----------------+----------+
|        addr        |     dummyAddr      |    version     |  status  |
+--------------------+--------------------+----------------+----------+
| **.***.***.**:6700 | **.***.***.**:7700 | 8fc48476+debug | follower |
| **.***.***.**:6701 | **.***.***.**:7701 | 8fc48476+debug | follower |
| **.***.***.**:6702 | **.***.***.**:7702 | 8fc48476+debug |  leader  |
+--------------------+--------------------+----------------+----------+

meataserver:
+--------------------+--------------------+----------------+--------+
|    externalAddr    |    internalAddr    |    version     | status |
+--------------------+--------------------+----------------+--------+
| **.***.***.**:6800 | **.***.***.**:6800 | 8fc48476+debug | online |
| **.***.***.**:6802 | **.***.***.**:6802 | 8fc48476+debug | online |
| **.***.***.**:6801 | **.***.***.**:6801 | 8fc48476+debug | online |
+--------------------+--------------------+----------------+--------+

copyset:
+------------+-----------+--------+--------+---------+
| copysetKey | copysetId | poolId | status | explain |
+------------+-----------+--------+--------+---------+
| 4294967297 |     1     |   1    |   ok   |         |
| 4294967298 |     2     |   1    |   ok   |         |
| 4294967299 |     3     |   1    |   ok   |         |
| 4294967300 |     4     |   1    |   ok   |         |
| 4294967301 |     5     |   1    |   ok   |         |
| 4294967302 |     6     |   1    |   ok   |         |
| 4294967303 |     7     |   1    |   ok   |         |
| 4294967304 |     8     |   1    |   ok   |         |
| 4294967305 |     9     |   1    |   ok   |         |
| 4294967306 |    10     |   1    |   ok   |         |
| 4294967307 |    11     |   1    |   ok   |         |
| 4294967308 |    12     |   1    |   ok   |         |
+------------+-----------+--------+--------+---------+

Cluster health is:  ok
```

#### umount

##### umount fs

umount fs from the dingofs cluster

Usage:

```shell
dingo umount fs --fsname test1 --mountpoint siku-QiTianM420-N000:9002:/dingofs/client/mnt/home/siku/temp/mnt1
```

Output:

```shell
+--------+-------------------------------------------------------------------+---------+
| fsName |                            mountpoint                             | result  |
+--------+-------------------------------------------------------------------+---------+
| test1  | siku-QiTianM420-N000:9003:/dingofs/client/mnt/home/siku/temp/mnt2 | success |
+--------+-------------------------------------------------------------------+---------+
```

#### usage

##### usage inode

get the inode usage of dingofs

Usage:

```shell
dingo usage inode
```

Output:

```shell
+------+----------------+-----+
| fsId |     fsType     | num |
+------+----------------+-----+
|  2   |   inode_num    |  3  |
|  2   | type_directory |  1  |
|  2   |   type_file    |  0  |
|  2   |    type_s3     |  2  |
|  2   | type_sym_link  |  0  |
|  3   |   inode_num    |  1  |
|  3   | type_directory |  1  |
|  3   |   type_file    |  0  |
|  3   |    type_s3     |  0  |
|  3   | type_sym_link  |  0  |
+------+----------------+-----+
```

##### usage metadata

get the usage of metadata in dingofs

Usage:

```shell
dingo usage metadata
```

Output:

```shell
+--------------------+---------+---------+---------+
|   metaserverAddr   |  total  |  used   |  left   |
+--------------------+---------+---------+---------+
| **.***.***.**:6800 | 2.0 TiB | 182 GiB | 1.8 TiB |
| **.***.***.**:6802 | 2.0 TiB | 182 GiB | 1.8 TiB |
| **.***.***.**:6801 | 2.0 TiB | 182 GiB | 1.8 TiB |
+--------------------+---------+---------+---------+
```

#### warmup

#### warmup add

warmup a file(directory), or given a list file contains a list of files(directories) that you want to warmup.

Usage:

```shell
dingo warmup add /mnt/dingofs/warmup
dingo warmup add --filelist /mnt/dingofs/warmup.list
```

> `dingo warmup add /mnt/dingofs/warmup` will warmup a file(directory).
> /mnt/dingofs/warmup.list

#### config
#### config fs

config fs quota for dingofs

Usage:

```shell
dingo config fs --fsid 1 --capacity 100
dingo config fs --fsname dingofs --capacity 10 --inodes 1000000000
```
#### config get

get fs quota for dingofs

Usage:

```shell
dingo config get --fsid 1
dingo config get --fsname dingofs
```
Output:

```shell
+------+---------+----------+------+------+---------------+-------+-------+
| FSID | FSNAME  | CAPACITY | USED | USE% |    INODES     | IUSED | IUSE% |
+------+---------+----------+------+------+---------------+-------+-------+
| 2    | dingofs | 10 GiB   | 0 B  | 0    | 1,000,000,000 | 0     | 0     |
+------+---------+----------+------+------+---------------+-------+-------+
```

#### config check

check quota of fs

Usage:

```shell
dingo config check --fsid 1
dingo config check --fsname dingofs
```
Output:

```shell
+------+----------+-----------------+---------------+---------------+-----------+-------+-----------+---------+
| FSID |  FSNAME  |    CAPACITY     |     USED      |   REALUSED    |  INODES   | IUSED | REALIUSED | STATUS  |
+------+----------+-----------------+---------------+---------------+-----------+-------+-----------+---------+
| 1    | dingofs  | 107,374,182,400 | 1,083,981,835 | 1,083,981,835 | unlimited | 9     | 9         | success |
+------+----------+-----------------+---------------+---------------+-----------+-------+-----------+---------+
```

#### quota
#### quota set

set quota to a directory

Usage:

```shell
dingo quota set --fsid 1 --path /quotadir --capacity 10 --inodes 100000
```
#### quota get

get fs quota for dingofs

Usage:

```shell
dingo quota get --fsid 1 --path /quotadir
dingo quota get --fsname dingofs --path /quotadir
```
Output:

```shell
+----------+------------+----------+------+------+------------+-------+-------+
|    ID    |    PATH    | CAPACITY | USED | USE% |   INODES   | IUSED | IUSE% |
+----------+------------+----------+------+------+------------+-------+-------+
| 10485760 | /quotadir1 | 10 GiB   | 6 B  | 0    | 20,000,000 | 1     | 0     |
+----------+------------+----------+------+------+------------+-------+-------+
```
#### quota list

list all directory quotas of fs

Usage:

```shell
dingo quota list --fsid 1
dingo quota list --fsname dingofs
```

Output:

```shell
+----------+------------+----------+------+------+------------+-------+-------+
|    ID    |    PATH    | CAPACITY | USED | USE% |   INODES   | IUSED | IUSE% |
+----------+------------+----------+------+------+------------+-------+-------+
| 10485760 | /quotadir1 | 10 GiB   | 6 B  | 0    | 20,000,000 | 1     | 0     |
+----------+------------+----------+------+------+------------+-------+-------+
| 2097152  | /quotadir2 | 100 GiB  | 0 B  | 0    | unlimited  | 0     |       |
+----------+------------+----------+------+------+------------+-------+-------+
```

#### quota delete

delete quota of a directory

Usage:

```shell
dingo quota delete --fsid 1 --path /quotadir
```
#### quota check

check quota of a directory

Usage:

```shell
dingo quota check --fsid 1 --path /quotadir
dingo quota check --fsid 1 --path /quotadir --repair
```


Output:

```shell
+----------+------------+----------------+------+----------+------------+-------+-----------+---------+
|    ID    |    NAME    |    CAPACITY    | USED | REALUSED |   INODES   | IUSED | REALIUSED | STATUS  |
+----------+------------+----------------+------+----------+------------+-------+-----------+---------+
| 10485760 | /quotadir | 10,737,418,240 | 22   | 22       | 20,000,000 | 2     | 22        | success |
+----------+------------+----------------+------+----------+------------+-------+-----------+---------+

or

+----------+------------+----------------+------+----------+------------+-------+-----------+--------+
|    ID    |    NAME    |    CAPACITY    | USED | REALUSED |   INODES   | IUSED | REALIUSED | STATUS |
+----------+------------+----------------+------+----------+------------+-------+-----------+--------+
| 10485760 | /quotadir | 10,737,418,240 | 22   | 33       | 20,000,000 | 2     | 3         | failed |
+----------+------------+----------------+------+----------+------------+-------+-----------+--------+
```

## Comparison of old and new commands

### dingo fs

| old                            | new                        |
| ------------------------------ | -------------------------- |
| dingofs_tool check-copyset     | dingo check copyset     |
| dingofs_tool create-fs         | dingo create fs         |
| dingofs_tool create-topology   | dingo create topology   |
| dingofs_tool delete-fs         | dingo delete fs         |
| dingofs_tool list-copyset      | dingo list copyset      |
| dingofs_tool list-fs           | dingo list fs           |
| dingofs_tool list-fs           | dingo list mountpoint   |
| dingofs_tool list-partition    | dingo list partition    |
| dingofs_tool query-copyset     | dingo query copyset     |
| dingofs_tool query-fs          | dingo query fs          |
| dingofs_tool query-inode       | dingo query inode       |
| dingofs_tool query-metaserver  | dingo query metaserver  |
| dingofs_tool query-partition   | dingo query partition   |
| dingofs_tool status-mds        | dingo status mds        |
| dingofs_tool status-metaserver | dingo status metaserver |
| dingofs_tool status-etcd       | dingo status etcd       |
| dingofs_tool status-copyset    | dingo status copyset    |
| dingofs_tool status-cluster    | dingo status cluster    |
| dingofs_tool umount-fs         | dingo umount fs         |
| dingofs_tool usage-inode       | dingo usage inode       |
| dingofs_tool usage-metadata    | dingo usage metadata    |