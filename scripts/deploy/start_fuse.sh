#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string fsname 'mds_fs_mono' 'server role'
DEFINE_string fstype 'vfs_v2' 'fs type'
DEFINE_string mountpoint '' 'mount point'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

USER=`whoami`
echo "user: ${USER}"

if [ "$USER" != "root" ]; then
  echo "please use root to start fuse"
  exit 1
fi


if [ -z "${FLAGS_fsname}" ]; then
    echo "fs name is empty"
    exit -1
fi

if [ -z "${FLAGS_fstype}" ]; then
    echo "fs type is empty"
    exit -1
fi

if [ -z "${FLAGS_mountpoint}" ]; then
    echo "mountpoint is empty"
    exit -1
fi

echo "start fuse fsname(${FLAGS_fsname}) fstype(${FLAGS_fstype}) mountpoint(${FLAGS_mountpoint})"

BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
FUSE_BASE_DIR=$BASE_DIR/dist/fuse
FUSE_BIN_PATH=$FUSE_BASE_DIR/bin/dingo-fuse
FUSE_CONF_PATH=$FUSE_BASE_DIR/conf/client.conf

cd $FUSE_BASE_DIR

set -x

nohup ${FUSE_BIN_PATH} -f -o default_permissions -o allow_other \
-o fsname=${FLAGS_fsname} -o fstype=$FLAGS_fstype -o user=dingofs \
-o conf=${FUSE_CONF_PATH} ${FLAGS_mountpoint} 2>&1 > ./log/out &