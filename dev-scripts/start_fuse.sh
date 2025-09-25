#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string fsname '' 'server role'
DEFINE_string fstype '' 'fs type'
DEFINE_string mountpoint '' 'mount point'
DEFINE_integer num 1 'fuse number'
DEFINE_integer force 1 'use kill -9 to stop'
DEFINE_boolean just_stop true 'just stop fuse, do not start'

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
FUSE_CONF_DIR=$FUSE_BASE_DIR/conf
FUSE_CONF_TEMPLATE=$FUSE_BASE_DIR/conf/client.template.conf
FUSE_CACHE_DIR=$FUSE_BASE_DIR/cache
FUSE_LOG_DIR=$FUSE_BASE_DIR/log

cd $FUSE_BASE_DIR

# set -x

# check if conf dir exist
if [ ! -d "$FUSE_CONF_DIR" ]; then
    mkdir -p $FUSE_CONF_DIR
fi

# check if cache dir exist
if [ ! -d "$FUSE_CACHE_DIR" ]; then
    mkdir -p $FUSE_CACHE_DIR
fi

# check if log dir exist
if [ ! -d "$FUSE_LOG_DIR" ]; then
    mkdir -p $FUSE_LOG_DIR
fi

function gen_conf() {
    index=$1
    prefix_name=${FLAGS_fsname}-${index}
    dist_conf="${FUSE_CONF_DIR}/client-${prefix_name}.conf"

    if [ ! -f "$FUSE_CONF_TEMPLATE" ]; then
        echo "fuse conf template(${FUSE_CONF_TEMPLATE}) already exist."
        exit -1
    fi

    cp $FUSE_CONF_TEMPLATE $dist_conf

    cache_dir=$FUSE_CACHE_DIR/${prefix_name}
    log_dir=$FUSE_LOG_DIR/${prefix_name}
    dummy_port=$(($RANDOM%20000 + 10000))

    sed  -i 's,\$CACHE_DIR\$,'"$cache_dir"',g'              $dist_conf
    sed  -i 's,\$LOG_DIR\$,'"$log_dir"',g'                  $dist_conf

    sed  -i 's,\$DUMMY_PORT\$,'"$dummy_port"',g'            $dist_conf
}


wait_for_process_exit() {
  local pid_killed=$1
  local begin=$(date +%s)
  local end
  while kill -0 $pid_killed > /dev/null 2>&1
  do
    echo -n "."
    sleep 1;
    end=$(date +%s)
    if [ $((end-begin)) -gt 60  ];then
      echo -e "\nTimeout"
      break;
    fi
  done
}


function stop() {
    process_no=$(ps -ef | grep ${FUSE_BASE_DIR} | grep $FLAGS_fsname | awk '{print $2}' | xargs)

    if [ "${process_no}" != "" ]; then
        echo "pid to kill: ${process_no}"
        if [ ${FLAGS_force} -eq 0 ]
        then
            kill ${process_no}
        else
            kill -9 ${process_no}
        fi

        wait_for_process_exit ${process_no}
    fi
}

function umount() {
    for ((i=1; i<=${FLAGS_num}; i++)); do
        index=$i
        prefix_name=${FLAGS_fsname}-${index}
        mountpoint_dir=${FLAGS_mountpoint}/${prefix_name}

        echo "umount ${mountpoint_dir}"
        fusermount -uz ${mountpoint_dir}
    done
}

function start() {
    index=$1
    prefix_name=${FLAGS_fsname}-${index}
    fuse_conf_path="${FUSE_CONF_DIR}/client-${prefix_name}.conf"
    log_dir=$FUSE_LOG_DIR/${prefix_name}
    mountpoint_dir=${FLAGS_mountpoint}/${prefix_name}

    if [ ! -d "${log_dir}" ]; then
        mkdir -p ${log_dir}
    else
        rm -rf ${log_dir}/*
    fi

    # echo "umount ${mountpoint_dir}"
    # fusermount -uz ${mountpoint_dir}
    # sleep 1

    if [ ! -d "${mountpoint_dir}" ]; then
        mkdir -p ${mountpoint_dir}
    fi

    if [ ! -f "${fuse_conf_path}" ]; then
        echo "fuse conf file(${fuse_conf_path}) not exist"
        exit -1
    fi

    nohup ${FUSE_BIN_PATH} -f \
        -o default_permissions \
        -o allow_other \
        -o max_threads=64 \
        -o fsname=${FLAGS_fsname} \
        -o fstype=${FLAGS_fstype} \
        -o user=dengzihui \
        -o conf=${fuse_conf_path} ${mountpoint_dir} 2>&1 > $log_dir/out &

}

# stop fuse
stop

# echo "wait for 3 seconds to stop fuse."
sleep 3

# umount fuse
umount

if [ ${FLAGS_just_stop} = 0 ]; then
    echo "just stop fuse, do not start"
    exit 0
fi

echo "wait for 1 seconds to start fuse."
sleep 1

for ((i=1; i<=${FLAGS_num}; i++)); do
    gen_conf $i
    if [ $? -ne 0 ]; then
        echo "gen fuse conf fail"
        exit -1
    fi

    start $i
    if [ $? -ne 0 ]; then
        echo "start fuse fail"
        exit -1
    fi
    sleep 1
done