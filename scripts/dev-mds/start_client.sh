#!/bin/bash
ulimit -c unlimited 
mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags


DEFINE_string meta '' 'meta url'
DEFINE_string mountpoint '' 'mount point'
DEFINE_integer num 1 'client number'
DEFINE_integer force 1 'use kill -9 to stop'
DEFINE_boolean stop false 'just stop client, do not start'
DEFINE_boolean upgrade true 'upgrade client'
DEFINE_boolean loop false 'loop restart client'
DEFINE_boolean clean_log false 'clean log'


# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

USER=`whoami`
echo "user: ${USER}"

if [ "$USER" != "root" ]; then
  echo "please use root to start client"
  exit 1
fi

if [ -z "${FLAGS_meta}" ]; then
    echo "meta url is empty"
    exit -1
fi

if [ -z "${FLAGS_mountpoint}" ]; then
    echo "mountpoint is empty"
    exit -1
fi

echo "params: meta(${FLAGS_meta}) mountpoint(${FLAGS_mountpoint})"

# meta url format:
# mds://${ip}:${port}/${fsname}
# local://${path}/${fsname}
# memory://${fsname}
FSNAME=$(echo ${FLAGS_meta} | awk -F'/' '{print $NF}')

echo "fsname: ${FSNAME}"

BASE_DIR=$(dirname $(dirname $(cd $(dirname $0); pwd)))
CLIENT_BASE_DIR=$BASE_DIR/dist/client
CLIENT_BIN_PATH=$CLIENT_BASE_DIR/bin/dingo-client
CLIENT_CONF_DIR=$CLIENT_BASE_DIR/conf
CLIENT_CACHE_DIR=$CLIENT_BASE_DIR/cache
CLIENT_LOG_DIR=$CLIENT_BASE_DIR/log

cd $CLIENT_BASE_DIR

# set -x

# check if conf dir exist
if [ ! -d "$CLIENT_CONF_DIR" ]; then
    mkdir -p $CLIENT_CONF_DIR
fi

# check if cache dir exist
if [ ! -d "$CLIENT_CACHE_DIR" ]; then
    mkdir -p $CLIENT_CACHE_DIR
fi

# check if log dir exist
if [ ! -d "$CLIENT_LOG_DIR" ]; then
    mkdir -p $CLIENT_LOG_DIR
fi

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
    process_no=$(ps -ef | grep ${CLIENT_BASE_DIR} | grep $FSNAME | awk '{print $2}' | xargs)

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
        prefix_name=${FSNAME}-${index}
        mountpoint_dir=${FLAGS_mountpoint}/${prefix_name}

        echo "umount ${mountpoint_dir}"
        fusermount -uz ${mountpoint_dir}
    done
}

function start() {
    index=$1
    prefix_name=${FSNAME}-${index}
    log_dir=$CLIENT_LOG_DIR/${prefix_name}
    mountpoint_dir=${FLAGS_mountpoint}/${prefix_name}

    if [ ! -d "${log_dir}" ]; then
        mkdir -p ${log_dir}
    else
        if [ ${FLAGS_clean_log} == 0 ]; then
            rm -rf ${log_dir}/*
        fi
    fi

    # echo "umount ${mountpoint_dir}"
    # fusermount -uz ${mountpoint_dir}
    # sleep 1

    if [ ! -d "${mountpoint_dir}" ]; then
        mkdir -p ${mountpoint_dir}
    fi

    dummy_port=$(($RANDOM%20000 + 10000))

    ${CLIENT_BIN_PATH} ${FLAGS_meta} ${mountpoint_dir} \
    --log_dir=${log_dir} \
    --vfs_dummy_server_port=${dummy_port} \
    --cache_store=none \
    --daemonize=true 2>&1 > $log_dir/out
}

if [ ${FLAGS_stop} = 0 ]; then
    echo "# stop client"

    # stop client
    stop

    # echo "wait for 3 seconds to stop client."
    sleep 3

    # umount client
    umount

    sleep 1

    echo "# done"
    exit 0
fi

if [ ${FLAGS_upgrade} != 0 ]; then
    echo "# restart client"

    # stop client
    stop

    # echo "wait for 3 seconds to stop client."
    sleep 3

    # umount client
    umount

    sleep 1
else
    echo "# upgrade client"
fi


if [ ${FLAGS_loop} == 0 ]; then

    for n in {1..100}; do
        echo "iteration epoch $n"

        for ((i=1; i<=${FLAGS_num}; i++)); do
            start $i
            if [ $? -ne 0 ]; then
                echo "start client fail"
                exit -1
            fi
            sleep 1
        done

        sleep 10
    done

else

    for ((i=1; i<=${FLAGS_num}; i++)); do
        start $i
        if [ $? -ne 0 ]; then
            echo "start client fail"
            exit -1
        fi
        sleep 1
    done

fi


echo "# done"
