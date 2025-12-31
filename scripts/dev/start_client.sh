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
CLIENT_CONF_TEMPLATE=$CLIENT_BASE_DIR/conf/client.template.conf
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

function gen_conf() {
    index=$1
    prefix_name=${FSNAME}-${index}
    dist_conf="${CLIENT_CONF_DIR}/client-${prefix_name}.conf"

    if [ ! -f "$CLIENT_CONF_TEMPLATE" ]; then
        echo "client conf template(${CLIENT_CONF_TEMPLATE}) already exist."
        exit -1
    fi

    cp $CLIENT_CONF_TEMPLATE $dist_conf

    cache_dir=$CLIENT_CACHE_DIR/${prefix_name}
    log_dir=$CLIENT_LOG_DIR/${prefix_name}
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
    client_conf_path="${CLIENT_CONF_DIR}/client-${prefix_name}.conf"
    log_dir=$CLIENT_LOG_DIR/${prefix_name}
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

    if [ ! -f "${client_conf_path}" ]; then
        echo "client conf file(${client_conf_path}) not exist"
        exit -1
    fi

    ${CLIENT_BIN_PATH} --daemonize=true --conf ${client_conf_path} ${FLAGS_meta} ${mountpoint_dir} 2>&1 > $log_dir/out &

}

if [ ${FLAGS_stop} = 0 ]; then
    echo "# stop client"

    # stop client
    stop

    # echo "wait for 3 seconds to stop client."
    sleep 3

    # umount client
    umount

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
else
    echo "# upgrade client"
fi

echo "wait for 1 seconds to start client."
sleep 1

for ((i=1; i<=${FLAGS_num}; i++)); do
    gen_conf $i
    if [ $? -ne 0 ]; then
        echo "gen client conf fail"
        exit -1
    fi

    start $i
    if [ $? -ne 0 ]; then
        echo "start client fail"
        exit -1
    fi
    sleep 1
done

echo "# done"
