#!/bin/bash
ulimit -c unlimited 
mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags


DEFINE_string mds '' 'mds addr'
DEFINE_string group 'cache_test' 'group name'
DEFINE_string cache_dir '' 'cache directory'
DEFINE_integer force 1 'use kill -9 to stop'
DEFINE_boolean stop false 'just stop client, do not start'
DEFINE_boolean clean_log false 'clean log'
DEFINE_integer port 39000 'server listen port'


# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

if [ -z "${FLAGS_mds}" ]; then
    echo "mds addr is empty"
    exit -1
fi

if [ -z "${FLAGS_cache_dir}" ]; then
    echo "cache directory is empty"
    exit -1
fi

BASE_DIR=$(dirname $(dirname $(cd $(dirname $0); pwd)))
CACHE_BASE_DIR=$BASE_DIR/dist/cache
CACHE_BIN_PATH=$CACHE_BASE_DIR/bin/dingo-cache  
CACHE_LOG_DIR=$CACHE_BASE_DIR/log
  
CACHE_NODE_LIST="27BF3B69-F44C-43BD-A1CC-A2F5374150F1,1B7E4F23-9C8E-4A42-8DEA-969C80BC693E"

function start() {
  id=$1
  index=$2
  log_dir=$CACHE_LOG_DIR/${id}

  listen_port=$((${FLAGS_port} + ${index}))

    if [ ! -d "${log_dir}" ]; then
      mkdir -p ${log_dir}
    else
      if [ ${FLAGS_clean_log} == 0 ]; then
        rm -rf ${log_dir}/*
      fi
    fi

  
  echo "start cache node(${id}) listen_port(${listen_port}) group_name(${FLAGS_group}) log_dir(${log_dir})"

  ${CACHE_BIN_PATH} \
  --id=${id} \
  --mds_addrs=${FLAGS_mds} \
  --listen_ip=0.0.0.0 \
  --listen_port=${listen_port} \
  --group_name=${FLAGS_group} \
  --group_weight=100 \
  --cache_dir=${FLAGS_cache_dir} \
  --cache_size_mb=1048576 \
  --log_dir=${log_dir} \
  --log_level=INFO \
  --daemonize=true 2>&1 > $log_dir/out
}

function wait_for_process_exit() {
  local pid
  for pid in "$@"; do
    while kill -0 "$pid" > /dev/null 2>&1; do
      sleep 1
    done
  done
}

function stop() {
  process_no=$(ps -eo pid=,args= | awk -v group_name="${FLAGS_group}" \
    '$0 ~ /dingo-cache([[:space:]]|$)/ &&
     index($0, "--group_name=" group_name) {print $1}' | xargs)

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



if [ ${FLAGS_stop} = 0 ]; then
  echo "# stop cache node"

  stop

  echo "# stop cache node done"

else
  echo "# start cache node"

  # traverse CACHE_NODE_LIST
  IFS=',' read -ra CACHE_NODE_ARRAY <<< "$CACHE_NODE_LIST"
  index=0
  for id in "${CACHE_NODE_ARRAY[@]}"; do
    start $id $index
    index=$((index + 1))
  done

  echo "# start cache node done"

fi
