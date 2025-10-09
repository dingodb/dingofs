#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string role 'mds' 'server role'
DEFINE_integer server_num 1 'server number'
DEFINE_boolean clean_log 1 'clean log'
DEFINE_boolean replace_conf 0 'replace conf'
DEFINE_string parameters 'deploy_parameters' 'server role'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"
echo "role: ${FLAGS_role}"
echo "parameters: ${FLAGS_parameters}"

BASE_DIR=$(dirname $(dirname $(cd $(dirname $0); pwd)))
DIST_DIR=$BASE_DIR/dist

if [ ! -d "$DIST_DIR" ]; then
  mkdir "$DIST_DIR"
fi

source $mydir/${FLAGS_parameters}


function deploy_server() {
  role=$1
  srcpath=$2
  dstpath=$3
  instance_id=$4
  server_port=$5

  echo "server $dstpath $role $instance_id $server_port"

  if [ ! -d "$dstpath" ]; then
    mkdir "$dstpath"
  fi

  if [ ! -d "$dstpath/bin" ]; then
    mkdir "$dstpath/bin"
  fi
  if [ ! -d "$dstpath/conf" ]; then
    mkdir "$dstpath/conf"
  fi
  if [ ! -d "$dstpath/log" ]; then
    mkdir "$dstpath/log"
  fi

  server_name="dingo-${role}"
  if [ -f "${dstpath}/bin/${server_name}" ]; then
    rm -f "${dstpath}/bin/${server_name}"
  fi
  ln "${srcpath}/build/bin/${server_name}" "${dstpath}/bin/${server_name}"

  # link dingo-mds-client
  if [ "${role}" == "mds" ]; then
    client_name="dingo-mds-client"
    if [ -f "${dstpath}/bin/${client_name}" ]; then
      rm -f "${dstpath}/bin/${client_name}"
    fi
    ln -s "${srcpath}/build/bin/${client_name}" "${dstpath}/bin/${client_name}"
  fi

  if [ "${FLAGS_replace_conf}" == "0" ]; then
    # conf file
    dist_conf="${dstpath}/conf/${role}.conf"
    cp $srcpath/conf/${role}.template.conf $dist_conf

    sed  -i 's,\$INSTANCE_ID\$,'"$instance_id"',g'                  $dist_conf
    sed  -i 's,\$SERVER_HOST\$,'"$SERVER_HOST"',g'                  $dist_conf
    sed  -i 's,\$SERVER_LISTEN_HOST\$,'"$SERVER_LISTEN_HOST"',g'    $dist_conf
    sed  -i 's,\$SERVER_PORT\$,'"$server_port"',g'                  $dist_conf
    sed  -i 's,\$BASE_PATH\$,'"$dstpath"',g'                        $dist_conf
    
    # coor_list file
    coor_file="${dstpath}/conf/coor_list"
    echo $COORDINATOR_ADDR > $coor_file

  fi

  if [ "${FLAGS_clean_log}" != "0" ]; then
    rm -rf $dstpath/log/*
  fi
}

for ((i=1; i<=$FLAGS_server_num; ++i)); do
  program_dir=$BASE_DIR/dist/${FLAGS_role}-${i}

  if [ ${FLAGS_role} == "mds" ]; then
    deploy_server ${FLAGS_role} ${BASE_DIR} ${program_dir} `expr ${MDS_INSTANCE_START_ID} + ${i}` `expr ${SERVER_START_PORT} + ${i}`
  fi

done