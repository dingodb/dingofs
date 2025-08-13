#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string role 'mdsv2' 'server role'
DEFINE_boolean clean_log 1 'clean log'
DEFINE_boolean replace_conf 0 'replace conf'
#DEFINE_string parameters 'deploy_parameters' 'server role'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"
echo "role: ${FLAGS_role}"
#echo "parameters: ${FLAGS_parameters}"

BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
DIST_DIR=$BASE_DIR/dist

if [ ! -d "$DIST_DIR" ]; then
  mkdir "$DIST_DIR"
fi

#source $mydir/${FLAGS_parameters}


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
  ln -s  "${srcpath}/build/bin/${server_name}" "${dstpath}/bin/${server_name}"

  # link dingo-mdsv2-client
  if [ "${role}" == "mdsv2" ]; then
    client_name="dingo-mdsv2-client"
    if [ -f "${dstpath}/bin/${client_name}" ]; then
      rm -f "${dstpath}/bin/${client_name}"
    fi
    ln -s  "${srcpath}/build/bin/${client_name}" "${dstpath}/bin/${client_name}"
  fi

  if [ "${FLAGS_replace_conf}" == "0" ]; then
    # conf file
    dist_conf="${dstpath}/conf/${server_name}.conf"
    cp $srcpath/confv2/${server_name}.template.conf $dist_conf

    sed  -i 's,\$INSTANCE_ID\$,'"$instance_id"',g'                  $dist_conf
    sed  -i 's,\$SERVER_HOST\$,'"$SERVER_HOST"',g'                  $dist_conf
    sed  -i 's,\$SERVER_LISTEN_HOST\$,'"$SERVER_LISTEN_HOST"',g'    $dist_conf
    sed  -i 's,\$SERVER_PORT\$,'"$server_port"',g'                  $dist_conf
    sed  -i 's,\$BASE_PATH\$,'"$dstpath"',g'                        $dist_conf

    # gflags file
    if [ -f $srcpath/conf/${server_name}.gflags ]
    then
        cp $srcpath/confv2/${server_name}.gflags $dstpath/conf/
    fi

    # coor_list file
    coor_file="${dstpath}/conf/coor_list"
    echo $COORDINATOR_ADDR > $coor_file

  fi

  if [ "${FLAGS_clean_log}" != "0" ]; then
    rm -rf $dstpath/log/*
  fi
}

program_dir=$BASE_DIR/dist/${FLAGS_role}

if [ ${FLAGS_role} == "mdsv2" ]; then
  echo "============ deploy mdsv2 ============"
  # print the parameters
  echo "FLAGS_role: ${FLAGS_role}"
  echo "BASE_DIR: ${BASE_DIR}"
  echo "program_dir: ${program_dir}"
  echo "MDSV2_INSTANCE_START_ID: ${MDSV2_INSTANCE_START_ID}"
  echo "SERVER_START_PORT: ${SERVER_START_PORT}"
  deploy_server ${FLAGS_role} ${BASE_DIR} ${program_dir} ${MDSV2_INSTANCE_START_ID} ${SERVER_START_PORT}
fi
