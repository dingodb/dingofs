#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_integer server_num 1 'server number'
DEFINE_boolean clean_log 1 'clean log'
DEFINE_boolean replace_conf 0 'replace conf'
DEFINE_string parameters 'mds_deploy_parameters' 'deploy parameters file'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"
echo "parameters: ${FLAGS_parameters}"

BASE_DIR=$(dirname $(dirname $(cd $(dirname $0); pwd)))
DIST_DIR=$BASE_DIR/dist

# validate BASE_DIR and BASE_DIR/src and BASE_DIR/build
if [ ! -d "$BASE_DIR" ] || [ ! -d "$BASE_DIR/src" ] || [ ! -d "$BASE_DIR/build" ]; then
  echo "error: script run dir wrong, please run this script in scripts/dev-mds dir."
  exit 1
fi

SERVER_NAME=mds
SERVER_BIN_NAME=dingo-mds
MDS_CLIENT_BIN_NAME=dingo-mds-client

if [ ! -d "$DIST_DIR" ]; then
  mkdir "$DIST_DIR"
fi

source $mydir/${FLAGS_parameters}


function deploy_server() {
  srcpath=$1
  dstpath=$2
  instance_id=$3
  server_port=$4

  echo "server $dstpath $instance_id $server_port"

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


  # hard link server binary
  if [ -f "${dstpath}/bin/${SERVER_BIN_NAME}" ]; then
    rm -f "${dstpath}/bin/${SERVER_BIN_NAME}"
  fi
  ln "${srcpath}/build/bin/${SERVER_BIN_NAME}" "${dstpath}/bin/${SERVER_BIN_NAME}"

  # link dingo-mds-client
  if [ -f "${dstpath}/bin/${MDS_CLIENT_BIN_NAME}" ]; then
    rm -f "${dstpath}/bin/${MDS_CLIENT_BIN_NAME}"
  fi
  ln -s "${srcpath}/build/bin/${MDS_CLIENT_BIN_NAME}" "${dstpath}/bin/${MDS_CLIENT_BIN_NAME}"



  if [ "${FLAGS_replace_conf}" == "0" ]; then
    # conf file
    dist_conf="${dstpath}/conf/${SERVER_NAME}.conf"
    cp $srcpath/scripts/dev-mds/${SERVER_NAME}.template.conf $dist_conf

    sed  -i 's,\$CLUSTER_ID\$,'"$CLUSTER_ID"',g'                    $dist_conf
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
  instance_dist_dir=$DIST_DIR/$SERVER_NAME-$i

  deploy_server ${BASE_DIR} ${instance_dist_dir} `expr ${MDS_INSTANCE_START_ID} + ${i}` `expr ${SERVER_START_PORT} + ${i}`
done

echo "deploy finish..."
