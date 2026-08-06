#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_integer cluster_id 0 'cluster id'
DEFINE_string parameters 'mds_deploy_parameters.local' 'deploy parameters file'


# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

echo "cluster_id: ${FLAGS_cluster_id}"

source $mydir/${FLAGS_parameters}


#check cluster id is valid
if [ ${FLAGS_cluster_id} -le 0 ]; then
    echo "cluster id is invalid"
    exit -1
fi


BASE_DIR=$(dirname $(dirname $(cd $(dirname $0); pwd)))
BUILD_DIR=$BASE_DIR/build
MDS_CLIENT_BIN_PATH=$BUILD_DIR/bin/dingo-mds-client


output=`$MDS_CLIENT_BIN_PATH --cmd=CreateAllTable --cluster_id=${FLAGS_cluster_id} --coor_addr=list://${COORDINATOR_ADDR} 2>&1`
is_fail=`echo $output | grep "rpc fail" | wc -l`
if [ $is_fail -eq 1 ]; then
  echo "create cluster fail, $output"
  exit 1
fi

is_success=`echo $output | grep success |  wc -l`
if [ $is_success -eq 0 ]; then
  echo "cluster ${FLAGS_cluster_id} create fail"
  exit 1
fi

echo "cluster ${FLAGS_cluster_id} create success"
