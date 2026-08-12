#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string fs_name '' 'fs name'
DEFINE_string mds_addr '' 'mds address'
DEFINE_string parameters 'mds_deploy_parameters.local' 'deploy parameters file'
DEFINE_boolean use_local_datastore false 'use local datastore'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

echo "fs_name: ${FLAGS_fs_name}"
echo "mds_addr: ${FLAGS_mds_addr}"

#check fs name
if [ -z "${FLAGS_fs_name}" ]; then
    echo "fs name is empty"
    exit -1
fi
#check mds address
if [ -z "${FLAGS_mds_addr}" ]; then
    echo "mds address is empty"
    exit -1
fi

source $mydir/${FLAGS_parameters}

BASE_DIR=$(dirname $(dirname $(cd $(dirname $0); pwd)))
BUILD_DIR=$BASE_DIR/build
MDS_CLIENT_BIN_PATH=$BUILD_DIR/bin/dingo-mds-client



# check mds client binary exist
if [ ! -f "$MDS_CLIENT_BIN_PATH" ]; then
    echo "mds client binary not exist, please build it first"
    exit -1
fi


# Query the filesystem using the machine-readable output. A successful
# command means the filesystem exists. A not-found response is the only
# expected failure; all other failures abort the deployment.
if output=$("$MDS_CLIENT_BIN_PATH" \
    --cmd=getfs \
    --format=json \
    --color=never \
    --mds_addr="${FLAGS_mds_addr}" \
    --fs_name="${FLAGS_fs_name}" 2>&1); then
  echo "fs ${FLAGS_fs_name} already exist"
  exit 0
elif ! echo "$output" | grep -q 'NOT_FOUND'; then
  echo "get fs fail, $output"
  exit 1
fi

echo "fs ${FLAGS_fs_name} not exist, create it"

# create fs
output=""
if [ ${FLAGS_use_local_datastore} = 0 ]; then
  local_storage_path=${LOCAL_DATASTORE_PATH}/${FLAGS_fs_name}
  mkdir -p ${local_storage_path}
  
  if ! output=$("$MDS_CLIENT_BIN_PATH" \
      --cmd=createfs \
      --format=json \
      --color=never \
      --mds_addr="${FLAGS_mds_addr}" \
      --fs_name="${FLAGS_fs_name}" \
      --fs_partition_type=parent_hash \
      --storage_path="${LOCAL_DATASTORE_PATH}" 2>&1); then
    echo "create fs fail, $output"
    exit 1
  fi

else
  if ! output=$("$MDS_CLIENT_BIN_PATH" \
      --cmd=createfs \
      --format=json \
      --color=never \
      --mds_addr="${FLAGS_mds_addr}" \
      --fs_name="${FLAGS_fs_name}" \
      --fs_partition_type=parent_hash \
      --s3_endpoint="${S3_ENDPOINT}" \
      --s3_ak="${S3_AK}" \
      --s3_sk="${S3_SK}" \
      --s3_bucketname="${S3_BUCKETNAME}" 2>&1); then
    echo "create fs fail, $output"
    exit 1
  fi

fi

echo "fs ${FLAGS_fs_name} create success"





