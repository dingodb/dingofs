#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_integer server_num 1 'server number'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"


echo "============ stop ============"
./stop_mds.sh --server_num=${FLAGS_server_num}

sleep 1
echo "============ deploy ============"
./deploy_mds.sh --server_num=${FLAGS_server_num}

sleep 1
echo "============ start ============"
./start_mds.sh --server_num=${FLAGS_server_num}

sleep 1
echo "============ done ============"