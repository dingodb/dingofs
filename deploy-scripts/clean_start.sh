#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string role 'mds' 'server role'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"


echo "============ stop ============"
$mydir/stop.sh --role=${FLAGS_role} 

sleep 1
echo "============ deploy ============"
$mydir/deploy.sh --role=${FLAGS_role}

sleep 1
echo "============ start ============"
$mydir/start.sh --role=${FLAGS_role}

sleep 1
echo ""============ done ============""