#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string role 'mds' 'server role'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"


echo "============ stop ============"
"$mydir/stop.sh" --role="${FLAGS_role}" || exit "$?"

sleep 1
echo "============ deploy ============"
"$mydir/deploy.sh" --role="${FLAGS_role}" || exit "$?"

sleep 1
echo "============ start ============"
# Preserve the server's exit status and deliver container signals directly.
exec "$mydir/start.sh" --role="${FLAGS_role}"