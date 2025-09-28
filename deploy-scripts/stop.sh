#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string role 'mds' 'server role'
DEFINE_integer force 0 'use kill -9 to stop'
DEFINE_integer use_pgrep 0 'use pgrep to get pid'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

echo "stop role(${FLAGS_role}) server"

BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
DIST_DIR=$BASE_DIR/dist


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


if [ ${FLAGS_use_pgrep} -ne 0 ]; then
    process_no=$(pgrep -f "${BASE_DIR}.*dingo-${FLAGS_role}" -U `id -u` | xargs)

    if [ "${process_no}" != "" ]; then
        echo "pid to kill: ${process_no}"
        if [ ${FLAGS_force} -eq 0 ]
        then
            kill ${process_no}
        else
            kill -9 ${process_no}
        fi

        wait_for_process_exit ${process_no}
    else
        echo "not exist ${FLAGS_role} process"
    fi
else
    program_dir=$BASE_DIR/dist/${FLAGS_role}
    pid_file=${program_dir}/log/pid

    # Check if the PID file exists
    if [ -f "$pid_file" ]; then
        # Read the PID from the file
        pid=$(<"$pid_file")

        # Check if the PID is a number
        if [[ "$pid" =~ ^[0-9]+$ ]]; then
            # Kill the process with the specified PID
            if [ ${FLAGS_force} -eq 0 ]
            then
                echo "killing process with pid($pid) on $pid_file"
                kill ${pid}
            else
                echo "killing -9 process with pid($pid) on $pid_file"
                kill -9 ${pid}
            fi

            wait_for_process_exit ${pid}
        else
            echo "invalid pid($pid) on $pid_file"
        fi
    else
        echo "not found $pid_file"
    fi
fi

echo "stop finish..."
