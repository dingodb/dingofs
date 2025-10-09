#!/bin/bash
# The ulimit is setup in start_server, the paramter of ulimit is:
#   ulimit -n 1048576
#   ulimit -u 4194304
#   ulimit -c unlimited
# If set ulimit failed, please use root or sudo to execute sysctl.sh to increase kernal limit.

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string role 'mds' 'server role'
DEFINE_integer server_num 1 'server number'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

echo "start role(${FLAGS_role}) server num(${FLAGS_server_num})"

BASE_DIR=$(dirname $(dirname $(cd $(dirname $0); pwd)))
DIST_DIR=$BASE_DIR/dist



function set_ulimit() {
    NUM_FILE=1048576
    NUM_PROC=4194304

    # 1. sysctl is the very-high-level hard limit:
    #     fs.nr_open = 1048576
    #     fs.file-max = 4194304
    # 2. /etc/security/limits.conf is the second-level limit for users, this is not required to setup.
    #    CAUTION: values in limits.conf can't bigger than sysctl kernel values, or user login will fail.
    #     * - nofile 1048576
    #     * - nproc  4194304
    # 3. we can use ulimit to set value before start service.
    #     ulimit -n 1048576
    #     ulimit -u 4194304
    #     ulimit -c unlimited

    # ulimit -n
    nfile=$(ulimit -n)
    echo "nfile="${nfile}
    if [ ${nfile} -lt ${NUM_FILE} ]
    then
        echo "try to increase nfile"
        ulimit -n ${NUM_FILE}

        nfile=$(ulimit -n)
        echo "nfile new="${nfile}
        if [ ${nfile} -lt ${NUM_FILE} ]
        then
            echo "need to increase nfile to ${NUM_FILE}, exit!"
            exit -1
        fi
    fi

    # ulimit -c
    ncore=$(ulimit -c)
    echo "ncore="${ncore}
    if [ ${ncore} != "unlimited" ]
    then
        echo "try to set ulimit -c unlimited"
        ulimit -c unlimited

        ncore=$(ulimit -c)
        echo "ncore new="${ncore}
        if [ ${ncore} != "unlimited" ]
        then
            echo "need to set ulimit -c unlimited, exit!"
            exit -1
        fi
    fi
}

function start_server() {
  role=$1
  root_dir=$2

  set_ulimit

  cd ${root_dir}

  server_name="dingo-${role}"
  echo "start server: ${root_dir}/bin/${server_name}"

  nohup ${root_dir}/bin/${server_name} --conf=${root_dir}/conf/${role}.conf 2>&1 >./log/out &
}


for ((i=1; i<=${FLAGS_server_num}; ++i)); do
  program_dir=$BASE_DIR/dist/${FLAGS_role}-${i}

  start_server ${FLAGS_role} ${program_dir}
done
