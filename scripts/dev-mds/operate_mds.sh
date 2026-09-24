#!/bin/bash
# operate_mds.sh -- 开发环境 MDS 运维脚本
# 合并自 deploy_mds.sh / start_mds.sh / stop_mds.sh / clean_start.sh
#
# usage: operate_mds.sh {stop|deploy|start|restart} [flags]

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_integer server_num 3 'server number'
DEFINE_boolean clean_log true 'clean log'
DEFINE_boolean replace_conf true 'replace conf'
DEFINE_string env 'env.local' 'deploy env file'
DEFINE_boolean force true 'use kill -9 to stop'
DEFINE_boolean use_pgrep true 'use pgrep to get pid'

FLAGS_HELP="usage: operate_mds.sh {stop|deploy|start|restart} [flags]

  stop     停止 MDS 实例（默认按 dist/mds-<i>/log/pid，--use_pgrep 时按进程名）
  deploy   重新生成 dist/mds-<i>：软链二进制、渲染 mds.conf
  start    启动 MDS 实例
  restart  等价于 stop + deploy + start
"

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

SERVER_NAME=mds
SERVER_BIN_NAME=dingo-mds
MDS_CLIENT_BIN_NAME=dingo-mds-client

BASE_DIR=$(dirname $(dirname $(cd $(dirname $0); pwd)))
DIST_DIR=$BASE_DIR/dist

function wait_for_process_exit() {
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
      return 1
    fi
  done
  return 0
}

function do_stop() {
  echo "============ stop ============"
  echo "stop server num(${FLAGS_server_num})"

  if [ "${FLAGS_use_pgrep}" -eq "${FLAGS_TRUE}" ]; then
    # Match on the conf path, not BASE_DIR: BASE_DIR depends on how the
    # script was invoked (symlinked vs real path), while the started
    # process always carries --conf=.../dist/mds-<i>/conf/mds.conf.
    process_no=$(pgrep -f -U `id -u` -- "--conf=.*/dist/${SERVER_NAME}-[0-9]+/conf/${SERVER_NAME}\.conf" | xargs)

    if [ "${process_no}" != "" ]; then
      echo "pid to kill: ${process_no}"
      if [ "${FLAGS_force}" -eq "${FLAGS_TRUE}" ]
      then
        kill -9 ${process_no}
      else
        kill ${process_no}
      fi

      wait_for_process_exit ${process_no} || return 1
    else
      echo "not exist ${SERVER_NAME} process"
    fi
  else
    for ((i=1; i<=$FLAGS_server_num; ++i)); do
      pid_file=$DIST_DIR/${SERVER_NAME}-${i}/log/pid

      # Check if the PID file exists
      if [ -f "$pid_file" ]; then
        # Read the PID from the file
        pid=$(<"$pid_file")

        # Check if the PID is a number
        if [[ "$pid" =~ ^[0-9]+$ ]]; then
          # Kill the process with the specified PID
          if [ "${FLAGS_force}" -eq "${FLAGS_TRUE}" ]
          then
            echo "killing -9 process with pid($pid) on $pid_file"
            kill -9 ${pid}
          else
            echo "killing process with pid($pid) on $pid_file"
            kill ${pid}
          fi

          wait_for_process_exit ${pid} || return 1
        else
          echo "invalid pid($pid) on $pid_file"
        fi
      else
        echo "not found $pid_file"
      fi
    done
  fi

  echo "stop finish..."
}

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


  # server binary and dingo-mds-client are symlinks into build/bin;
  # rm -f (not [ -f ] && rm) because a dangling symlink is not -f.
  rm -f "${dstpath}/bin/${SERVER_BIN_NAME}" "${dstpath}/bin/${MDS_CLIENT_BIN_NAME}"
  ln -s "${srcpath}/build/bin/${SERVER_BIN_NAME}" "${dstpath}/bin/${SERVER_BIN_NAME}"
  ln -s "${srcpath}/build/bin/${MDS_CLIENT_BIN_NAME}" "${dstpath}/bin/${MDS_CLIENT_BIN_NAME}"


  if [ "${FLAGS_replace_conf}" -eq "${FLAGS_TRUE}" ]; then
    # conf file
    dist_conf="${dstpath}/conf/${SERVER_NAME}.conf"
    cp $srcpath/scripts/dev-mds/${SERVER_NAME}.template.conf $dist_conf

    sed  -i 's,\$CLUSTER_ID,'"$CLUSTER_ID"',g'                    $dist_conf
    sed  -i 's,\$INSTANCE_ID,'"$instance_id"',g'                  $dist_conf
    sed  -i 's,\$SERVER_HOST,'"$SERVER_HOST"',g'                  $dist_conf
    sed  -i 's,\$SERVER_LISTEN_HOST,'"$SERVER_LISTEN_HOST"',g'    $dist_conf
    sed  -i 's,\$SERVER_PORT,'"$server_port"',g'                  $dist_conf
    sed  -i 's,\$BASE_PATH,'"$dstpath"',g'                        $dist_conf
    sed  -i 's,\$STORAGE_ENGINE,'"$STORAGE_ENGINE"',g'            $dist_conf
    sed  -i 's,\$STORAGE_URL,'"$STORAGE_URL"',g'                  $dist_conf
    sed  -i 's,\$LOG_LEVEL,'"$LOG_LEVEL"',g'                      $dist_conf
    sed  -i 's,\$LOG_V,'"$LOG_V"',g'                              $dist_conf

    # coor_list file
    coor_file="${dstpath}/conf/coor_list"
    echo $COORDINATOR_ADDR > $coor_file

  fi

  if [ "${FLAGS_clean_log}" -eq "${FLAGS_TRUE}" ]; then
    rm -rf $dstpath/log/*
  fi
}

function do_deploy() {
  echo "============ deploy ============"
  echo "env: ${FLAGS_env}"

  # validate BASE_DIR and BASE_DIR/src and BASE_DIR/build
  if [ ! -d "$BASE_DIR" ] || [ ! -d "$BASE_DIR/src" ] || [ ! -d "$BASE_DIR/build" ]; then
    echo "error: script run dir wrong, please run this script in scripts/dev-mds dir."
    return 1
  fi

  if [ ! -f "$mydir/${FLAGS_env}" ]; then
    echo "error: env file not found: $mydir/${FLAGS_env}"
    return 1
  fi
  source $mydir/${FLAGS_env} || return 1

  if [ ! -d "$DIST_DIR" ]; then
    mkdir "$DIST_DIR"
  fi

  for ((i=1; i<=$FLAGS_server_num; ++i)); do
    instance_dist_dir=$DIST_DIR/$SERVER_NAME-$i

    deploy_server ${BASE_DIR} ${instance_dist_dir} `expr ${MDS_INSTANCE_START_ID} + ${i}` `expr ${SERVER_START_PORT} + ${i}`
  done

  echo "deploy finish..."
}

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
  root_dir=$1

  set_ulimit

  cd ${root_dir}


  echo "start server: ${root_dir}/bin/${SERVER_BIN_NAME}"

  ${root_dir}/bin/${SERVER_BIN_NAME} --daemonize=true --conf=${root_dir}/conf/${SERVER_NAME}.conf 2>&1 >./log/out &
}

function do_start() {
  echo "============ start ============"
  echo "start server num(${FLAGS_server_num})"

  for ((i=1; i<=${FLAGS_server_num}; ++i)); do
    ininstance_dist_dir=$DIST_DIR/$SERVER_NAME-$i

    start_server ${ininstance_dist_dir}
  done

  echo "start finish..."
}

case "${1:-}" in
  stop)
    do_stop
    ;;
  deploy)
    do_deploy
    ;;
  start)
    do_start
    ;;
  restart)
    do_stop || exit 1
    sleep 1
    do_deploy || exit 1
    sleep 1
    do_start
    sleep 1
    echo "============ done ============"
    ;;
  *)
    echo "error: unknown action '${1:-}'" >&2
    flags_help
    exit 1
    ;;
esac
