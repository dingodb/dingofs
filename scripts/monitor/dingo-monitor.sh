#!/bin/sh

# This script is used to start or stop dingofs monitor system.
# You must be in the directory where dingo-monitor.sh installed to run this script.
# Usage:
# sudo sh dingo-monitor.sh start
# sudo sh dingo-monitor.sh stop
# sudo sh dingo-monitor.sh restart

change_permission() {
    echo "change prometheus directory permission"
    mkdir -p prometheus/data
    chmod -R 777 prometheus
    if [ $? -ne 0 ]; then
        echo "Error: Failed to execute 'chmod -R 777 prometheus'"
        exit 1
    fi
    echo "change grafana directory permission"
    mkdir -p grafana/data
    chmod -R 777 grafana
    if [ $? -ne 0 ]; then
        echo "Error: Failed to execute 'chmod -R 777 grafana'"
        exit 1
    fi
}

start() {
    change_permission
    echo "starting monitor system..."
    docker-compose up -d
    if [ $? -eq 0 ]; then
        echo "start monitor system success"
    else
        echo "start monitor system failed"
        exit 1
    fi
}

stop() {
    echo "stopping monitor system..."
    docker-compose down
    if [ $? -eq 0 ]; then
        echo "stop monitor system success"
    else
        echo "stop monitor system failed"
        exit 1
    fi
    ID=`(ps -ef | grep "target_json.py"| grep -v "grep") | awk '{print $2}'`
    for id in $ID
    do
        if  kill -9 $id ; then
            echo "killed $id success"
        else
            echo "killed $id failed"
            exit 1
        fi
    done
}

restart() {
    stop
    sleep 3
    start
}

WORKDIR=`pwd`
echo "monitor work dirctory is: ${WORKDIR}"

if [ ! -d ${WORKDIR} ]; then
  echo "${WORKDIR} does not exists"
  exit 1
fi

cd ${WORKDIR}

case "$1" in
    'start')
        start
        ;;
    'stop')
        stop
        ;;
    'restart')
        restart
        ;;
    *)
    echo "usage: $0 {start|stop|restart}"
    exit 1
        ;;
esac   
