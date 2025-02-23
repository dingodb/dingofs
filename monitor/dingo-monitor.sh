#!/bin/sh

WORKDIR=`pwd`
echo "monitor work dirctory is: ${WORKDIR}"

if [ ! -d $WORKDIR ]; then
  echo "${WORKDIR} not exists"
  exit 1
fi

cd $WORKDIR
chmod -R 777 prometheus
chmod -R 777 grafana

start() {
    echo "==========start==========="
    echo "" > monitor.log
    docker-compose up -d >> monitor.log 2>&1 &
    if [ $? -eq 0 ]; then
        echo "start monitor system success!"
    else
        echo "start monitor system failed,please check monitor.log"
    fi
}

stop() {
    echo "===========stop============"
    docker-compose down >> monitor.log 2>&1 &
    if [ $? -eq 0 ]; then
        echo "stop monitor system success!"
    else
        echo "stop monitor system failed,please check monitor.log"
    fi
    ID=`(ps -ef | grep "target_json.py"| grep -v "grep") | awk '{print $2}'`
    for id in $ID
    do
        kill -9 $id
	echo "killed $id"
    done
}

restart() {
    stop
    echo "sleeping........."
    sleep 3
    start
}

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
