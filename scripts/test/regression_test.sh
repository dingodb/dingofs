#!/bin/bash


mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string mountpoint '' 'mount point'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"
echo "mountpoint: ${FLAGS_mountpoint}"

if [ -z "${FLAGS_mountpoint}" ]; then
  echo "error: mountpoint is required."
  exit 1
fi

dir_suffix=$(date +%Y%m%d%H%M%S)
TEST_DIR=${FLAGS_mountpoint}/dtt_$dir_suffix
LOG_DIR=/tmp/dtt
RESULT_FILE=${LOG_DIR}/result.log

mkdir -p ${TEST_DIR}
mkdir -p ${LOG_DIR}

# check mkdir result
if [ ! -d "${TEST_DIR}" ]; then
  echo "Failed to create directory ${TEST_DIR}"
  exit 1
fi


# set dtt config
dtt config set testdir ${TEST_DIR}
dtt config set output ${LOG_DIR}
dtt config set runtime podman



# pjd test
function pjd_test() {
  dtt -t pjd -s all


  # check result file
  # search ${RESULT_FILE} for "FAIL" keyword
  cat ${RESULT_FILE}
  if grep -q "Status: FAIL" ${RESULT_FILE}; then
    echo "pjd test failed, please check ${RESULT_FILE} for details."
    exit 1
  fi

  echo "pjd test completed, logs are stored in ${LOG_DIR}"
}

# mdtest test
function mdtest_test() {
  dtt -t mdtest -s all

  # check result file
  # search ${RESULT_FILE} for "FAIL" keyword
  cat ${RESULT_FILE}
  if grep -q "Status: FAIL" ${RESULT_FILE}; then
    echo "mdtest test failed, please check ${RESULT_FILE} for details."
    exit 1
  fi

  echo "mdtest test completed, logs are stored in ${LOG_DIR}"
}


# vdbench test
function vdbench_test() {
  dtt -t vdbench -s all

  # check result file
  # search ${RESULT_FILE} for "FAIL" keyword
  cat ${RESULT_FILE}
  if grep -q "Status: FAIL" ${RESULT_FILE}; then
    echo "vdbench test failed, please check ${RESULT_FILE} for details."
    exit 1
  fi

  echo "vdbench test completed, logs are stored in ${LOG_DIR}"
}


# fio test
function fio_test() {
  dtt -t fio -s all

  # check result file
  # search ${RESULT_FILE} for "FAIL" keyword
  cat ${RESULT_FILE}
  if grep -q "Status: FAIL" ${RESULT_FILE}; then
    echo "fio test failed, please check ${RESULT_FILE} for details."
    exit 1
  fi

  echo "fio test completed, logs are stored in ${LOG_DIR}"
}

pjd_test

sleep 5
mdtest_test

sleep 5
vdbench_test

sleep 5
fio_test