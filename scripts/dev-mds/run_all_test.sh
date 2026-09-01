#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags



DEFINE_string type 'all' 'test type'
DEFINE_string mds_addr '' 'mds address'
DEFINE_string mountpoint '' 'mount point'


# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"


if [ -z "${FLAGS_type}" ]; then
    echo "type is empty"
    exit -1
fi

if [ -z "${FLAGS_mountpoint}" ]; then
    echo "mountpoint is empty"
    exit -1
fi



BASE_DIR=$(dirname $(dirname $(cd $(dirname $0); pwd)))
MOUNTPOINT=${FLAGS_mountpoint}
SUFFIX=$(date +%Y%m%d%H%M%S)

function run_e2e_test() {
  echo "### [e2e] run test......"


  E2E_DIR=$BASE_DIR/test/e2e
  TEST_ROOT_DIR=$MOUNTPOINT/e2e_${SUFFIX}
  E2E_LOG_DIR=/tmp/dev-regression-test/e2e_test_${SUFFIX}

  # create test directory and log directory
  mkdir -p $TEST_ROOT_DIR
  mkdir -p $E2E_LOG_DIR

  cd $E2E_DIR
  uv sync

  # run test command
  uv run pytest --mount-point=$TEST_ROOT_DIR > $E2E_LOG_DIR/e2e_test.log 2>&1

  # uv run pytest quota --mount-point=$TEST_ROOT_DIR -m slow  --mds-addr=${FLAGS_mds_addr} --fs-id=10000 --root-ino=1

  echo "### [e2e] test done, log file: $E2E_LOG_DIR/e2e_test.log"
}


function run_pjdtest_test() {
  echo "### [pjdtest] run test......"

  # env information
  PJD_DIR=/home/dengzihui/work/dingofs-test/pjdfstest/tests
  PJD_TEST_DIR=$MOUNTPOINT/pjd_test_${SUFFIX}
  PJD_LOG_DIR=/tmp/dev-regression-test/pjd_test_${SUFFIX}

  # create test directory and log directory
  mkdir -p ${PJD_TEST_DIR}
  mkdir -p ${PJD_LOG_DIR}

  cd ${PJD_TEST_DIR} 

  # run test command
  sudo prove -rv --exec 'bash -x' ${PJD_DIR} > $PJD_LOG_DIR/pjd_test.log 2>&1

  echo "### [pjdtest] test done, log file: $PJD_LOG_DIR/pjd_test.log"
}



function run_fsx_test() {
  echo "### [fsx] run test......"

  # env information
  FSX_TEST_FILE=$MOUNTPOINT/fsx_test_${SUFFIX}
  FSX_LOG_DIR=/tmp/dev-regression-test/fsx_test_${SUFFIX}

  # create test directory and log directory
  mkdir -p ${FSX_TEST_FILE}
  mkdir -p ${FSX_LOG_DIR}

  # run test command
  fsx -l 1073741824 -o 1048576 -S 0 -p 10000 --duration=3600 --record-ops=$FSX_LOG_DIR/fsx.ops -P $FSX_LOG_DIR $FSX_TEST_FILE

  echo "### [fsx] test done, log file: $FSX_LOG_DIR/fsx.ops"
}


function run_mdtest_test() {
  echo "### [mdtest] run test......"

  # env information
  MDTEST_TEST_DIR=$MOUNTPOINT/mdtest_test_${SUFFIX}
  MDTEST_LOG_DIR=/tmp/dev-regression-test/mdtest_test_${SUFFIX}

  # create test directory and log directory
  mkdir -p ${MDTEST_TEST_DIR}
  mkdir -p ${MDTEST_LOG_DIR}

  # run test command
  mpirun -np 4 mdtest -z 2 -b 4 -n 1000 -L -d ${MDTEST_TEST_DIR} > ${MDTEST_LOG_DIR}/mdtest.log 2>&1

  echo "### [mdtest] test done, log file: $MDTEST_LOG_DIR/mdtest.log"
}


function run_fio_test() {
  echo "### [fio] run test......"

  # env information
  FIO_TEST_DIR=$MOUNTPOINT/fio_test_${SUFFIX}
  FIO_LOG_DIR=/tmp/dev-regression-test/fio_test_${SUFFIX}


  # create test directory
  mkdir -p ${FIO_TEST_DIR}
  mkdir -p ${FIO_LOG_DIR}

  # change to test directory
  cd ${FIO_TEST_DIR}

  # run test command
  echo "#### running fio write test..."
  fio --ioengine=libaio --iodepth=1 --direct=1 --rw=write --bs=128KB --size=512MB --numjobs=8 --group_reporting --name=test > ${FIO_LOG_DIR}/fio.log 2>&1
  echo "#### running fio read test..."
  fio --ioengine=libaio --iodepth=1 --direct=1 --rw=read --bs=128KB --size=512MB --numjobs=8 --group_reporting --name=test >> ${FIO_LOG_DIR}/fio.log 2>&1
  echo "#### running fio randread test..."
  fio --ioengine=libaio --iodepth=1 --direct=1 --rw=randread --bs=128KB --size=512MB --numjobs=8 --group_reporting --name=test >> ${FIO_LOG_DIR}/fio.log 2>&1
  echo "#### running fio randwrite test..."
  fio --ioengine=libaio --iodepth=1 --direct=1 --rw=randwrite --bs=128KB --size=512MB --numjobs=8 --group_reporting --name=test >> ${FIO_LOG_DIR}/fio.log 2>&1
  echo "#### running fio randrw test..."
  fio --ioengine=libaio --iodepth=1 --direct=1 --rw=randrw --bs=128KB --size=512MB --numjobs=8 --group_reporting --name=test >> ${FIO_LOG_DIR}/fio.log 2>&1

  echo "### [fio] test done, log file: $FIO_LOG_DIR/fio.log"
}

function run_fsstress_test() {
  echo "### [fsstress] run test......"

  # env information
  FSSTRESS_TEST_DIR=$MOUNTPOINT/fsstress_test_${SUFFIX}
  FSSTRESS_LOG_DIR=/tmp/dev-regression-test/fsstress_test_${SUFFIX}

  # create test directory and log directory
  mkdir -p ${FSSTRESS_TEST_DIR}
  mkdir -p ${FSSTRESS_LOG_DIR}

  # change to test directory
  cd ${FSSTRESS_TEST_DIR}

  # run test command
  /opt/ltp/testcases/bin/fsstress -d ${FSSTRESS_TEST_DIR} -n 10000 -p 8 -v > ${FSSTRESS_LOG_DIR}/fsstress.log 2>&1

  echo "### [fsstress] test done, log file: $FSSTRESS_LOG_DIR/fsstress.log"
}


function run_all_tests() {
  run_e2e_test
  run_pjdtest_test
  run_fsx_test
  run_mdtest_test
  run_fio_test
  run_fsstress_test
}

if [ "$FLAGS_type" = "all" ]; then
  run_all_tests
elif [ "$FLAGS_type" = "e2e" ]; then
  run_e2e_test
elif [ "$FLAGS_type" = "pjdtest" ]; then
  run_pjdtest_test
elif [ "$FLAGS_type" = "fsx" ]; then
  run_fsx_test
elif [ "$FLAGS_type" = "mdtest" ]; then
  run_mdtest_test
elif [ "$FLAGS_type" = "fio" ]; then
  run_fio_test
elif [ "$FLAGS_type" = "fsstress" ]; then
  run_fsstress_test
fi