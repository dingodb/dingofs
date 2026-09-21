#!/bin/bash

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags



DEFINE_string type 'all' 'test type'
DEFINE_string mds_addr '' 'mds address'
DEFINE_string mountpoint '' 'mount point'
DEFINE_integer round 1 'test round count'
DEFINE_string cases '' 'xfstests cases to run, comma or space separated (default: all in xfstests/supported)'
DEFINE_boolean clean_log false 'remove all logs under /tmp/dev-regression-test before running'


# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"


if [ -z "${FLAGS_type}" ]; then
    echo "type is empty"
    exit -1
fi

# xfstests brings up its own mounts, so it does not use --mountpoint
if [ "${FLAGS_type}" != "xfstests" ] && [ -z "${FLAGS_mountpoint}" ]; then
    echo "mountpoint is empty"
    exit -1
fi



BASE_DIR=$(dirname $(dirname $(cd $(dirname $0); pwd)))
MOUNTPOINT=${FLAGS_mountpoint}
LOG_ROOT_DIR=/tmp/dev-regression-test

if [ "${FLAGS_clean_log}" -eq "${FLAGS_TRUE}" ]; then
    echo "### cleaning ${LOG_ROOT_DIR} ......"
    rm -rf "${LOG_ROOT_DIR:?}"
fi

function run_e2e_test() {
  echo "### [e2e] run test......"


  E2E_DIR=$BASE_DIR/test/e2e
  TEST_ROOT_DIR=$MOUNTPOINT/e2e_${SUFFIX}
  E2E_LOG_DIR=$LOG_ROOT_DIR/e2e_test_${SUFFIX}

  # create test directory and log directory
  mkdir -p $TEST_ROOT_DIR
  mkdir -p $E2E_LOG_DIR

  cd $E2E_DIR
  uv sync

  # run test command
  uv run pytest --mount-point=$TEST_ROOT_DIR > $E2E_LOG_DIR/e2e_test.log 2>&1

  # verify result
  if grep -qE '[0-9]+ passed' $E2E_LOG_DIR/e2e_test.log &&
     ! grep -qE '[0-9]+ (failed|error)' $E2E_LOG_DIR/e2e_test.log; then
    echo "### [e2e] result: PASS"
  else
    echo "### [e2e] result: FAIL"
    FAILED=1
  fi

  # uv run pytest quota --mount-point=$TEST_ROOT_DIR -m slow  --mds-addr=${FLAGS_mds_addr} --fs-id=10000 --root-ino=1

  echo "### [e2e] test done, log file: $E2E_LOG_DIR/e2e_test.log"
}


function run_pjdtest_test() {
  echo "### [pjdtest] run test......"

  # env information
  PJD_DIR=/home/dengzihui/work/dingofs-test/pjdfstest/tests
  PJD_TEST_DIR=$MOUNTPOINT/pjd_test_${SUFFIX}
  PJD_LOG_DIR=$LOG_ROOT_DIR/pjd_test_${SUFFIX}

  # create test directory and log directory
  mkdir -p ${PJD_TEST_DIR}
  mkdir -p ${PJD_LOG_DIR}

  cd ${PJD_TEST_DIR} 

  # run test command
  sudo prove -rv --exec 'bash -x' ${PJD_DIR} > $PJD_LOG_DIR/pjd_test.log 2>&1

  # verify result
  if grep -q '^Result: PASS' $PJD_LOG_DIR/pjd_test.log; then
    echo "### [pjdtest] result: PASS"
  else
    echo "### [pjdtest] result: FAIL"
    FAILED=1
  fi

  echo "### [pjdtest] test done, log file: $PJD_LOG_DIR/pjd_test.log"
}



function run_fsx_test() {
  echo "### [fsx] run test......"

  # env information
  FSX_TEST_FILE=$MOUNTPOINT/fsx_test_${SUFFIX}
  FSX_LOG_DIR=$LOG_ROOT_DIR/fsx_test_${SUFFIX}

  # create test directory and log directory
  mkdir -p ${FSX_LOG_DIR}

  # run test command
  fsx -l 1073741824 -o 1048576 -S 0 -p 10000 --duration=3600 --record-ops=$FSX_LOG_DIR/fsx.ops -P $FSX_LOG_DIR $FSX_TEST_FILE > $FSX_LOG_DIR/fsx.log 2>&1

  # verify result
  if grep -qE '^All [0-9]+ operations completed A-OK' $FSX_LOG_DIR/fsx.log; then
    echo "### [fsx] result: PASS"
  else
    echo "### [fsx] result: FAIL"
    FAILED=1
  fi

  echo "### [fsx] test done, log file: $FSX_LOG_DIR/fsx.ops"
}


function run_mdtest_test() {
  echo "### [mdtest] run test......"

  # env information
  MDTEST_TEST_DIR=$MOUNTPOINT/mdtest_test_${SUFFIX}
  MDTEST_LOG_DIR=$LOG_ROOT_DIR/mdtest_test_${SUFFIX}

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
  FIO_LOG_DIR=$LOG_ROOT_DIR/fio_test_${SUFFIX}


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
  FSSTRESS_LOG_DIR=$LOG_ROOT_DIR/fsstress_test_${SUFFIX}

  # create test directory and log directory
  mkdir -p ${FSSTRESS_TEST_DIR}
  mkdir -p ${FSSTRESS_LOG_DIR}

  # change to test directory
  cd ${FSSTRESS_TEST_DIR}

  # run test command
  /opt/ltp/testcases/bin/fsstress -d ${FSSTRESS_TEST_DIR} -n 10000 -p 8 -v > ${FSSTRESS_LOG_DIR}/fsstress.log 2>&1

  echo "### [fsstress] test done, log file: $FSSTRESS_LOG_DIR/fsstress.log"
}


function run_vdbench_test() {
  echo "### [vdbench] run test......"

  # env information
  VDB_DIR=/home/dengzihui/work/dingofs-test/vdbench
  VDB_TEST_DIR=$MOUNTPOINT/vdbench_test_${SUFFIX}
  VDB_LOG_DIR=$LOG_ROOT_DIR/vdbench_test_${SUFFIX}
  VDB_PARAM_FILE=$VDB_LOG_DIR/vdbench.params

  # pre-check: vdbench and java are installed out of band, this script only runs them
  if [ ! -x "${VDB_DIR}/vdbench" ]; then
    echo "### [vdbench] result: FAIL (${VDB_DIR}/vdbench not found)"
    FAILED=1
    return
  fi
  if ! command -v java >/dev/null; then
    echo "### [vdbench] result: FAIL (java not found)"
    FAILED=1
    return
  fi

  # create test directory and log directory
  mkdir -p ${VDB_TEST_DIR}
  mkdir -p ${VDB_LOG_DIR}

  # generate parameter file bound to this round's test directory
  cat > ${VDB_PARAM_FILE} <<EOF
validate=yes
data_errors=1

fsd=fsd1,anchor=${VDB_TEST_DIR},depth=1,width=10,files=10,sizes=(100m,0),openflags=o_direct

fwd=fwd1,fsd=fsd1,threads=10,rdpct=50,xfersize=(512,20,4k,20,64k,20,512k,20,1024k,20),fileio=random,fileselect=random

rd=rd1,fwd=fwd*,fwdrate=max,format=yes,elapsed=300,interval=1
EOF

  # run test command
  cd ${VDB_TEST_DIR}
  ${VDB_DIR}/vdbench -f ${VDB_PARAM_FILE} -o ${VDB_LOG_DIR}/output > ${VDB_LOG_DIR}/vdbench.log 2>&1
  VDB_RC=$?

  # verify result: clean exit, run completed, and no data validation / I/O errors
  VDB_ERRORS=$(grep -hE 'Data Validation error for|Vdbench terminating due to Data Validation|marked in error: *[1-9]' \
    ${VDB_LOG_DIR}/vdbench.log ${VDB_LOG_DIR}/output/errorlog.html 2>/dev/null)
  if [ ${VDB_RC} -eq 0 ] &&
     grep -q 'Vdbench execution completed successfully' ${VDB_LOG_DIR}/vdbench.log &&
     [ -z "${VDB_ERRORS}" ]; then
    echo "### [vdbench] result: PASS"
  else
    echo "### [vdbench] result: FAIL"
    [ -n "${VDB_ERRORS}" ] && echo "${VDB_ERRORS}"
    FAILED=1
  fi

  echo "### [vdbench] test done, log file: $VDB_LOG_DIR/vdbench.log"
}


function run_xfstests_test() {
  echo "### [xfstests] run test......"

  # env information
  XFS_DIR=/home/dengzihui/work/dingofs-test/xfstests-dev
  XFS_CASE_FILE=$BASE_DIR/xfstests/supported
  XFS_LOG_DIR=$LOG_ROOT_DIR/xfstests_test_${SUFFIX}

  # resolve the case list: explicit --cases wins, else the supported file
  if [ -n "${FLAGS_cases}" ]; then
    XFS_CASES=$(echo "${FLAGS_cases}" | tr ',' ' ')
    XFS_CASE_FILE=
  else
    XFS_CASES=$(grep -vE '^[[:space:]]*(#|$)' ${XFS_CASE_FILE})
  fi
  if [ -z "${XFS_CASES}" ]; then
    echo "### [xfstests] result: FAIL (no cases to run)"
    FAILED=1
    return
  fi
  echo "### [xfstests] cases: ${XFS_CASES}"

  # pre-check: the xfstests adapter is installed out of band, this script only runs it
  for f in ${XFS_DIR} ${XFS_CASE_FILE} /sbin/mount.fuse.dingofs /etc/dingofs-xfstests.conf; do
    if [ ! -e "${f}" ]; then
      echo "### [xfstests] result: FAIL (${f} not found)"
      echo "### [xfstests] run 'bash ${BASE_DIR}/xfstests/setup.sh ${XFS_DIR}' first"
      FAILED=1
      return
    fi
  done
  if ! command -v sudo >/dev/null; then
    echo "### [xfstests] result: FAIL (sudo not found)"
    FAILED=1
    return
  fi

  # create log directory
  mkdir -p ${XFS_LOG_DIR}

  # drop stale mounts/clients; in MDS mode remote fs contents are kept
  if ! bash ${BASE_DIR}/xfstests/reset.sh; then
    echo "### [xfstests] result: FAIL (reset.sh failed)"
    FAILED=1
    return
  fi
  echo "### [xfstests] note: reset.sh does not clear remote fs contents in MDS mode"

  # run test command
  cd ${XFS_DIR}
  sudo env RESULT_BASE=${XFS_LOG_DIR}/results \
    ./check ${XFS_CASES} \
    > ${XFS_LOG_DIR}/xfstests.log 2>&1

  # ./check runs as root, hand the results back so they can be inspected/cleaned as the user
  sudo chown -R $(id -u):$(id -g) ${XFS_LOG_DIR}

  # verify result: at least one case really ran, and none failed
  if grep -qE '^Passed all ([1-9][0-9]*) tests' ${XFS_LOG_DIR}/xfstests.log &&
     ! grep -qE '^Failures:' ${XFS_LOG_DIR}/xfstests.log; then
    echo "### [xfstests] result: PASS"
  else
    echo "### [xfstests] result: FAIL"
    grep -E '^(Failures|Not run):' ${XFS_LOG_DIR}/xfstests.log
    echo "### [xfstests] see ${XFS_LOG_DIR}/results/ for .out.bad"
    FAILED=1
  fi

  echo "### [xfstests] test done, log file: ${XFS_LOG_DIR}/xfstests.log"
}


function run_all_tests() {
  run_e2e_test
  run_pjdtest_test
  run_fsx_test
  run_mdtest_test
  # run_fio_test
  run_fsstress_test
  run_vdbench_test
}

FAILED=0

for ((i = 1; i <= ${FLAGS_round}; i++)); do
  echo "### ===== round $i/${FLAGS_round} ====="
  SUFFIX=$(date +%Y%m%d%H%M%S)_${i}

  if [ "$FLAGS_type" == "all" ]; then
    run_all_tests
  elif [ "$FLAGS_type" == "e2e" ]; then
    run_e2e_test
  elif [ "$FLAGS_type" == "pjdtest" ]; then
    run_pjdtest_test
  elif [ "$FLAGS_type" == "fsx" ]; then
    run_fsx_test
  elif [ "$FLAGS_type" == "mdtest" ]; then
    run_mdtest_test
  elif [ "$FLAGS_type" == "fio" ]; then
    run_fio_test
  elif [ "$FLAGS_type" == "fsstress" ]; then
    run_fsstress_test
  elif [ "$FLAGS_type" == "vdbench" ]; then
    run_vdbench_test
  elif [ "$FLAGS_type" == "xfstests" ]; then
    run_xfstests_test
  fi

  sleep 10
done

if [ ${FAILED} -ne 0 ]; then
  echo "### some tests FAILED"
  exit 1
fi