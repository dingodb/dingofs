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

# pjd test
function pjd_test() {
  PJD_TEST_DIR=${FLAGS_mountpoint}/pjd_test_$dir_suffix
  LOG_DIR=/tmp/log/pjd_test_$dir_suffix
  mkdir -p ${PJD_TEST_DIR}
  mkdir -p ${LOG_DIR}

  # check mkdir result
  if [ ! -d "${PJD_TEST_DIR}" ]; then
    echo "Failed to create directory ${PJD_TEST_DIR}"
    exit 1
  fi

  podman run --rm -v ${PJD_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools  -t pjdtest -s pjdtest -m /data -o /output

  echo "pjd test completed, logs are stored in ${LOG_DIR}"
}

# mdtest test
function mdtest_test() {
  MDTEST_TEST_DIR=${FLAGS_mountpoint}/mdtest_test_$dir_suffix
  LOG_DIR=/tmp/log/mdtest_test_$dir_suffix
  mkdir -p ${MDTEST_TEST_DIR}

  # check mkdir result
  if [ ! -d "${MDTEST_TEST_DIR}" ]; then
    echo "Failed to create directory ${MDTEST_TEST_DIR}"
    exit 1
  fi

  mkdir -p ${LOG_DIR}/mdtest_z0_n100
  podman run --rm -v ${MDTEST_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools -t mdtest -s mdtest_z0_n100 -m /data -o /output

  mkdir -p ${LOG_DIR}/mdtest_z5_b4_i1
  podman run --rm -v ${MDTEST_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools -t mdtest -s mdtest_z5_b4_i1 -m /data -o /output

  mkdir -p ${LOG_DIR}/mdtest_z6_b3_i1
  podman run --rm -v ${MDTEST_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools -t mdtest -s mdtest_z6_b3_i1 -m /data -o /output

  mkdir -p ${LOG_DIR}/mdtest_z9_b2_i1
  podman run --rm -v ${MDTEST_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools -t mdtest -s mdtest_z9_b2_i1 -m /data -o /output

  echo "mdtest test completed, logs are stored in ${LOG_DIR}"
}


# vdbench test
function vdbench_test() {
  VDBENCH_TEST_DIR=${FLAGS_mountpoint}/vdbench_test_$dir_suffix
  LOG_DIR=/tmp/log/vdbench_test_$dir_suffix
  mkdir -p ${VDBENCH_TEST_DIR}

  # check mkdir result
  if [ ! -d "${VDBENCH_TEST_DIR}" ]; then
    echo "Failed to create directory ${VDBENCH_TEST_DIR}"
    exit 1
  fi

  mkdir -p ${LOG_DIR}/seq_wr
  podman run --rm -v ${VDBENCH_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools -t vdbench -s seq_wr -m /data -o /output

  mkdir -p ${LOG_DIR}/seq_rd
  podman run --rm -v ${VDBENCH_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools -t vdbench -s seq_rd -m /data -o /output

  mkdir -p ${LOG_DIR}/rand_wr
  podman run --rm -v ${VDBENCH_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools -t vdbench -s rand_wr -m /data -o /output

  mkdir -p ${LOG_DIR}/rand_rd
  podman run --rm -v ${VDBENCH_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools -t vdbench -s rand_rd -m /data -o /output


  echo "vdbench test completed, logs are stored in ${LOG_DIR}"
}


# fio test
function fio_test() {
  FIO_TEST_DIR=${FLAGS_mountpoint}/fio_test_$dir_suffix
  LOG_DIR=/tmp/log/fio_test_$dir_suffix
  mkdir -p ${FIO_TEST_DIR}

  # check mkdir result
  if [ ! -d "${FIO_TEST_DIR}" ]; then
    echo "Failed to create directory ${FIO_TEST_DIR}"
    exit 1
  fi

  mkdir -p ${LOG_DIR}/rand_read_0d_128k_16j
  podman run --rm -v ${FIO_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools -t fio -s rand_read_0d_128k_16j -m /data -o /output

  mkdir -p ${LOG_DIR}/rand_read_0d_128k_1j
  podman run --rm -v ${FIO_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools -t fio -s rand_read_0d_128k_1j -m /data -o /output

  mkdir -p ${LOG_DIR}/rand_write_0d_128k_16j
  podman run --rm -v ${FIO_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools -t fio -s rand_write_0d_128k_16j -m /data -o /output

  mkdir -p ${LOG_DIR}/rand_write_0d_1m_16j
  podman run --rm -v ${FIO_TEST_DIR}:/data -v ${LOG_DIR}:/output dingofs-benchmark-tools -t fio -s rand_write_0d_1m_16j -m /data -o /output

  echo "fio test completed, logs are stored in ${LOG_DIR}"
}

pjd_test

sleep 5
mdtest_test

sleep 5
vdbench_test

sleep 5
fio_test