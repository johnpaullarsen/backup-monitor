#!/bin/bash
set -ex

VENV_ROOT=backup_monitor_venv
# Temp directory for pip to not fill /tmp
TMPDIR=${VENV_ROOT}

echo "removing old virtual environment" && rm -rf ${VENV_ROOT}
echo "creating new virtual environment" && python3 -m venv ${VENV_ROOT}

source ${VENV_ROOT}/bin/activate
pip3 install --upgrade pip
pip3 install --upgrade wheel
pip3 install -r src/requirements.txt