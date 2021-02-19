#!/usr/bin/env bash

set -euo pipefail


# You can change these constants.
PYTHON_BIN=python3
VIRTUALENV=.venv
rm -rf ${VIRTUALENV}
PIP="${VIRTUALENV}/bin/pip"
REQUIREMENTS_FILE=requirements.txt
"${PYTHON_BIN}" -m venv "${VIRTUALENV}"


"${PIP}" install pip --upgrade
# "${PIP}" install -r "${REQUIREMENTS_FILE}" 
cat $(out_location //src/scripts/python:venv_requirements)