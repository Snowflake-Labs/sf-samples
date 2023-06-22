#!/bin/bash

# Setup a conda environemnt & installs snowpark ML.
#
# Usage
# snowpark-ml.sh [-f <conda tar.bz2 file>] [-d <output dir>] [-e <conda env name>] [-p 3.8|3.9|3.10] [-h] 

set -o pipefail
set -eu

PROD=$0

if ! command -v conda &> /dev/null
then
    echo "## conda could not be found. This script is only useful for conda."
    exit 1
fi

CONDA_ENV="default"
TAR_FILE="default"
CHANNEL_HOME="${HOME}/mychannel"
PY_VERSION="3.8"
# Needs to be updated every release. Can be moved to snowml repo once that is open sourced.
DEFAULT_DOWNLOAD_URL="https://drive.google.com/uc?export=download&id=10h3U3fDzXw7C2QYCzhpoNUxry7hzMVf9"
DEFAULT_FILENAME="snowflake-ml-python-1.0.1-py_0.tar.bz2"

function help() {
    exitcode=$1 && shift
    echo "Usage: ${PROG} [-f <conda tar.bz2 file>] [-d <output dir>] [-e <conda env name>] [-p 3.8|3.9|3.10] [-h]"
    echo "  -f CONDA_TARBALL: By default, downloads latest the internet."
    echo "  -d OUTPUT_DIR: Default is ${CHANNEL_HOME}"
    echo "  -p PY_VERSION: Default is 3.8. Options are 3.9, 3.10."
    echo "  -e CONDA_ENV_NAME: Default is conda's default env, usually `base`. If it is an existing env, it will reuse."
    exit ${exitcode}
}

while (($#)); do
    case $1 in
        -f)
            shift
            TAR_FILE=$1
            ;;
        -d)
            shift
            CHANNEL_HOME=$1
            ;;
        -e)
            shift
            CONDA_ENV=$1
            ;;
        -p)
            shift
            if [[ $1 = "3.8" || $1 = "3.9" || $1 == "3.10" ]]; then
                PY_VERSION=$1
            else
                echo "Invalid python version: $1"
                help 1
            fi
            ;;
        -h|--help)
            help 0
            ;;
        *)
            help 1
            ;;
    esac
    shift
done

if [[ "${TAR_FILE}" == "default" ]]; then
    TARGET_FILE="${CHANNEL_HOME}/noarch/${DEFAULT_FILENAME}"
    if [[ -f "${TARGET_FILE}" ]]; then
        echo "## File already exists. Skipping download"
    else
        rm -rf ${CHANNEL_HOME}
        mkdir -p ${CHANNEL_HOME}/noarch

        echo "## Downloading latest package from Google Drive"
        wget "${DEFAULT_DOWNLOAD_URL}" -O "${TARGET_FILE}"
    fi
else
    cp "${TAR_FILE}" "${CHANNEL_HOME}/noarch/"
fi

if ! command -v conda-index &> /dev/null
then
    echo "## Installing conda-build"
    conda install conda-build
fi

echo "## Indexing local channel at ${CHANNEL_HOME}"
conda index ${CHANNEL_HOME}

eval "$(conda shell.bash hook)"

if [[ "${CONDA_ENV}" == "default" ]]; then
    echo "## No conda env specified. Using default. Assuming setup correctly."
    conda activate
else
    if conda env list | grep "${CONDA_ENV}" >/dev/null 2>&1; then
        echo "## Conda env ${CONDA_ENV} exists. Assuming setup correctly."
        conda activate "${CONDA_ENV}"
    else
        echo "## Creating conda env ${CONDA_ENV}"
        if [[ $(uname -m) == 'arm64' ]]; then
            echo "## Mac M1 detected. Following special conda treatment as per https://docs.snowflake.com/en/developer-guide/snowpark/python/setup"
            CONDA_SUBDIR=osx-64 conda create -n {CONDA_ENV} python=${PY_VERSION} numpy pandas --override-channels -c https://repo.anaconda.com/pkgs/snowflake
            conda activate "${CONDA_ENV}"
            conda config --env --set subdir osx-64
        else
            conda create --name "${CONDA_ENV}" --override-channels -c https://repo.anaconda.com/pkgs/snowflake python=${PY_VERSION} numpy pandas
            conda activate "${CONDA_ENV}"
        fi
    fi
fi

echo "## Installing snowpark ML"
conda install -c "file://${CHANNEL_HOME}" -c "https://repo.anaconda.com/pkgs/snowflake/" --override-channel snowflake-ml-python

echo "## ALL DONE. Please activate the env by executing `conda activate ${CONDA_ENV}`"