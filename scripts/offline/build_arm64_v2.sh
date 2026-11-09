#!/usr/bin/env bash
set -euo pipefail

: "${CI_CUS_LINUX_ARM_PYTHON_PATH_V2:?Set the CPython 3.11 executable for this runner}"
: "${DOCKER_TAG_V2:?Set the preloaded manylinux image tag}"
: "${DDB_ARM64_V2_OFFLINE_DIR:?Set the extracted offline bundle directory}"

offline_dir=$(realpath "$DDB_ARM64_V2_OFFLINE_DIR")
image="manylinux2014_aarch64:$DOCKER_TAG_V2"
for group in host-v2-cp311-aarch64 v2-cp{38,39,310,311,312,313,313t}-aarch64; do
    cmp "scripts/offline/$group.txt" "$offline_dir/$group/requirements.txt"
done
"$CI_CUS_LINUX_ARM_PYTHON_PATH_V2" -c 'import sys; assert sys.version_info[:2] == (3, 11), "Offline host wheels require Python 3.11"'
test "$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image")" = linux/arm64

job_dir=$(mktemp -d)
trap 'rm -rf "$job_dir"' EXIT
"$CI_CUS_LINUX_ARM_PYTHON_PATH_V2" -m venv "$job_dir/venv"
python="$job_dir/venv/bin/python"
"$python" -m pip --isolated install --no-index --only-binary=:all: \
    --find-links "$offline_dir/host-v2-cp311-aarch64" \
    --requirement "$offline_dir/host-v2-cp311-aarch64/requirements.txt"

export CIBW_MANYLINUX_AARCH64_IMAGE="$image"
export CIBW_ARCHS_LINUX=aarch64
export CIBW_BEFORE_BUILD_LINUX='python --version'
export CIBW_ENVIRONMENT_LINUX='PIP_NO_INDEX=1 PIP_FIND_LINKS=/opt/ddb-offline-wheels PIP_ONLY_BINARY=:all: PIP_DISABLE_PIP_VERSION_CHECK=1'
for version in 38 39 310 311 312 313 313t; do
    export CIBW_BUILD="cp${version}-manylinux_aarch64"
    export CIBW_CONTAINER_ENGINE="docker; create_args: --network=none --volume='$offline_dir/v2-cp${version}-aarch64:/opt/ddb-offline-wheels:ro'"
    "$python" -m cibuildwheel --platform linux --output-dir wheelhouse
done
