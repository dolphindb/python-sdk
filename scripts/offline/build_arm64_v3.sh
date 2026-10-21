#!/usr/bin/env bash
# Run from the repository root on the Linux ARM64 shell runner.
set -euo pipefail

: "${CI_CUS_LINUX_ARM_PYTHON_PATH_V2:?Set the CPython 3.11 executable for this runner}"
: "${DOCKER_TAG_V3:?Set the preloaded manylinux image tag}"
: "${DDB_ARM64_V3_OFFLINE_DIR:?Set the extracted offline bundle directory}"

offline_dir=$(realpath "$DDB_ARM64_V3_OFFLINE_DIR")
image="manylinux_2_28_aarch64:$DOCKER_TAG_V3"

for group in host-cp311-aarch64 cp314-aarch64; do
    # Compare version lists so an outdated uploaded bundle fails before building.
    cmp "scripts/offline/$group.txt" "$offline_dir/$group/requirements.txt"
done

"$CI_CUS_LINUX_ARM_PYTHON_PATH_V2" -c \
    'import sys; assert sys.version_info[:2] == (3, 11), "Offline host wheels require Python 3.11"'
test "$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image")" = linux/arm64

# Each job gets its own tools; do not mutate the runner's shared Conda environment.
job_dir=$(mktemp -d)
trap 'rm -rf "$job_dir"' EXIT
"$CI_CUS_LINUX_ARM_PYTHON_PATH_V2" -m venv "$job_dir/venv"
python="$job_dir/venv/bin/python"
"$python" -m pip --isolated install --no-index --only-binary=:all: \
    --find-links "$offline_dir/host-cp311-aarch64" \
    --requirement "$offline_dir/host-cp311-aarch64/requirements.txt"

export CIBW_MANYLINUX_AARCH64_IMAGE="$image"
export CIBW_BUILD=cp314-manylinux_aarch64
export CIBW_ARCHS_LINUX=aarch64
export CIBW_CONTAINER_ENGINE="docker; create_args: --network=none --volume='$offline_dir/cp314-aarch64:/opt/ddb-offline-wheels:ro'"
export CIBW_ENVIRONMENT_LINUX="PIP_NO_INDEX=1 PIP_FIND_LINKS=/opt/ddb-offline-wheels PIP_ONLY_BINARY=:all: PIP_DISABLE_PIP_VERSION_CHECK=1"
# Override the online pip configuration/upgrade in pyproject.toml for this job.
export CIBW_BEFORE_BUILD_LINUX="python --version && python -m pip install --requirement /opt/ddb-offline-wheels/requirements.txt"
export CIBW_BUILD_FRONTEND=build
export CIBW_TEST_COMMAND="python -c 'import dolphindb; print(dolphindb.__version__)'"

"$python" -m cibuildwheel --platform linux --output-dir wheelhouse
