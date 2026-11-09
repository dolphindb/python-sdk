"""Prepare offline ARM64 build dependencies on a connected machine (Python >= 3.11)."""

import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "scripts/offline"
VERSIONS = ("3.8", "3.9", "3.10", "3.11", "3.12", "3.13")


def run(args):
    print("+", " ".join(map(str, args)), flush=True)
    subprocess.run(args, check=True, cwd=ROOT)


def prepare(name, version, source, bundle, index, upgrade, free_threaded=False):
    lock = CONFIG / f"{name}.txt"
    command = ["uv", "pip", "compile", str(source), "--python-version", version,
               "--python-platform", "aarch64-manylinux2014", "--only-binary", ":all:",
               "--no-python-downloads", "--no-header", "--no-annotate",
               "--default-index", index, "--output-file", str(lock)]
    if upgrade:
        command.append("--upgrade")
    run(command)
    destination = bundle / name
    destination.mkdir()
    run([sys.executable, "-m", "pip", "--isolated", "download", "--index-url", index,
         "--dest", str(destination), "--only-binary=:all:", "--no-deps",
         "--python-version", version, "--implementation", "cp",
         "--abi", "cp" + version.replace(".", "") + ("t" if free_threaded else ""), "--abi", "abi3", "--abi", "none",
         "--platform", "manylinux2014_aarch64", "--platform", "manylinux_2_17_aarch64",
         "--requirement", str(lock)])
    shutil.copyfile(lock, destination / "requirements.txt")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-url", default="https://pypi.org/simple")
    parser.add_argument("--upgrade", action="store_true")
    args = parser.parse_args()
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    output = ROOT / ".offline"
    output.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="prepare-v2-", dir=output) as tmp:
        tmp = Path(tmp)
        bundle = tmp / "arm64-v2"
        bundle.mkdir()
        source = tmp / "build.in"
        source.write_text("\n".join(project["build-system"]["requires"]) + "\n")
        prepare("host-v2-cp311-aarch64", "3.11", CONFIG / "host-v2-cp311.in", bundle, args.index_url, args.upgrade)
        for version in VERSIONS:
            prepare("v2-cp" + version.replace(".", "") + "-aarch64", version, source, bundle, args.index_url, args.upgrade)
        prepare("v2-cp313t-aarch64", "3.13", source, bundle, args.index_url, args.upgrade, free_threaded=True)
        archive = output / "arm64-v2.tar.gz"
        with tarfile.open(archive, "w:gz") as packaged:
            packaged.add(bundle, arcname="arm64-v2")
        print(f"Prepared {archive}")


if __name__ == "__main__":
    main()
