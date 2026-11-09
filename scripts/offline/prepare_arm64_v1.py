"""Prepare ARM64 Python 3.7 offline build wheels on a connected machine."""

import argparse
from pathlib import Path
import tarfile
import tempfile
import tomllib

from prepare_arm64_v2 import CONFIG, ROOT, prepare


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-url", default="https://pypi.org/simple")
    parser.add_argument("--upgrade", action="store_true")
    args = parser.parse_args()
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    output = ROOT / ".offline"
    output.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="prepare-v1-", dir=output) as tmp:
        tmp = Path(tmp)
        bundle = tmp / "arm64-v1"
        bundle.mkdir()
        source = tmp / "build.in"
        source.write_text("\n".join(project["build-system"]["requires"]) + "\n")
        prepare("host-v1-cp311-aarch64", "3.11", CONFIG / "host-v1-cp311.in", bundle, args.index_url, args.upgrade)
        prepare("v1-cp37-aarch64", "3.7", source, bundle, args.index_url, args.upgrade)
        archive = output / "arm64-v1.tar.gz"
        with tarfile.open(archive, "w:gz") as packaged:
            packaged.add(bundle, arcname="arm64-v1")
        print(f"Prepared {archive}")


if __name__ == "__main__":
    main()
