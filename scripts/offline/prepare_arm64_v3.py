"""Resolve and download ARM64 wheels without importing or compiling the SDK.

Requires Python >= 3.11, pip, and uv on the connected preparation machine.
"""

import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib


ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "scripts" / "offline"


def run(*args: str) -> None:
    print("+", " ".join(args), flush=True)
    subprocess.run(args, check=True, cwd=ROOT)


def prepare_group(
    name: str,
    python_version: str,
    source: Path,
    bundle: Path,
    index_url: str,
    upgrade: bool,
) -> None:
    lock = CONFIG / f"{name}.txt"
    resolve = [
        "uv", "pip", "compile", str(source),
        "--python-version", python_version,
        "--python-platform", "aarch64-manylinux_2_28",
        "--only-binary", ":all:",
        "--no-python-downloads", "--no-header", "--no-annotate",
        "--default-index", index_url,
        "--output-file", str(lock),
    ]
    if upgrade:
        resolve.append("--upgrade")
    if python_version == "3.14":
        # cibuildwheel applies these constraints again when installing virtualenv
        # for wheel tests. Its transitive pins must also be available offline.
        resolve.extend(["--constraint", str(CONFIG / "cibuildwheel-4.2.0-cp314-constraints.txt")])
    run(*resolve)

    # A fresh bundle must not retain obsolete wheels from an earlier resolution.
    wheel_dir = bundle / name
    wheel_dir.mkdir()
    download = [
        sys.executable, "-m", "pip", "--isolated", "download",
        "--index-url", index_url,
        "--dest", str(wheel_dir), "--only-binary=:all:", "--no-deps",
        "--python-version", python_version, "--implementation", "cp",
        "--abi", "cp" + python_version.replace(".", ""),
        "--abi", "abi3", "--abi", "none",
        "--requirement", str(lock),
    ]
    # Include every compatible glibc baseline, not just the image's upper bound.
    for glibc_minor in range(28, 16, -1):
        download.extend(["--platform", f"manylinux_2_{glibc_minor}_aarch64"])
    download.extend(["--platform", "manylinux2014_aarch64"])
    run(*download)
    shutil.copyfile(lock, wheel_dir / "requirements.txt")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-url", default="https://pypi.org/simple")
    parser.add_argument("--upgrade", action="store_true",
                        help="Explicitly update versions already pinned in the lock files")
    args = parser.parse_args()
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    output = ROOT / ".offline"
    output.mkdir(exist_ok=True)

    # Resolve with the target Python/platform markers via uv. pip download then
    # fetches that complete pinned list, without re-resolving on the local CPU.
    with tempfile.TemporaryDirectory(prefix="prepare-", dir=output) as temporary:
        temporary = Path(temporary)
        target_input = temporary / "cp314.in"
        target_input.write_text("\n".join([
            *project["build-system"]["requires"],
            *project["project"]["dependencies"],
            (CONFIG / "cp314-tools.in").read_text(),
        ]) + "\n")
        bundle = temporary / "arm64-v3"
        bundle.mkdir()
        prepare_group("host-cp311-aarch64", "3.11", CONFIG / "host-cp311.in",
                      bundle, args.index_url, args.upgrade)
        prepare_group("cp314-aarch64", "3.14", target_input,
                      bundle, args.index_url, args.upgrade)
        shutil.copyfile(CONFIG / "README.md", bundle / "README.md")

        archive = output / "arm64-v3.tar.gz"
        with tarfile.open(archive, "w:gz") as packaged:
            packaged.add(bundle, arcname="arm64-v3")
        print(f"Prepared {archive} ({archive.stat().st_size / 1024**2:.1f} MiB)")


if __name__ == "__main__":
    main()
