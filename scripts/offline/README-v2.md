# Linux ARM64 v2 离线构建

v2 使用 manylinux2014 镜像构建 CPython 3.8–3.13（包括 3.13 自由线程版本）。构建容器禁用网络，pip 从每个版本独立的只读 wheel 目录安装 PEP 517 构建依赖。保留原有 cibuildwheel 2.22.0 和现有 wheel 修复流程。此任务沿用原有配置，不执行成品导入测试。

在联网机器使用 Python 3.11 以上、pip 和 uv，从仓库根目录执行：

```bash
python3 scripts/offline/prepare_arm64_v2.py
```

输出 `.offline/arm64-v2.tar.gz` 和 `scripts/offline/` 内的版本清单。可使用 `--index-url` 指定准备时的源，或用 `--upgrade` 显式更新清单。清单和脚本须随 CI 变更一起交付；修改 `pyproject.toml` 构建依赖后重新准备。准备阶段仅下载 wheel，不编译 SDK。

将压缩包传到每台实际承载 `Linux-arm64` Runner 的 Docker 宿主机，在稳定目录解压：

```bash
umask 022
mkdir -p /hdd/hdd5/gitlab-runner/offline
tar -xzf /传入位置/arm64-v2.tar.gz -C /hdd/hdd5/gitlab-runner/offline
```

Runner 必须可读取该目录；自定义路径时修改 `DDB_ARM64_V2_OFFLINE_DIR`。Runner 需提供 `CI_CUS_LINUX_ARM_PYTHON_PATH_V2` 对应的 CPython 3.11，以及已加载的 `manylinux2014_aarch64:$DOCKER_TAG_V2` 镜像；镜像应包含各目标解释器、编译器、CMake、原生库和 auditwheel。镜像与离线材料不会在构建时从网络下载。

运行 GitLab 任务 `linux:arm64:v2`，检查七个目标均成功生成 wheel。成功后原有 FTP 上传任务会运行。
