# Linux ARM64 v1 离线构建

v1 使用 manylinux2014 镜像构建 CPython 3.7。构建容器禁用网络，pip 使用只读目录中的 wheel 安装隔离构建依赖。保留 cibuildwheel 2.15.0、`PYTHON_REQUIRES` 和现有 wheel 修复流程；该任务不执行成品导入测试。

联网准备机器需有 Python 3.11 以上、pip 和 uv。在仓库根目录执行：

```bash
python3 scripts/offline/prepare_arm64_v1.py
```

输出 `.offline/arm64-v1.tar.gz` 及两份版本清单。可用 `--index-url` 指定包源、`--upgrade` 显式更新清单；构建依赖变化时应重新准备。`prepare_arm64_v1.py` 复用 v2 的下载函数，相关脚本和清单需一起提交。

在每台承载 `Linux-arm64` Runner 的 Docker 宿主机上上传并解压：

```bash
umask 022
mkdir -p /hdd/hdd5/gitlab-runner/offline
tar -xzf /传入位置/arm64-v1.tar.gz -C /hdd/hdd5/gitlab-runner/offline
```

Runner 必须能读取文件。自定义路径时修改 `DDB_ARM64_V1_OFFLINE_DIR`；Runner 需提供 `CI_CUS_LINUX_ARM_PYTHON_PATH_V2` 对应的 CPython 3.11，并预装 `manylinux2014_aarch64:$DOCKER_TAG_V1` 镜像。镜像需包含 Python 3.7、编译器、CMake、原生库和 auditwheel。

运行 GitLab 任务 `linux:arm64:v1`，检查构建产物。成功后原有 FTP 上传任务会运行。
