# Linux ARM64 / Python 3.14 离线打包

范围：使用离线依赖构建 `linux:arm64:v3`。ARM64 v1/v2 的离线构建见对应说明。

## 改动

- `.gitlab-ci.yml`：该任务改用 `build_arm64_v3.sh`；保留构建目标、Runner 标签、触发规则和 `wheelhouse/` 构建产物。
- `prepare_arm64_v3.py`：在联网机器解析、下载两组 ARM64 wheel，并打成离线包；只下载，不编译 SDK。
- `*.in` / `*.txt`：工具直接依赖与生成的完整版本清单。SDK 构建、运行依赖从 `pyproject.toml` 读取；uv 按目标 Python/平台解析，避免本机环境标记影响依赖选择。
- `cibuildwheel-4.2.0-cp314-constraints.txt`：随版本保存的官方工具约束，包括测试环境安装 virtualenv 时会使用的间接依赖版本。
- `build_arm64_v3.sh`：检查已上传材料和预制镜像，创建任务独享的 Python 3.11 venv，离线安装 cibuildwheel；构建容器禁用网络，并只读挂载 Python 3.14 离线材料。
- 保留 PEP 517 隔离构建，临时环境里的 pip 同样从本地目录安装依赖。覆盖原先在线配置包源和升级 pip 的 hook。
- 增加安装后 `import dolphindb` 冒烟检查，cibuildwheel 在源码目录之外执行。安装测试需要的 virtualenv 和 SDK 运行依赖也一并准备。
- `.offline/` 已忽略，不把下载的二进制包提交到 Git。

## 本机准备

要求 Python >= 3.11、pip 和 uv。uv 仅用于联网准备，不要求安装到 Runner。

在源码仓库根目录运行：

```bash
python3 scripts/offline/prepare_arm64_v3.py
```

输出 `.offline/arm64-v3.tar.gz`，结构：

```text
arm64-v3/
  host-cp311-aarch64/    # cibuildwheel 4.2.0 及其完整依赖
    requirements.txt
    *.whl
  cp314-aarch64/        # 构建工具、SDK 构建/运行依赖、测试环境工具
    requirements.txt
    *.whl
  README.md
```

默认沿用现有 `*.txt` 的锁定版本；SDK 依赖变化时重新解析并更新必要项。只有主动升级工具/依赖时加 `--upgrade`。可用 `--index-url` 指定联网准备时的包源。

下载只接受二进制 wheel。没有目标平台的合适包会直接失败，不退回源码编译。版本清单、CI 和脚本要一起交付，不能只上传压缩包。

## 服务器部署

1. 使用有上传权限的账号将离线包传到真正承载 `Linux-arm64` Runner 的机器，例如日志中的 `armserver`。多个候选 Runner 都需要部署。
2. 解压到稳定目录，不要放在会被 GitLab checkout 清理的项目工作目录。默认布局如下：

```bash
umask 022
mkdir -p /hdd/hdd5/gitlab-runner/offline
tar -xzf /传入位置/arm64-v3.tar.gz -C /hdd/hdd5/gitlab-runner/offline
```

确认 Runner 账号可读取目录中的文件。CI 的 `DDB_ARM64_V3_OFFLINE_DIR` 指向解压后的 `/hdd/hdd5/gitlab-runner/offline/arm64-v3`；若使用其他路径，同步调整该变量。挂载源路径必须位于 Docker 服务所在的机器。该路径位于项目 checkout 之外，在其父目录有写入权限的账号可自行创建；目录使用 755、文件使用 644，供 Runner 读取。不要放入权限为 700 的个人 home 或 Runner 的 `builds` 项目目录。

3. 确认 Runner 使用的同一个 Docker 服务已有 `manylinux_2_28_aarch64:$DOCKER_TAG_V3`，架构为 `linux/arm64`。

```bash
docker image inspect --format '{{.Os}}/{{.Architecture}}' manylinux_2_28_aarch64:dolphindb_v3
docker run --rm --network=none manylinux_2_28_aarch64:dolphindb_v3 \
  /opt/python/cp314-cp314/bin/python --version
```

`dolphindb_v3` 来自本次日志，实际以 GitLab 的 `DOCKER_TAG_V3` 为准。现有日志表明该镜像和 CPython 3.14 已安装；本次不重新制作或导出镜像。镜像仍须自带编译器、CMake、libuuid 开发库及 OpenSSL 等原生依赖，不能在 CI 中执行 yum 或下载 OpenSSL。

如果换机器缺少镜像，在已有镜像的机器执行 `docker save -o sdk-build-image.tar 镜像名:标签`，传入目标机器后执行 `docker load -i sdk-build-image.tar`。不要在离线任务里拉取外网镜像。

4. 将本次源码改动放到 GitLab 上要验证的分支。保留现有 `CI_CUS_LINUX_ARM_PYTHON_PATH_V2`（日志中为 CPython 3.11）、`DOCKER_TAG_V3` 变量。

## 服务器验证

在 GitLab 启动 `linux:arm64:v3`。无需给上传材料的个人账号增加 Docker 或 Conda 访问权限。该任务成功后，现有上传任务会自动清理对应 FTP 发布目录并上传。

如果已有直接运行构建所需的权限，也可在源码目录设置与 GitLab 相同的 `CI_CUS_LINUX_ARM_PYTHON_PATH_V2`、`DOCKER_TAG_V3` 后执行：

```bash
DDB_ARM64_V3_OFFLINE_DIR=/hdd/hdd5/gitlab-runner/offline/arm64-v3 \
  bash scripts/offline/build_arm64_v3.sh
```

脚本固定构建 `cp314-manylinux_aarch64`，手动执行也不会意外构建其他 Python 版本。脚本本身不上传 FTP。构建任务中观察：

1. 版本清单匹配、镜像架构检查通过，宿主机工具安装日志只出现本地离线目录。
2. 容器以 `--network=none` 运行；pip 不访问外部包源。
3. `before_build` 安装工具成功，隔离构建环境安装 `pybind11==3.1.0`、`numpy==2.3.5` 等依赖成功。
4. CMake/C++ 编译、`auditwheel repair` 成功。
5. Testing wheel 阶段安装成品并成功输出 SDK 版本；`wheelhouse/` 可下载到 cp314 / aarch64 的 wheel。

没有材料、版本清单不匹配或镜像缺失时立即退出，不在线补装。上述完整链路尚需在真实 ARM Runner 验证；本机不运行 SDK 原生编译或 cibuildwheel。

其他平台的阻塞式手动任务可能让整个 Pipeline 显示 blocked，先以 ARM 构建任务及其构建产物判断结果。连库验证另在允许访问内网 DolphinDB 的环境执行。上传任务仍在 `Linux-x86_64` Runner 上执行，需要它能访问 GitLab 构建产物和 FTP。

## 本次本地验证（2026-10-19）

- 准备了 11 个宿主机工具 wheel、25 个 CPython 3.14 构建/运行依赖 wheel。
- 检查所有 wheel 的 ZIP 完整性、ARM64/通用平台标签、锁定版本及目标 Python 环境下的完整依赖链。
- 已从离线目录在本机 CPython 3.11 的独立 venv 实际安装全部宿主机工具，`pip check` 通过。该组文件都是通用 Python wheel，不包含 ARM 原生扩展。
- CPython 3.14 / ARM64 依赖使用真实 pip 做 `--dry-run --ignore-installed --no-index --no-cache-dir` 检查，通过；未在 x86 本机安装或执行 ARM 扩展。
- 移除 pybind11 wheel 后，真实 pip 检查明确失败，未访问在线包源或退回源码编译。
- 通过实际 cibuildwheel 4.2.0 的配置解析器确认网络禁用、只读挂载、本地包源、hook 覆盖、构建目标和 import 冒烟配置生效；仅解析配置，未启动构建。
- 当时 CI YAML 结构、Bash/Python 语法检查通过。
- 尚未执行真实 ARM64 C++ 编译、动态库处理、成品导入或 FTP 上传；以上仍需服务器验证。

## 本次服务器材料核对（2026-10-19）

- 已将离线包上传并解压到 `armserver` 的 `/hdd/hdd5/gitlab-runner/offline/arm64-v3`。
- 在服务器核对两组 wheel 的文件名、数量及 requirements 内容，均与本地版本清单一致；全部 wheel 的 ZIP 完整性检查通过。
- 路径各级目录可遍历，依赖文件有读取权限，可供 Runner 使用；不需要个人账号访问 Docker。
- 个人账号不能读取构建 Conda 环境或访问 Docker，实际 Python 3.11、镜像及 SDK 编译/导入验证需由 GitLab 构建任务完成。

## APY-2070 启动检查修正

- 修正必填参数提示中的单引号，避免 Bash 对后续初始化代码产生错误解析。
- 已用真实 Bash 执行检查必填变量缺失、正常初始化及 Python 版本校验；并在 `armserver` 的 Bash 5.0.17 上确认路径、版本清单比较及镜像变量初始化正常。
- 本次修正不改变离线依赖，服务器无需重新上传依赖包；完整 ARM 构建需在 GitLab 构建任务中验证。

## 官方依据

- [cibuildwheel 4.2.0 配置](https://cibuildwheel.pypa.io/en/v4.2.0/options/)
- [cibuildwheel 4.2.0 CPython 3.14 工具约束](https://github.com/pypa/cibuildwheel/blob/v4.2.0/cibuildwheel/resources/constraints-python314.txt)
- [pip 跨平台下载](https://pip.pypa.io/en/stable/cli/pip_download/)
- [uv 目标平台解析](https://docs.astral.sh/uv/pip/compile/)
