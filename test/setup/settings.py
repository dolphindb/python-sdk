import os
import sys

cur_py_version = sys.version_info

# 本地测试时，修改以下配置即可
#########################################################
# 本地测试时默认为false，自动化环境中测试时为true
AUTO_TESTING = False

HOST='192.168.0.54'
# HOST='127.0.0.1'
PORT=8851
# PORT=8848
SUBPORT=36155
CTL_PORT=8900
CLIENT_HOST = "127.0.0.1"

USER="admin"
PASSWD="123456"
WORK_DIR=(os.path.abspath(os.path.join(os.path.abspath(__file__), "../..")))+ '/workdir'
# WORK_DIR="d:/code/python-sdk/test/workdir"
# WORK_DIR='/opt/codes/api_python3/test/workdir'
# DATA_DIR=(os.path.abspath(os.path.join(os.path.abspath(__file__), "../..")))+ '/data'
DATA_DIR='/opt/codes/api_python3/test/data'
# DATA_DIR="C:/Users/jianbo.shi/Downloads/data"
if AUTO_TESTING:
    LOCAL_DATA_DIR=DATA_DIR
else:
    # LOCAL_DATA_DIR=(os.path.abspath(os.path.join(os.path.abspath(__file__), "../..")))+ '/data'
    LOCAL_DATA_DIR="C:/Users/jianbo.shi/Downloads/data"

ARROW_DIR="/opt/dolphindb/plugin/Arrow/release300/PluginArrow.txt"
# ARROW_DIR=r"D:\\test\\plugin\\Arrow\\release200\\PluginArrow.txt"
#########################################################


# 以下为Jenkins自动化测试集群配置参数，本地测试时可不管
#########################################################
SERVER_ROOT_PATH = 'D:/test'
SERVER_COPY_PATH = 'D:/test'
# SERVER_COPY_PATH = f'/hdd/hdd5/yzou/api_python_mac_testing/mac_arm/py{cur_py_version.major}{cur_py_version.minor}'
PORTS_SAVE_PATH = 'D:/test/ports.txt' # 自动化测试时，windows下杀进程需要用到
#########################################################
