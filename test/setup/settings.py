import os
import sys

cur_py_version = sys.version_info
AUTO_TESTING = False
HOST = '192.168.0.54'
PORT = 8851
SUBPORT = 36155
CTL_PORT = 35550
CLIENT_HOST = "127.0.0.1"
USER = "admin"
PASSWD = "123456"
WORK_DIR = (os.path.abspath(os.path.join(os.path.abspath(__file__), "../.."))) + '/workdir'
WORK_DIR="d:/code/python-sdk/test/workdir"
DATA_DIR = '/opt/downloads/data'
# DATA_DIR="C:/Users/jianbo.shi/Downloads/data"
if AUTO_TESTING:
    LOCAL_DATA_DIR = DATA_DIR
    REMOTE_WORK_DIR = WORK_DIR
else:
    # LOCAL_DATA_DIR=(os.path.abspath(os.path.join(os.path.abspath(__file__), "../..")))+ '/data'
    LOCAL_DATA_DIR = "C:/Users/jianbo.shi/Downloads/data"
    REMOTE_WORK_DIR = "/opt/download/test"

ARROW_DIR = "/opt/dolphindb/single/release300/server/plugins/Arrow/PluginArrow.txt"
# ARROW_DIR=r"D:\\test\\plugin\\Arrow\\release200\\PluginArrow.txt"

SERVER_ROOT_PATH = 'D:/test'
SERVER_COPY_PATH = 'D:/test'
# SERVER_COPY_PATH = f'/hdd/hdd5/yzou/api_python_mac_testing/mac_arm/py{cur_py_version.major}{cur_py_version.minor}'
PORTS_SAVE_PATH = 'D:/test/ports.txt'
