import pytest
import time
import dolphindb as ddb
import json
from filelock import FileLock
from setup.settings import *
from setup.utils import SSHConnection
import sys
import os

if sys.platform.startswith('win'):
    cur_os = 'win'
elif sys.platform.startswith('linux'):
    cur_os = 'linux'
else:
    cur_os = 'mac'


class ServerManager:
    def __init__(self, host, port, user, password, is_cluster, ctl_port=None):
        self.is_cluster = is_cluster
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.ctl_p = ctl_port
        self.restart_cmd = None

    def exec_cmd(self, cmd):
        if cur_os in ['win', 'linux']:
            os.system(cmd)
        elif cur_os == 'mac':
            conn = SSHConnection(self.host, 22, 'yzou', 'dol123')
            conn.cmd(cmd)
            conn.disconnect()

    def set_restart_cmd(self, sh):
        self.restart_cmd = sh

    def connect_to_server(self, h, po, u, pawd):
        conn = ddb.session()
        conn.connect(h, po, u, pawd)
        return conn

    def check_server_status(self, conn: ddb.session):
        try:
            conn.run("1+1")
            return True
        except RuntimeError:
            return False

    def check_if_restart(self):
        conn = ddb.session()

        if self.is_cluster:
            conn_ctl = ddb.session()
            if not conn_ctl.connect(self.host, self.ctl_p, self.user, self.password):
                print("The cluster has crashed, try to restart this cluster.")
                command = self.restart_cmd
                self.exec_cmd(command)
                wait_num = 1
                while wait_num <= 10:
                    time.sleep(6)
                    conn_ctl.connect(self.host, self.ctl_p, self.user, self.password)
                    conn.connect(self.host, self.port, self.user, self.password)
                    if self.check_server_status(conn) and self.check_server_status(conn_ctl):
                        break
                    wait_num += 1

                if wait_num == 10:
                    print("Time out, cluster is not ready yet.")
                    return False

            elif not conn.connect(self.host, self.port, self.user, self.password):
                print("Some datanodes in cluster are crashed, try to restart them.")
                restart_s = f"""
                                login("{self.user}", "{self.password}");
                                nodes = exec name from getClusterPerf() where state!=1 and mode !=1;
                                startDataNode(nodes);
                            """
                wait_num = 1
                conn_ctl.run(restart_s)

                while wait_num <= 10:
                    time.sleep(6)
                    conn.connect(self.host, self.port,
                                 self.user, self.password)
                    if self.check_server_status(conn):
                        break
                    wait_num += 1

                if wait_num == 10:
                    print("Time out, some datanodes are not ready yet.")
                    return False

            conn_ctl.close()

        else:
            if not conn.connect(self.host, self.port, self.user, self.password):
                print("Current single-mode server is crashed, try to restart it.")
                command = self.restart_cmd
                self.exec_cmd(command)
                wait_num = 1
                while wait_num <= 10:
                    time.sleep(6)
                    conn.connect(self.host, self.port,
                                 self.user, self.password)
                    if self.check_server_status(conn):
                        break
                    wait_num += 1

                if wait_num == 10:
                    print("Time out, current single-node is not ready yet.")
                    return False
                print("Restart successfully.")

        conn.close()
        return True


def check_version():
    cur_version = ddb.__version__
    setup_version = os.environ.get("API_VERSION")
    if not cur_version == setup_version:
        pytest.exit("current api version is different to env [API_VERSION]")
    return 'OK'


def get_restart_scripts():
    if cur_os == 'linux':
        _RESTRAT_SINGLE_INIT = 'cd {} && sh startSingle.sh'
        _RESTRAT_CLUSTER_INIT = 'cd {} && sh startAgent.sh && sh startController.sh'
    elif cur_os == 'win':
        _RESTRAT_SINGLE_INIT = 'cd /d {} && backgroundSingle.vbs'
        _RESTRAT_CLUSTER_INIT = 'cd /d {} && backgroundStartAgent.vbs && ping -n 2 127.0.0.1 && backgroundStartController.vbs'
    else:
        _RESTRAT_SINGLE_INIT = 'cd {} && sh startSingle.sh'
        _RESTRAT_CLUSTER_INIT = 'cd {} && sh startAgent.sh && sh startController.sh'
    return _RESTRAT_SINGLE_INIT, _RESTRAT_CLUSTER_INIT


if AUTO_TESTING:
    import pickle

    with open('setup/SERVER_MAP.pickle', 'rb') as handle:
        SERVER_MAP = pickle.load(handle)

    with open('setup/CONFIG_MAP.pickle', 'rb') as handle:
        CONFIG_MAP = pickle.load(handle)
    RESTRAT_SINGLE_INIT, RESTRAT_CLUSTER_INIT = get_restart_scripts()


@pytest.fixture(scope="session", autouse=AUTO_TESTING)
def check_package_version(tmp_path_factory, worker_id):
    if worker_id == "master":
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        check_version()

    # get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    fn = root_tmp_dir / "data.json"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            json.loads(fn.read_text())
        else:
            data = 'NG' if check_version() != 'OK' else 'OK'
            fn.write_text(json.dumps(data))


@pytest.fixture(scope="module", autouse=AUTO_TESTING)
def check_server_status(request):
    setup_name = request.node.nodeid.split('.')[0].replace('/', '.')
    manager = ServerManager(*CONFIG_MAP[setup_name])

    if CONFIG_MAP[setup_name][4]:
        manager.set_restart_cmd(
            RESTRAT_CLUSTER_INIT.format(SERVER_MAP[setup_name]))
    else:
        manager.set_restart_cmd(
            RESTRAT_SINGLE_INIT.format(SERVER_MAP[setup_name]))
    if not manager.check_if_restart():
        pytest.exit("Server cannot be started, may it was core dumped.")
