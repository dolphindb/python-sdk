import sys
import os
import threading
import time
import string, random
import inspect
import paramiko
from tqdm import tqdm
from stat import S_ISDIR as isdir

class CountBatchDownLatch():
    def __init__(self, count):
        self.count = count
        self.lock = threading.Lock()
        self.semp = threading.Semaphore(0)

    def wait(self):
        self.semp.acquire()

    def wait_s(self, timeout: float):
        return self.semp.acquire(timeout=timeout)

    def countDown(self, count):
        with self.lock:
            self.count -= count
            if (self.count == 0):
                self.semp.release()

    def getCount(self):
        with self.lock:
            return self.count

    def reset(self, count):
        with self.lock:
            self.count = count

def random_string(length):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for i in range(length))

def get_pid():
    return str(os.getpid())


def get_init_args(obj) -> dict:
    init_args = inspect.getmembers(obj, lambda x: inspect.ismethod(x) and x.__func__.__name__ == '__init__')[0][1].__annotations__
    return {k: getattr(obj, k.lower()) for k in init_args.keys()}


def get_sliding_window_data(data, start, length, window_size, step_size):
    num_rows = data.shape[0]

    if start < 0 or length > num_rows or window_size <= 0 or step_size <= 0:
        raise ValueError("Invalid input parameters.")

    result = []
    i = start
    while i + window_size <= length:
        window = data.iloc[i:i+window_size]
        # print(window)
        result.append(window.values.tolist())
        i += step_size

    return result

class SSHConnection(object):

    def __init__(self, host:str, port:int, username:str, pwd:str):
        self._ssh : paramiko.SSHClient = self._connect(host, port, username, pwd)
        self._sftp : paramiko.SFTPClient = self._ssh.open_sftp()

    def _connect(self, host, port, user, pwd):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, port, user, pwd)
        return ssh

    def disconnect(self):
        self._ssh.close()

    def upload(self,local_file,remote_file):
        self._sftp.put(local_file,remote_file)
        return True

    def download(self,remote_file,local_file):
        self._sftp.get(remote_file,local_file)
        return True

    def uploadfolder(self, local_path, remote_path):
        file_count = sum([len(files) for _, _, files in os.walk(local_path)])
        with tqdm(range(file_count), desc="uploading", ncols=100) as pbar:
            for root, _, files in os.walk(local_path):
                # 创建远程文件夹
                remote_dir = remote_path + root.replace(local_path, '').replace(os.sep, '/')
                try:
                    self._sftp.listdir(remote_dir)
                except:
                    self._sftp.mkdir(remote_dir)

                # 上传文件
                for file in files:
                    local_file_path = os.path.join(root, file)
                    remote_file_path = remote_dir + '/' + file
                    self._sftp.put(local_file_path, remote_file_path)
                    pbar.update(1)

    def downloadfolder(self, remote_path, local_path):
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        file_count = int(self.cmd(f"find {remote_path} -type f | wc -l")[0])

        with tqdm(range(file_count), desc="downloading", ncols=100) as pbar:
            for item in self._sftp.listdir_attr(remote_path):
                item_path = os.path.join(remote_path, item.filename)
                local_item_path = os.path.join(local_path, item.filename)

                if isdir(item.st_mode):
                    os.makedirs(local_item_path, exist_ok=True)
                    self.downloadfolder(item_path, local_item_path)
                else:
                    self._sftp.get(item_path, local_item_path)
                    pbar.update(1)


    def cmd(self, command, printFlag=True):
        (stdin, stdout, stderr) = self._ssh.exec_command(command)

        exitcode = stdout.channel.recv_exit_status()
        if exitcode != 0:
            err = stderr.read().decode('utf-8')
            raise RuntimeError(err)
        if not stdout.readlines():
            return None

        res = [x.replace('\n','') for x in stdout.readlines()]
        if printFlag:
            for i in res:
                # 打印执行反馈结果
                print(i)
        return res
