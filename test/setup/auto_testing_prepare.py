import shutil
import random
import pickle
import re
from settings import *
import os
import sys

if sys.platform.startswith('win'):
    cur_os = 'win'
elif sys.platform.startswith('linux'):
    cur_os = 'linux'
else:
    cur_os = 'mac'

CONFIG_MAP = dict()

SERVER_MAP = dict()


def get_unique_ports(count):
    while True:
        numbers = random.sample(range(2000, 65535), count)
        if len(set(numbers)) == count:
            return numbers


def find_test_files(directory, ignoredir=None):
    test_files = []

    for root, dirs, _files in os.walk(directory):

        if ignoredir and ignoredir in dirs:
            dirs.remove(ignoredir)

        for _file in _files:
            if re.match(r'^test_.*\.py$', _file):
                test_files.append(os.path.join(root, _file).replace(os.sep, "/"))

    return test_files


test_path = sys.argv[1]
files = find_test_files(test_path)

for file in files:
    with open(file, encoding='utf-8') as f:
        content = f.read()
        r_port, r_sub_port, r_ctl_port = get_unique_ports(3)
        pattern = r'/test(/.*)?$'
        test_module = re.search(pattern, file).group(1)[1:]
        test_module = test_module.replace('/', '.')[:-3]
        r_server_path = SERVER_COPY_PATH + '/' + test_module

        if 'CTL_PORT' in content:
            CONFIG_MAP[test_module] = [HOST, r_port, USER, PASSWD, True, r_ctl_port]
        else:
            CONFIG_MAP[test_module] = [HOST, r_port, USER, PASSWD, False, None]

        SERVER_MAP[test_module] = r_server_path
    new_content = re.sub(r'\bCTL_PORT\b', str(r_ctl_port), content)
    new_content = re.sub(r'\bSUBPORT\b', str(r_sub_port), new_content)
    new_content = re.sub(r'\bPORT\b', str(r_port), new_content)
    with open(file, 'w', encoding='utf-8') as f:
        f.write(new_content)

if os.path.exists(PORTS_SAVE_PATH) and cur_os == 'win':
    os.remove(PORTS_SAVE_PATH)
for setup_name, server_path in SERVER_MAP.items():

    if cur_os in ['linux', 'win']:
        if os.path.exists(server_path):
            shutil.rmtree(server_path)

        if not CONFIG_MAP[setup_name][4]:
            shutil.copytree(SERVER_ROOT_PATH + '/server_single', server_path)
            with open(server_path + '/dolphindb.cfg', encoding='utf-8') as f:
                content = f.read()

            new_content = re.sub(r'8848', str(CONFIG_MAP[setup_name][1]), content)
            new_content = re.sub(r'8849', str(random.randint(2000, 65530)), new_content)

            with open(server_path + '/dolphindb.cfg', 'w', encoding='utf-8') as f:
                f.write(new_content)

            if cur_os == 'win':
                with open(PORTS_SAVE_PATH, 'a+', encoding='utf-8') as f:
                    f.write(str(CONFIG_MAP[setup_name][1]) + '\n')
        else:  # 集群
            if cur_os == 'linux':
                shutil.copytree(SERVER_ROOT_PATH + '/server_cluster', server_path)
                os.system(
                    f"cd {server_path}/config && sed -i 's/13900/{CONFIG_MAP[setup_name][5]}/g' `grep -rl 13900 .`")
                os.system(f"cd {server_path}/config && sed -i 's/13901/{CONFIG_MAP[setup_name][5] + 1}/g' *.*")
                os.system(f"cd {server_path}/config && sed -i 's/13902/{CONFIG_MAP[setup_name][1]}/g' *.*")
                os.system(f"cd {server_path}/config && sed -i 's/13903/{CONFIG_MAP[setup_name][1] + 2}/g' *.*")
                os.system(f"cd {server_path}/config && sed -i 's/13904/{CONFIG_MAP[setup_name][1] + 3}/g' *.*")
                os.system(f"cd {server_path}/config && sed -i 's/13905/{CONFIG_MAP[setup_name][1] + 4}/g' *.*")
                os.system(f"cd {server_path}/config && sed -i 's/13908/{CONFIG_MAP[setup_name][1] + 11}/g' *.*")
                os.system(f"cd {server_path}/config && sed -i 's/13909/{CONFIG_MAP[setup_name][1] + 12}/g' *.*")
                os.system(f"cd {server_path}/config && sed -i 's/13910/{CONFIG_MAP[setup_name][1] + 13}/g' *.*")
                os.system(f"cd {server_path}/config && sed -i 's/13911/{CONFIG_MAP[setup_name][1] + 14}/g' *.*")
            elif cur_os == 'win':
                shutil.copytree(SERVER_ROOT_PATH + '/server_cluster', server_path)
                # 确保当前执行环境中%sed%环境变量已配置（eg. set sed="D:/Git/usr/bin/sed.exe"）
                os.system(f"cd /d {server_path}/config && %sed% -i 's/13900/{CONFIG_MAP[setup_name][5]}/g' *.*")
                os.system(f"cd /d {server_path}/config && %sed% -i 's/13901/{CONFIG_MAP[setup_name][5] + 1}/g' *.*")
                os.system(f"cd /d {server_path}/config && %sed% -i 's/13902/{CONFIG_MAP[setup_name][1]}/g' *.*")
                os.system(f"cd /d {server_path}/config && %sed% -i 's/13903/{CONFIG_MAP[setup_name][1] + 2}/g' *.*")
                os.system(f"cd /d {server_path}/config && %sed% -i 's/13904/{CONFIG_MAP[setup_name][1] + 3}/g' *.*")
                os.system(f"cd /d {server_path}/config && %sed% -i 's/13905/{CONFIG_MAP[setup_name][1] + 4}/g' *.*")
                os.system(f"cd /d {server_path}/config && %sed% -i 's/13908/{CONFIG_MAP[setup_name][1] + 11}/g' *.*")
                os.system(f"cd /d {server_path}/config && %sed% -i 's/13909/{CONFIG_MAP[setup_name][1] + 12}/g' *.*")
                os.system(f"cd /d {server_path}/config && %sed% -i 's/13910/{CONFIG_MAP[setup_name][1] + 13}/g' *.*")
                os.system(f"cd /d {server_path}/config && %sed% -i 's/13911/{CONFIG_MAP[setup_name][1] + 14}/g' *.*")
                with open(PORTS_SAVE_PATH, 'a+', encoding='utf-8') as f:
                    f.write(str(CONFIG_MAP[setup_name][5]) + '\n')
                    f.write(str(CONFIG_MAP[setup_name][5] + 1) + '\n')
                    f.write(str(CONFIG_MAP[setup_name][1]) + '\n')
                    f.write(str(CONFIG_MAP[setup_name][1] + 2) + '\n')
                    f.write(str(CONFIG_MAP[setup_name][1] + 3) + '\n')
                    f.write(str(CONFIG_MAP[setup_name][1] + 4) + '\n')

    elif cur_os == 'mac':  # mac
        # not support
        pass

with open('SERVER_MAP.pickle', 'wb') as handle:
    pickle.dump(SERVER_MAP, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('CONFIG_MAP.pickle', 'wb') as handle:
    pickle.dump(CONFIG_MAP, handle, protocol=pickle.HIGHEST_PROTOCOL)
