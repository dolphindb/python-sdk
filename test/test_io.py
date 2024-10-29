import inspect
from importlib.util import find_spec

import dolphindb as ddb
import numpy as np
import pandas as pd
import pytest
from dolphindb.io import dumps, loads, dump, load

from basic_testing.prepare import DataUtils,PANDAS_VERSION
from basic_testing.utils import equalPlus
from setup.settings import *
from setup.utils import get_pid


class TestSerialize(object):
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except RuntimeError:
            self.conn.connect(HOST, PORT, USER, PASSWD)

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    @pytest.mark.parametrize('data', DataUtils.getScalar('upload').values(),
                             ids=DataUtils.getScalar('upload').keys())
    def test_serialize_scalar(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getPair('download').values(),
                             ids=DataUtils.getPair('download').keys())
    def test_serialize_pair(self, data):
        func_name = inspect.currentframe().f_code.co_name
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({data['value']})
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getPairContainNone('download').values(),
                             ids=DataUtils.getPairContainNone('download').keys())
    def test_serialize_pair_contain_none(self, data):
        func_name = inspect.currentframe().f_code.co_name
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({data['value']})
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getVector('upload').values(),
                             ids=DataUtils.getVector('upload').keys())
    def test_serialize_vector(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getVectorContainNone('upload').values(),
                             ids=DataUtils.getVectorContainNone('upload').keys())
    def test_serialize_vector_contain_none(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getVectorMix('upload').values(),
                             ids=DataUtils.getVectorMix('upload').keys())
    def test_serialize_vector_mix(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getVectorSpecial('upload').values(),
                             ids=DataUtils.getVectorSpecial('upload').keys())
    def test_serialize_vector_special(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getMatrix('upload').values(),
                             ids=DataUtils.getMatrix('upload').keys())
    def test_serialize_matrix(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getMatrixContainNone('upload').values(),
                             ids=DataUtils.getMatrixContainNone('upload').keys())
    def test_serialize_matrix_contain_none(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.skip
    @pytest.mark.parametrize('data', DataUtils.getMatrixSpecial('upload').values(),
                             ids=DataUtils.getMatrixSpecial('upload').keys())
    def test_serialize_matrix_special(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getSet('upload').values(),
                             ids=DataUtils.getSet('upload').keys())
    def test_serialize_set(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getSetContainNone('upload').values(),
                             ids=DataUtils.getSetContainNone('upload').keys())
    def test_serialize_set_contain_none(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getSetSpecial('download').values(),
                             ids=DataUtils.getSetSpecial('download').keys())
    def test_serialize_set_special(self, data):
        func_name = inspect.currentframe().f_code.co_name
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({data['value']})
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getDict('upload').values(),
                             ids=DataUtils.getDict('upload').keys())
    def test_serialize_dict(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getDictContainNone('upload').values(),
                             ids=DataUtils.getDictContainNone('upload').keys())
    def test_serialize_dict_contain_none(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getDictMix('upload').values(),
                             ids=DataUtils.getDictMix('upload').keys())
    def test_serialize_dict_mix(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getDictKey('upload').values(),
                             ids=DataUtils.getDictKey('upload').keys())
    def test_serialize_dict_key(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getDictKeyContainNone('upload').values(),
                             ids=DataUtils.getDictKeyContainNone('upload').keys())
    def test_serialize_dict_key_contain_none(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getDictSpecial('upload').values(),
                             ids=DataUtils.getDictSpecial('upload').keys())
    def test_serialize_dict_special(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getTable('upload').values(),
                             ids=DataUtils.getTable('upload').keys())
    def test_serialize_table(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getTableContainNone('upload').values(),
                             ids=DataUtils.getTableContainNone('upload').keys())
    def test_serialize_table_contain_none(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getTableMix('upload').values(),
                             ids=DataUtils.getTableMix('upload').keys())
    def test_serialize_table_mix(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getTableSpecial('upload').values(),
                             ids=DataUtils.getTableSpecial('upload').keys())
    def test_serialize_table_special(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.getTableExtensionDtype('upload').values(),
                             ids=DataUtils.getTableExtensionDtype('upload').keys())
    def test_serialize_table_extension_dtype(self, data):
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
            f.writeObject({func_name}_expect)
            f.seek(0,HEAD)
            len=f.seek(0,TAIL)
            f.seek(0,HEAD)
            x=f.readBytes(len)
            f.close();
            x
        """)
        bytes_list = []
        for i in _:
            if np.isnan(i):
                bytes_list.append(128)
            elif i > -1:
                bytes_list.append(int(i))
            else:
                bytes_list.append(int(i) + 256)
        serialize_bytes_server = bytes(bytes_list)
        deserialize_api = loads(serialize_bytes_server)
        assert equalPlus(deserialize_api, expect)

    @pytest.mark.parametrize('data', DataUtils.DATA_SCALAR_SET_TYPE.values(),
                             ids=DataUtils.DATA_SCALAR_SET_TYPE.keys())
    def test_serialize_scalar_set_type(self, data):
        func_name = inspect.currentframe().f_code.co_name
        # api->api bytes
        serialize_bytes = dumps(**data['params'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, data['expect'])
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(**data['params'], file=f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, data['expect'])
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f'{func_name}_serialize_bytes_char_vector': serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close()
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.DATA_VECTOR_SET_TYPE.values(),
                             ids=DataUtils.DATA_VECTOR_SET_TYPE.keys())
    def test_serialize_vector_set_type(self, data):
        func_name = inspect.currentframe().f_code.co_name
        # api->api bytes
        serialize_bytes = dumps(**data['params'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, data['expect'])
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(**data['params'], file=f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, data['expect'])
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f'{func_name}_serialize_bytes_char_vector': serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close()
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.DATA_MATRIX_SET_TYPE.values(),
                             ids=DataUtils.DATA_MATRIX_SET_TYPE.keys())
    def test_serialize_matrix_set_type(self, data):
        func_name = inspect.currentframe().f_code.co_name
        # api->api bytes
        serialize_bytes = dumps(**data['params'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, data['expect'])
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(**data['params'], file=f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, data['expect'])
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f'{func_name}_serialize_bytes_char_vector': serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close()
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.DATA_SET_SET_TYPE.values(),
                             ids=DataUtils.DATA_SET_SET_TYPE.keys())
    def test_serialize_set_set_type(self, data):
        func_name = inspect.currentframe().f_code.co_name
        # api->api bytes
        serialize_bytes = dumps(**data['params'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, data['expect'])
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(**data['params'], file=f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, data['expect'])
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f'{func_name}_serialize_bytes_char_vector': serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close()
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.DATA_DICTIONARY_SET_TYPE.values(),
                             ids=DataUtils.DATA_DICTIONARY_SET_TYPE.keys())
    def test_serialize_dictionary_set_type(self, data):
        func_name = inspect.currentframe().f_code.co_name
        # api->api bytes
        serialize_bytes = dumps(**data['params'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, data['expect'])
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(**data['params'], file=f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, data['expect'])
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f'{func_name}_serialize_bytes_char_vector': serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close()
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.DATA_TABLE_SET_TYPE.values(),
                             ids=DataUtils.DATA_TABLE_SET_TYPE.keys())
    def test_serialize_table_set_type(self, data):
        func_name = inspect.currentframe().f_code.co_name
        # api->api bytes
        serialize_bytes = dumps(**data['params'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, data['expect'])
        # api->api files
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
            dump(**data['params'], file=f)
        with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, data['expect'])
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
        self.__class__.conn.upload({f'{func_name}_serialize_bytes_char_vector': serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close()
            readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, data['expect'])

    if PANDAS_VERSION >= (2, 0, 0) and find_spec("pyarrow") is not None:

        @pytest.mark.parametrize('data', DataUtils.getTableArrow('upload').values(),
                                 ids=[i for i in DataUtils.getTableArrow('upload')])
        def test_serialize_table_arrow(self, data):
            func_name = inspect.currentframe().f_code.co_name
            self.__class__.conn.upload({f"{func_name}_expect": data['value']})
            expect = self.__class__.conn.run(f"{func_name}_expect")
            # api->api bytes
            serialize_bytes = dumps(data['value'])
            deserialize_bytes = loads(serialize_bytes)
            assert equalPlus(deserialize_bytes, expect)
            # api->api files
            with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
                dump(data['value'], f)
            with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
                deserialize_file = load(f)
            assert equalPlus(deserialize_file, expect)
            # api->server
            serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
            self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
            deserialize_server = self.__class__.conn.run(f"""
                f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
                f.writeBytes({func_name}_serialize_bytes_char_vector)
                f.close();
                readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
            """)
            assert equalPlus(deserialize_server, expect)
            # server->api
            _ = self.__class__.conn.run(f"""
                f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
                f.writeObject({func_name}_expect)
                f.seek(0,HEAD)
                len=f.seek(0,TAIL)
                f.seek(0,HEAD)
                x=f.readBytes(len)
                f.close();
                x
            """)
            bytes_list = []
            for i in _:
                if np.isnan(i):
                    bytes_list.append(128)
                elif i > -1:
                    bytes_list.append(int(i))
                else:
                    bytes_list.append(int(i) + 256)
            serialize_bytes_server = bytes(bytes_list)
            deserialize_api = loads(serialize_bytes_server)
            assert equalPlus(deserialize_api, expect)

        @pytest.mark.parametrize('data', DataUtils.getTableArrowContainNone('upload').values(),
                                 ids=[i for i in DataUtils.getTableArrowContainNone('upload')])
        def test_serialize_table_arrow_contain_none(self, data):
            func_name = inspect.currentframe().f_code.co_name
            self.__class__.conn.upload({f"{func_name}_expect": data['value']})
            expect = self.__class__.conn.run(f"{func_name}_expect")
            # api->api bytes
            serialize_bytes = dumps(data['value'])
            deserialize_bytes = loads(serialize_bytes)
            assert equalPlus(deserialize_bytes, expect)
            # api->api files
            with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
                dump(data['value'], f)
            with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
                deserialize_file = load(f)
            assert equalPlus(deserialize_file, expect)
            # api->server
            serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
            self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
            deserialize_server = self.__class__.conn.run(f"""
                f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
                f.writeBytes({func_name}_serialize_bytes_char_vector)
                f.close();
                readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
            """)
            assert equalPlus(deserialize_server, expect)
            # server->api
            _ = self.__class__.conn.run(f"""
                f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
                f.writeObject({func_name}_expect)
                f.seek(0,HEAD)
                len=f.seek(0,TAIL)
                f.seek(0,HEAD)
                x=f.readBytes(len)
                f.close();
                x
            """)
            bytes_list = []
            for i in _:
                if np.isnan(i):
                    bytes_list.append(128)
                elif i > -1:
                    bytes_list.append(int(i))
                else:
                    bytes_list.append(int(i) + 256)
            serialize_bytes_server = bytes(bytes_list)
            deserialize_api = loads(serialize_bytes_server)
            assert equalPlus(deserialize_api, expect)

        @pytest.mark.parametrize('data', DataUtils.getTableArrowSpecial('upload').values(),
                                 ids=[i for i in DataUtils.getTableArrowSpecial('upload')])
        def test_serialize_table_arrow_special(self, data):
            func_name = inspect.currentframe().f_code.co_name
            self.__class__.conn.upload({f"{func_name}_expect": data['value']})
            expect = self.__class__.conn.run(f"{func_name}_expect")
            # api->api bytes
            serialize_bytes = dumps(data['value'])
            deserialize_bytes = loads(serialize_bytes)
            assert equalPlus(deserialize_bytes, expect)
            # api->api files
            with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
                dump(data['value'], f)
            with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
                deserialize_file = load(f)
            assert equalPlus(deserialize_file, expect)
            # api->server
            serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
            self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
            deserialize_server = self.__class__.conn.run(f"""
                f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
                f.writeBytes({func_name}_serialize_bytes_char_vector)
                f.close();
                readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
            """)
            assert equalPlus(deserialize_server, expect)
            # server->api
            _ = self.__class__.conn.run(f"""
                f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
                f.writeObject({func_name}_expect)
                f.seek(0,HEAD)
                len=f.seek(0,TAIL)
                f.seek(0,HEAD)
                x=f.readBytes(len)
                f.close();
                x
            """)
            bytes_list = []
            for i in _:
                if np.isnan(i):
                    bytes_list.append(128)
                elif i > -1:
                    bytes_list.append(int(i))
                else:
                    bytes_list.append(int(i) + 256)
            serialize_bytes_server = bytes(bytes_list)
            deserialize_api = loads(serialize_bytes_server)
            assert equalPlus(deserialize_api, expect)

        def test_serialize_table_arrow_array_vector(self):
            for data in DataUtils.getTableArrowArrayVector('upload').values():
                func_name = inspect.currentframe().f_code.co_name
                self.__class__.conn.upload({f"{func_name}_expect": data['value']})
                expect = self.__class__.conn.run(f"{func_name}_expect")
                # api->api bytes
                serialize_bytes = dumps(data['value'])
                deserialize_bytes = loads(serialize_bytes)
                assert equalPlus(deserialize_bytes, expect)
                # api->api files
                with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
                    dump(data['value'], f)
                with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
                    deserialize_file = load(f)
                assert equalPlus(deserialize_file, expect)
                # api->server
                serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
                self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
                deserialize_server = self.__class__.conn.run(f"""
                    f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
                    f.writeBytes({func_name}_serialize_bytes_char_vector)
                    f.close();
                    readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
                """)
                assert equalPlus(deserialize_server, expect)
                # server->api
                _ = self.__class__.conn.run(f"""
                    f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
                    f.writeObject({func_name}_expect)
                    f.seek(0,HEAD)
                    len=f.seek(0,TAIL)
                    f.seek(0,HEAD)
                    x=f.readBytes(len)
                    f.close();
                    x
                """)
                bytes_list = []
                for i in _:
                    if np.isnan(i):
                        bytes_list.append(128)
                    elif i > -1:
                        bytes_list.append(int(i))
                    else:
                        bytes_list.append(int(i) + 256)
                serialize_bytes_server = bytes(bytes_list)
                deserialize_api = loads(serialize_bytes_server)
                assert equalPlus(deserialize_api, expect)

        def test_serialize_table_arrow_array_vector_contain_none(self):
            for data in DataUtils.getTableArrowArrayVectorContainNone('upload').values():
                func_name = inspect.currentframe().f_code.co_name
                self.__class__.conn.upload({f"{func_name}_expect": data['value']})
                expect = self.__class__.conn.run(f"{func_name}_expect")
                # api->api bytes
                serialize_bytes = dumps(data['value'])
                deserialize_bytes = loads(serialize_bytes)
                assert equalPlus(deserialize_bytes, expect)
                # api->api files
                with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
                    dump(data['value'], f)
                with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
                    deserialize_file = load(f)
                assert equalPlus(deserialize_file, expect)
                # api->server
                serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
                self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
                deserialize_server = self.__class__.conn.run(f"""
                    f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
                    f.writeBytes({func_name}_serialize_bytes_char_vector)
                    f.close();
                    readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
                """)
                assert equalPlus(deserialize_server, expect)
                # server->api
                _ = self.__class__.conn.run(f"""
                    f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
                    f.writeObject({func_name}_expect)
                    f.seek(0,HEAD)
                    len=f.seek(0,TAIL)
                    f.seek(0,HEAD)
                    x=f.readBytes(len)
                    f.close();
                    x
                """)
                bytes_list = []
                for i in _:
                    if np.isnan(i):
                        bytes_list.append(128)
                    elif i > -1:
                        bytes_list.append(int(i))
                    else:
                        bytes_list.append(int(i) + 256)
                serialize_bytes_server = bytes(bytes_list)
                deserialize_api = loads(serialize_bytes_server)
                assert equalPlus(deserialize_api, expect)

        def test_serialize_table_arrow_array_vector_contain_empty(self):
            for data in DataUtils.getTableArrowArrayVectorContainEmpty('upload').values():
                func_name = inspect.currentframe().f_code.co_name
                self.__class__.conn.upload({f"{func_name}_expect": data['value']})
                expect = self.__class__.conn.run(f"{func_name}_expect")
                # api->api bytes
                serialize_bytes = dumps(data['value'])
                deserialize_bytes = loads(serialize_bytes)
                assert equalPlus(deserialize_bytes, expect)
                # api->api files
                with open(WORK_DIR + os.sep + f"{func_name}.bin", 'wb') as f:
                    dump(data['value'], f)
                with open(WORK_DIR + os.sep + f"{func_name}.bin", 'rb') as f:
                    deserialize_file = load(f)
                assert equalPlus(deserialize_file, expect)
                # api->server
                serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.int8)
                self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
                deserialize_server = self.__class__.conn.run(f"""
                    f=file("{REMOTE_WORK_DIR}/{func_name}_api_server.bin","w+")
                    f.writeBytes({func_name}_serialize_bytes_char_vector)
                    f.close();
                    readObject("{REMOTE_WORK_DIR}/{func_name}_api_server.bin")
                """)
                assert equalPlus(deserialize_server, expect)
                # server->api
                _ = self.__class__.conn.run(f"""
                    f=file("{REMOTE_WORK_DIR}/{func_name}_server_api.bin","w+")
                    f.writeObject({func_name}_expect)
                    f.seek(0,HEAD)
                    len=f.seek(0,TAIL)
                    f.seek(0,HEAD)
                    x=f.readBytes(len)
                    f.close();
                    x
                """)
                bytes_list = []
                for i in _:
                    if np.isnan(i):
                        bytes_list.append(128)
                    elif i > -1:
                        bytes_list.append(int(i))
                    else:
                        bytes_list.append(int(i) + 256)
                serialize_bytes_server = bytes(bytes_list)
                deserialize_api = loads(serialize_bytes_server)
                assert equalPlus(deserialize_api, expect)
