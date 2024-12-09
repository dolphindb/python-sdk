import inspect
from importlib.util import find_spec

import dolphindb as ddb
import numpy as np
import pytest
from dolphindb.io import dumps, loads, dump, load

from basic_testing.prepare import DataUtils, PANDAS_VERSION
from basic_testing.utils import equalPlus
from setup.settings import HOST, PORT, USER, PASSWD, REMOTE_WORK_DIR, WORK_DIR


class TestSerialize(object):
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.getScalar('upload').values(), DataUtils.getScalar('upload').keys()),
                             ids=DataUtils.getScalar('upload').keys())
    def test_serialize_scalar(self, data, ids):
        if data['value'] is None:
            pytest.skip()
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.getPair('download').values(), DataUtils.getPair('download').keys()),
                             ids=DataUtils.getPair('download').keys())
    def test_serialize_pair(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getPairContainNone('download').values(),
                                             DataUtils.getPairContainNone('download').keys()),
                             ids=DataUtils.getPairContainNone('download').keys())
    def test_serialize_pair_contain_none(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.getVector('upload').values(), DataUtils.getVector('upload').keys()),
                             ids=DataUtils.getVector('upload').keys())
    def test_serialize_vector(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getVectorContainNone('upload').values(),
                                             DataUtils.getVectorContainNone('upload').keys()),
                             ids=DataUtils.getVectorContainNone('upload').keys())
    def test_serialize_vector_contain_none(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.getVectorMix('upload').values(), DataUtils.getVectorMix('upload').keys()),
                             ids=DataUtils.getVectorMix('upload').keys())
    def test_serialize_vector_mix(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getVectorSpecial('upload').values(),
                                             DataUtils.getVectorSpecial('upload').keys()),
                             ids=DataUtils.getVectorSpecial('upload').keys())
    def test_serialize_vector_special(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.getMatrix('upload').values(), DataUtils.getMatrix('upload').keys()),
                             ids=DataUtils.getMatrix('upload').keys())
    def test_serialize_matrix(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getMatrixContainNone('upload').values(),
                                             DataUtils.getMatrixContainNone('upload').keys()),
                             ids=DataUtils.getMatrixContainNone('upload').keys())
    def test_serialize_matrix_contain_none(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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
    @pytest.mark.parametrize('data,ids', zip(DataUtils.getMatrixSpecial('upload').values(),
                                             DataUtils.getMatrixSpecial('upload').keys()),
                             ids=DataUtils.getMatrixSpecial('upload').keys())
    def test_serialize_matrix_special(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getSet('upload').values(), DataUtils.getSet('upload').keys()),
                             ids=DataUtils.getSet('upload').keys())
    def test_serialize_set(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getSetContainNone('upload').values(),
                                             DataUtils.getSetContainNone('upload').keys()),
                             ids=DataUtils.getSetContainNone('upload').keys())
    def test_serialize_set_contain_none(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getSetSpecial('download').values(),
                                             DataUtils.getSetSpecial('download').keys()),
                             ids=DataUtils.getSetSpecial('download').keys())
    def test_serialize_set_special(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getDict('upload').values(), DataUtils.getDict('upload').keys()),
                             ids=DataUtils.getDict('upload').keys())
    def test_serialize_dict(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getDictContainNone('upload').values(),
                                             DataUtils.getDictContainNone('upload').keys()),
                             ids=DataUtils.getDictContainNone('upload').keys())
    def test_serialize_dict_contain_none(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.getDictMix('upload').values(), DataUtils.getDictMix('upload').keys()),
                             ids=DataUtils.getDictMix('upload').keys())
    def test_serialize_dict_mix(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.getDictKey('upload').values(), DataUtils.getDictKey('upload').keys()),
                             ids=DataUtils.getDictKey('upload').keys())
    def test_serialize_dict_key(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getDictKeyContainNone('upload').values(),
                                             DataUtils.getDictKeyContainNone('upload').keys()),
                             ids=DataUtils.getDictKeyContainNone('upload').keys())
    def test_serialize_dict_key_contain_none(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getDictSpecial('upload').values(),
                                             DataUtils.getDictSpecial('upload').keys()),
                             ids=DataUtils.getDictSpecial('upload').keys())
    def test_serialize_dict_special(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.getTable('upload').values(), DataUtils.getTable('upload').keys()),
                             ids=DataUtils.getTable('upload').keys())
    def test_serialize_table(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getTableContainNone('upload').values(),
                                             DataUtils.getTableContainNone('upload').keys()),
                             ids=DataUtils.getTableContainNone('upload').keys())
    def test_serialize_table_contain_none(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.getTableMix('upload').values(), DataUtils.getTableMix('upload').keys()),
                             ids=DataUtils.getTableMix('upload').keys())
    def test_serialize_table_mix(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getTableSpecial('upload').values(),
                                             DataUtils.getTableSpecial('upload').keys()),
                             ids=DataUtils.getTableSpecial('upload').keys())
    def test_serialize_table_special(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids', zip(DataUtils.getTableExtensionDtype('upload').values(),
                                             DataUtils.getTableExtensionDtype('upload').keys()),
                             ids=DataUtils.getTableExtensionDtype('upload').keys())
    def test_serialize_table_extension_dtype(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        self.__class__.conn.upload({f"{func_name}_expect": data['value']})
        expect = self.__class__.conn.run(f"{func_name}_expect")
        # api->api bytes
        serialize_bytes = dumps(data['value'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, expect)
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(data['value'], f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, expect)
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close();
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, expect)
        # server->api
        _ = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.DATA_SCALAR_SET_TYPE.values(), DataUtils.DATA_SCALAR_SET_TYPE.keys()),
                             ids=DataUtils.DATA_SCALAR_SET_TYPE.keys())
    def test_serialize_scalar_set_type(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        # api->api bytes
        serialize_bytes = dumps(**data['params'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, data['expect'])
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(**data['params'], file=f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, data['expect'])
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f'{func_name}_serialize_bytes_char_vector': serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close()
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, data['expect'])

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.DATA_VECTOR_SET_TYPE.values(), DataUtils.DATA_VECTOR_SET_TYPE.keys()),
                             ids=DataUtils.DATA_VECTOR_SET_TYPE.keys())
    def test_serialize_vector_set_type(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        # api->api bytes
        serialize_bytes = dumps(**data['params'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, data['expect'])
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(**data['params'], file=f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, data['expect'])
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f'{func_name}_serialize_bytes_char_vector': serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close()
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, data['expect'])

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.DATA_MATRIX_SET_TYPE.values(), DataUtils.DATA_MATRIX_SET_TYPE.keys()),
                             ids=DataUtils.DATA_MATRIX_SET_TYPE.keys())
    def test_serialize_matrix_set_type(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        # api->api bytes
        serialize_bytes = dumps(**data['params'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, data['expect'])
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(**data['params'], file=f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, data['expect'])
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f'{func_name}_serialize_bytes_char_vector': serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close()
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, data['expect'])

    @pytest.mark.parametrize('data,ids', zip(DataUtils.DATA_SET_SET_TYPE.values(), DataUtils.DATA_SET_SET_TYPE.keys()),
                             ids=DataUtils.DATA_SET_SET_TYPE.keys())
    def test_serialize_set_set_type(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        # api->api bytes
        serialize_bytes = dumps(**data['params'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, data['expect'])
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(**data['params'], file=f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, data['expect'])
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f'{func_name}_serialize_bytes_char_vector': serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close()
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, data['expect'])

    @pytest.mark.parametrize('data,ids', zip(DataUtils.DATA_DICTIONARY_SET_TYPE.values(),
                                             DataUtils.DATA_DICTIONARY_SET_TYPE.keys()),
                             ids=DataUtils.DATA_DICTIONARY_SET_TYPE.keys())
    def test_serialize_dictionary_set_type(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        # api->api bytes
        serialize_bytes = dumps(**data['params'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, data['expect'])
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(**data['params'], file=f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, data['expect'])
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f'{func_name}_serialize_bytes_char_vector': serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close()
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, data['expect'])

    @pytest.mark.parametrize('data,ids',
                             zip(DataUtils.DATA_TABLE_SET_TYPE.values(), DataUtils.DATA_TABLE_SET_TYPE.keys()),
                             ids=DataUtils.DATA_TABLE_SET_TYPE.keys())
    def test_serialize_table_set_type(self, data, ids):
        func_name = inspect.currentframe().f_code.co_name + ids
        # api->api bytes
        serialize_bytes = dumps(**data['params'])
        deserialize_bytes = loads(serialize_bytes)
        assert equalPlus(deserialize_bytes, data['expect'])
        # api->api files
        with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
            dump(**data['params'], file=f)
        with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
            deserialize_file = load(f)
        assert equalPlus(deserialize_file, data['expect'])
        # api->server
        serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(np.int8)
        self.__class__.conn.upload({f'{func_name}_serialize_bytes_char_vector': serialize_bytes_char_vector})
        deserialize_server = self.__class__.conn.run(f"""
            f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
            f.writeBytes({func_name}_serialize_bytes_char_vector)
            f.close()
            readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
        """)
        assert equalPlus(deserialize_server, data['expect'])

    if PANDAS_VERSION >= (2, 0, 0) and find_spec("pyarrow") is not None:

        @pytest.mark.parametrize('data,ids', zip(DataUtils.getTableArrow('upload').values(),
                                                 DataUtils.getTableArrow('upload').keys()),
                                 ids=[i for i in DataUtils.getTableArrow('upload')])
        def test_serialize_table_arrow(self, data, ids):
            func_name = inspect.currentframe().f_code.co_name + ids
            self.__class__.conn.upload({f"{func_name}_expect": data['value']})
            expect = self.__class__.conn.run(f"{func_name}_expect")
            # api->api bytes
            serialize_bytes = dumps(data['value'])
            deserialize_bytes = loads(serialize_bytes)
            assert equalPlus(deserialize_bytes, expect)
            # api->api files
            with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
                dump(data['value'], f)
            with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
                deserialize_file = load(f)
            assert equalPlus(deserialize_file, expect)
            # api->server
            serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(
                np.int8)
            self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
            deserialize_server = self.__class__.conn.run(f"""
                f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
                f.writeBytes({func_name}_serialize_bytes_char_vector)
                f.close();
                readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
            """)
            assert equalPlus(deserialize_server, expect)
            # server->api
            _ = self.__class__.conn.run(f"""
                f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

        @pytest.mark.parametrize('data,ids', zip(DataUtils.getTableArrowContainNone('upload').values(),
                                                 DataUtils.getTableArrowContainNone('upload').keys()),
                                 ids=[i for i in DataUtils.getTableArrowContainNone('upload')])
        def test_serialize_table_arrow_contain_none(self, data, ids):
            func_name = inspect.currentframe().f_code.co_name + ids
            self.__class__.conn.upload({f"{func_name}_expect": data['value']})
            expect = self.__class__.conn.run(f"{func_name}_expect")
            # api->api bytes
            serialize_bytes = dumps(data['value'])
            deserialize_bytes = loads(serialize_bytes)
            assert equalPlus(deserialize_bytes, expect)
            # api->api files
            with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
                dump(data['value'], f)
            with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
                deserialize_file = load(f)
            assert equalPlus(deserialize_file, expect)
            # api->server
            serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(
                np.int8)
            self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
            deserialize_server = self.__class__.conn.run(f"""
                f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
                f.writeBytes({func_name}_serialize_bytes_char_vector)
                f.close();
                readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
            """)
            assert equalPlus(deserialize_server, expect)
            # server->api
            _ = self.__class__.conn.run(f"""
                f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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

        @pytest.mark.parametrize('data,ids', zip(DataUtils.getTableArrowSpecial('upload').values(),
                                                 DataUtils.getTableArrowSpecial('upload').keys()),
                                 ids=[i for i in DataUtils.getTableArrowSpecial('upload')])
        def test_serialize_table_arrow_special(self, data, ids):
            func_name = inspect.currentframe().f_code.co_name + ids
            self.__class__.conn.upload({f"{func_name}_expect": data['value']})
            expect = self.__class__.conn.run(f"{func_name}_expect")
            # api->api bytes
            serialize_bytes = dumps(data['value'])
            deserialize_bytes = loads(serialize_bytes)
            assert equalPlus(deserialize_bytes, expect)
            # api->api files
            with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
                dump(data['value'], f)
            with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
                deserialize_file = load(f)
            assert equalPlus(deserialize_file, expect)
            # api->server
            serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(
                np.int8)
            self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
            deserialize_server = self.__class__.conn.run(f"""
                f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
                f.writeBytes({func_name}_serialize_bytes_char_vector)
                f.close();
                readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
            """)
            assert equalPlus(deserialize_server, expect)
            # server->api
            _ = self.__class__.conn.run(f"""
                f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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
            for ids, data in DataUtils.getTableArrowArrayVector('upload').items():
                func_name = inspect.currentframe().f_code.co_name + ids
                self.__class__.conn.upload({f"{func_name}_expect": data['value']})
                expect = self.__class__.conn.run(f"{func_name}_expect")
                # api->api bytes
                serialize_bytes = dumps(data['value'])
                deserialize_bytes = loads(serialize_bytes)
                assert equalPlus(deserialize_bytes, expect)
                # api->api files
                with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
                    dump(data['value'], f)
                with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
                    deserialize_file = load(f)
                assert equalPlus(deserialize_file, expect)
                # api->server
                serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(
                    np.int8)
                self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
                deserialize_server = self.__class__.conn.run(f"""
                    f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
                    f.writeBytes({func_name}_serialize_bytes_char_vector)
                    f.close();
                    readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
                """)
                assert equalPlus(deserialize_server, expect)
                # server->api
                _ = self.__class__.conn.run(f"""
                    f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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
            for ids, data in DataUtils.getTableArrowArrayVectorContainNone('upload').items():
                func_name = inspect.currentframe().f_code.co_name + ids
                self.__class__.conn.upload({f"{func_name}_expect": data['value']})
                expect = self.__class__.conn.run(f"{func_name}_expect")
                # api->api bytes
                serialize_bytes = dumps(data['value'])
                deserialize_bytes = loads(serialize_bytes)
                assert equalPlus(deserialize_bytes, expect)
                # api->api files
                with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
                    dump(data['value'], f)
                with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
                    deserialize_file = load(f)
                assert equalPlus(deserialize_file, expect)
                # api->server
                serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(
                    np.int8)
                self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
                deserialize_server = self.__class__.conn.run(f"""
                    f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
                    f.writeBytes({func_name}_serialize_bytes_char_vector)
                    f.close();
                    readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
                """)
                assert equalPlus(deserialize_server, expect)
                # server->api
                _ = self.__class__.conn.run(f"""
                    f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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
            for ids, data in DataUtils.getTableArrowArrayVectorContainEmpty('upload').items():
                func_name = inspect.currentframe().f_code.co_name + ids
                self.__class__.conn.upload({f"{func_name}_expect": data['value']})
                expect = self.__class__.conn.run(f"{func_name}_expect")
                # api->api bytes
                serialize_bytes = dumps(data['value'])
                deserialize_bytes = loads(serialize_bytes)
                assert equalPlus(deserialize_bytes, expect)
                # api->api files
                with open(WORK_DIR + f"{func_name}.bin", 'wb') as f:
                    dump(data['value'], f)
                with open(WORK_DIR + f"{func_name}.bin", 'rb') as f:
                    deserialize_file = load(f)
                assert equalPlus(deserialize_file, expect)
                # api->server
                serialize_bytes_char_vector = np.array([ord(chr(b)) for b in serialize_bytes], dtype=np.uint8).astype(
                    np.int8)
                self.__class__.conn.upload({f"{func_name}_serialize_bytes_char_vector": serialize_bytes_char_vector})
                deserialize_server = self.__class__.conn.run(f"""
                    f=file("{REMOTE_WORK_DIR}{func_name}_api_server.bin","w+")
                    f.writeBytes({func_name}_serialize_bytes_char_vector)
                    f.close();
                    readObject("{REMOTE_WORK_DIR}{func_name}_api_server.bin")
                """)
                assert equalPlus(deserialize_server, expect)
                # server->api
                _ = self.__class__.conn.run(f"""
                    f=file("{REMOTE_WORK_DIR}{func_name}_server_api.bin","w+")
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
