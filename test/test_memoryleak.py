#!/usr/bin/python3
# -*- coding:utf-8 -*-
""""
@Author: shijianbo
@Time: 2024/2/5 10:31
@Note: 
"""
import gc

import dolphindb as ddb
import pytest
from memory_profiler import profile

from basic_testing.prepare import DataUtils
from setup.settings import *


@pytest.mark.skipif(AUTO_TESTING, reason="auto test not support")
class TestMemoryLeak:
    N = 100

    @staticmethod
    # @profile(precision=5)
    def run(conn, script, n):
        for i in range(n):
            conn.run(script)

    @staticmethod
    # @profile(precision=5)
    def upload(conn, data, n):
        for i in range(n):
            conn.upload(data)

    @profile(precision=5)
    @pytest.mark.parametrize('data', DataUtils.getScalar('download').values(),
                             ids=[i for i in DataUtils.getScalar('download')])
    def test_memory_leak_download_scalar(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        self.run(conn, data['value'], self.__class__.N)
        gc.collect()
        self.run(conn, data['value'], self.__class__.N)
        self.run(conn, data['value'], self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', DataUtils.getPair('download').values(),
                             ids=[i for i in DataUtils.getPair('download')])
    def test_memory_leak_download_pair(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        self.run(conn, data['value'], self.__class__.N)
        gc.collect()
        self.run(conn, data['value'], self.__class__.N)
        self.run(conn, data['value'], self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', DataUtils.getVector('download').values(),
                             ids=[i for i in DataUtils.getVector('download')])
    def test_memory_leak_download_vector(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        self.run(conn, data['value'], self.__class__.N)
        gc.collect()
        self.run(conn, data['value'], self.__class__.N)
        self.run(conn, data['value'], self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', DataUtils.getMatrix('download').values(),
                             ids=[i for i in DataUtils.getMatrix('download')])
    def test_memory_leak_download_matrix(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        self.run(conn, data['value'], self.__class__.N)
        gc.collect()
        self.run(conn, data['value'], self.__class__.N)
        self.run(conn, data['value'], self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', DataUtils.getSet('download').values(),
                             ids=[i for i in DataUtils.getSet('download')])
    def test_memory_leak_download_set(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        self.run(conn, data['value'], self.__class__.N)
        gc.collect()
        self.run(conn, data['value'], self.__class__.N)
        self.run(conn, data['value'], self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', DataUtils.getDict('download').values(),
                             ids=[i for i in DataUtils.getDict('download')])
    def test_memory_leak_download_dict(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        self.run(conn, data['value'], self.__class__.N)
        gc.collect()
        self.run(conn, data['value'], self.__class__.N)
        self.run(conn, data['value'], self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', DataUtils.getTable('download').values(),
                             ids=[i for i in DataUtils.getTable('download')])
    def test_memory_leak_download_table(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD, enablePickle=False)
        self.run(conn, data['value'], self.__class__.N)
        gc.collect()
        self.run(conn, data['value'], self.__class__.N)
        self.run(conn, data['value'], self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', DataUtils.getArrayVector('download').values(),
                             ids=[i for i in DataUtils.getArrayVector('download')])
    def test_memory_leak_download_array_vector(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        self.run(conn, data['value'], self.__class__.N)
        gc.collect()
        self.run(conn, data['value'], self.__class__.N)
        self.run(conn, data['value'], self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', DataUtils.getArrayVectorTable('download').values(),
                             ids=[i for i in DataUtils.getArrayVectorTable('download')])
    def test_memory_leak_download_array_vector_table(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD, enablePickle=False)
        self.run(conn, data['value'], self.__class__.N)
        gc.collect()
        self.run(conn, data['value'], self.__class__.N)
        self.run(conn, data['value'], self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getScalar('upload').items()],
                             ids=[i for i in DataUtils.getScalar('upload')])
    def test_memory_leak_upload_scalar(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        for k, v in data.items():
            self.upload(conn, {k: v['value']}, self.__class__.N)
            gc.collect()
            self.upload(conn, {k: v['value']}, self.__class__.N)
            self.upload(conn, {k: v['value']}, self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getVector('upload').items()],
                             ids=[i for i in DataUtils.getVector('upload')])
    def test_memory_leak_upload_vector(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        for k, v in data.items():
            self.upload(conn, {k: v['value']}, self.__class__.N)
            gc.collect()
            self.upload(conn, {k: v['value']}, self.__class__.N)
            self.upload(conn, {k: v['value']}, self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getMatrix('upload').items()],
                             ids=[i for i in DataUtils.getMatrix('upload')])
    def test_memory_leak_upload_matrix(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        for k, v in data.items():
            self.upload(conn, {k: v['value']}, self.__class__.N)
            gc.collect()
            self.upload(conn, {k: v['value']}, self.__class__.N)
            self.upload(conn, {k: v['value']}, self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getSet('upload').items()],
                             ids=[i for i in DataUtils.getSet('upload')])
    def test_memory_leak_upload_set(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        for k, v in data.items():
            self.upload(conn, {k: v['value']}, self.__class__.N)
            gc.collect()
            self.upload(conn, {k: v['value']}, self.__class__.N)
            self.upload(conn, {k: v['value']}, self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getDict('upload').items()],
                             ids=[i for i in DataUtils.getDict('upload')])
    def test_memory_leak_upload_dict(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        for k, v in data.items():
            self.upload(conn, {k: v['value']}, self.__class__.N)
            gc.collect()
            self.upload(conn, {k: v['value']}, self.__class__.N)
            self.upload(conn, {k: v['value']}, self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTable('upload').items()],
                             ids=[i for i in DataUtils.getTable('upload')])
    def test_memory_leak_upload_table(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        for k, v in data.items():
            self.upload(conn, {k: v['value']}, self.__class__.N)
            gc.collect()
            self.upload(conn, {k: v['value']}, self.__class__.N)
            self.upload(conn, {k: v['value']}, self.__class__.N)
        conn.close()

    @profile(precision=5)
    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getArrayVectorTable('upload').items()],
                             ids=[i for i in DataUtils.getArrayVectorTable('upload')])
    def test_memory_leak_upload_array_vector_table(self, data):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        for k, v in data.items():
            self.upload(conn, {k: v['value']}, self.__class__.N)
            gc.collect()
            self.upload(conn, {k: v['value']}, self.__class__.N)
            self.upload(conn, {k: v['value']}, self.__class__.N)
        conn.close()
