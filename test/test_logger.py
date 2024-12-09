#!/usr/bin/python3
# -*- coding:utf-8 -*-
""""
@Author: shijianbo
@Time: 2024/12/3 11:39
@Note: 
"""
import inspect
import os.path
import subprocess
import sys

import dolphindb as ddb
from dolphindb.logger import Sink, LogMessage

from setup.settings import HOST, PORT, USER, PASSWD, WORK_DIR


class TestLogger(object):

    def test_logger_default_logger_stdout(self):
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                                 f"conn.run(\"\"\"fromUTF8(\"中文\",\"gbk\")\"\"\");"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='gbk')
        assert "Error" in result.stdout

    def test_logger_default_logger_stdout_disable(self):
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                                 "ddb.logger.default_logger.disable_stdout_sink();"
                                 f"conn.run(\"\"\"fromUTF8(\"中文\",\"gbk\")\"\"\");"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='gbk')
        assert result.stdout == ""

    def test_logger_default_logger_stdout_enable(self):
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                                 "ddb.logger.default_logger.disable_stdout_sink();"
                                 "ddb.logger.default_logger.enable_stdout_sink();"
                                 f"conn.run(\"\"\"fromUTF8(\"中文\",\"gbk\")\"\"\");"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='gbk')
        assert "Error" in result.stdout

    def test_logger_default_logger_file_sink(self):
        func_name = inspect.currentframe().f_code.co_name
        subprocess.run([sys.executable, '-c',
                        "import dolphindb as ddb;"
                        f"ddb.logger.default_logger.enable_file_sink('{WORK_DIR}{func_name}.log');"
                        f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                        f"conn.run(\"\"\"fromUTF8(\"中文\",\"gbk\")\"\"\");"
                        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='gbk')
        with open(f"{WORK_DIR}{func_name}.log", encoding="gbk") as f:
            assert "Error" in f.read()

    def test_logger_default_logger_file_sink_disable(self):
        func_name = inspect.currentframe().f_code.co_name
        if os.path.exists(f"{WORK_DIR}{func_name}.log"):
            os.remove(f"{WORK_DIR}{func_name}.log")
        subprocess.run([sys.executable, '-c',
                        "import dolphindb as ddb;"
                        f"ddb.logger.default_logger.enable_file_sink('{WORK_DIR}{func_name}.log');"
                        "ddb.logger.default_logger.disable_file_sink();"
                        f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                        f"conn.run(\"\"\"fromUTF8(\"中文\",\"gbk\")\"\"\");"
                        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='gbk')
        with open(f"{WORK_DIR}{func_name}.log", encoding="gbk") as f:
            assert f.read() == ""

    def test_logger_default_logger_file_sink_enable(self):
        func_name = inspect.currentframe().f_code.co_name
        if os.path.exists(f"{WORK_DIR}{func_name}.log"):
            os.remove(f"{WORK_DIR}{func_name}.log")
        subprocess.run([sys.executable, '-c',
                        "import dolphindb as ddb;"
                        f"ddb.logger.default_logger.enable_file_sink('{WORK_DIR}{func_name}.log');"
                        "ddb.logger.default_logger.disable_file_sink();"
                        f"ddb.logger.default_logger.enable_file_sink('{WORK_DIR}{func_name}.log');"
                        f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                        f"conn.run(\"\"\"fromUTF8(\"中文\",\"gbk\")\"\"\");"
                        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='gbk')
        with open(f"{WORK_DIR}{func_name}.log", encoding="gbk") as f:
            assert "Error" in f.read()

    def test_logger_default_logger_file_sink_exists(self):
        func_name = inspect.currentframe().f_code.co_name
        with open(f"{WORK_DIR}{func_name}.log", "w+", encoding="gbk") as f:
            f.writelines(func_name + "\n")
        subprocess.run([sys.executable, '-c',
                        "import dolphindb as ddb;"
                        f"ddb.logger.default_logger.enable_file_sink('{WORK_DIR}{func_name}.log');"
                        f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                        f"conn.run(\"\"\"fromUTF8(\"中文\",\"gbk\")\"\"\");"
                        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='gbk')
        with open(f"{WORK_DIR}{func_name}.log", encoding="gbk") as f:
            assert func_name in f.readline()
            assert "Error" in f.readline()

    def test_logger_session_msg_logger_stdout(self):
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                                 f"print('test');"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert "test" in result.stdout

    def test_logger_session_msg_logger_stdout_disable(self):
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                                 "conn.msg_logger.disable_stdout_sink();"
                                 f"conn.run(\"print('test')\");"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout == ""

    def test_logger_session_msg_logger_stdout_enable(self):
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                                 "conn.msg_logger.disable_stdout_sink();"
                                 "conn.msg_logger.enable_stdout_sink();"
                                 f"conn.run(\"print('test')\");"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert "test" in result.stdout

    def test_logger_default_logger_customize_sink(self):
        logList = []

        class MySink(Sink):

            def handle(self, msg: LogMessage):
                logList.append(msg)

        ddb.logger.default_logger.add_sink(MySink("MySink"))
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        conn.run("fromUTF8(\"中文\",\"gbk\")")
        assert len(logList) == 1
        assert logList[0].level == ddb.logger.Level.ERROR
        assert 'Cannot decode data' in logList[0].log

    def test_logger_session_msg_logger_customize_sink(self):
        logList = []

        class MySink(Sink):

            def handle(self, msg: LogMessage):
                logList.append(msg)

        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        conn.msg_logger.add_sink(MySink("MySink"))
        conn.run("print('test')")
        assert len(logList) == 1
        assert logList[0].level == ddb.logger.Level.INFO
        assert 'test' in logList[0].log

    def test_logger_session_msg_logger_customize_sink_2(self):
        logList_1 = []
        logList_2 = []

        class MySink(Sink):

            def handle(self, msg: LogMessage):
                if self.name == 'MySink_1':
                    logList_1.append(msg)
                else:
                    logList_2.append(msg)

        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        conn.msg_logger.add_sink(MySink("MySink_1"))
        conn.msg_logger.add_sink(MySink("MySink_2"))
        conn.run("print('test')")
        assert len(logList_1) == 1
        assert logList_1[0].level == ddb.logger.Level.INFO
        assert 'test' in logList_1[0].log
        assert len(logList_2) == 1
        assert logList_2[0].level == ddb.logger.Level.INFO
        assert 'test' in logList_2[0].log

    def test_logger_session_msg_logger_list_sinks(self):
        logList = []

        class MySink(Sink):

            def handle(self, msg: LogMessage):
                logList.append(msg)

        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        assert len(conn.msg_logger.list_sinks()) == 0
        conn.msg_logger.add_sink(MySink("MySink_1"))
        assert len(conn.msg_logger.list_sinks()) == 1
        conn.msg_logger.add_sink(MySink("MySink_2"))
        assert len(conn.msg_logger.list_sinks()) == 2
        assert conn.msg_logger.list_sinks()[0].name == "MySink_1"
        assert conn.msg_logger.list_sinks()[1].name == "MySink_2"

    def test_logger_session_msg_logger_min_level(self):
        logList = []

        class MySink(Sink):

            def handle(self, msg: LogMessage):
                logList.append(msg)

        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        conn.msg_logger.add_sink(MySink("MySink"))
        conn.msg_logger.min_level = ddb.logger.Level.ERROR
        conn.run("print('test')")
        assert len(logList) == 0
