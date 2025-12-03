import pytest
from threading import Thread
from dolphindb import DBConnection, ConnectionSetting, ConnectionConfig
from setup.settings import HOST, PORT, USER, PASSWD


class TestDBConnection(object):

    def test_DBConnection_create_from_params(self):
        conn = DBConnection(enable_ssl=True,enable_async=True,compress=True,parser="kdb",protocol="ddb",show_output=False,sql_std="ddb")

    def test_DBConnection_create_from_config_class(self):
        DBConnection(config=ConnectionSetting(enable_ssl=True,enable_async=True,compress=True,parser="kdb",protocol="ddb",show_output=False,sql_std="ddb"))

    def test_DBConnection_create_from_config_dict(self):
        DBConnection(config=dict(enable_ssl=True,enable_async=True,compress=True,parser="kdb",protocol="ddb",show_output=False,sql_std="ddb"))

    def test_DBConnection_connect_from_params(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD, startup="print(1)")
        assert conn.is_alive

    def test_DBConnection_connect_from_config_class(self):
        conn = DBConnection()
        conn.connect(config=ConnectionConfig(host=HOST, port=PORT, userid=USER, password=PASSWD, startup="print(1)"))
        assert conn.is_alive

    def test_DBConnection_connect_from_config_dict(self):
        conn = DBConnection()
        conn.connect(config=dict(host=HOST, port=PORT, userid=USER, password=PASSWD, startup="print(1)"))
        assert conn.is_alive

    @pytest.mark.parametrize("enable_encryption", [True, False])
    def test_DBConnection_login(self, enable_encryption):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT)
        conn.login(USER, PASSWD, enable_encryption)
        assert conn.is_alive

    def test_DBConnection_close(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        assert conn.run("true")
        conn.close()
        assert conn.is_closed
        assert not conn.is_alive

    def test_DBConnection_session_id(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        assert conn.session_id == str(conn.call("getCurrentSessionAndUser")[0])

    def test_DBConnection_host(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        assert conn.host == HOST

    def test_DBConnection_port(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        assert conn.port == PORT

    def test_DBConnection_startup(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD, startup="print(1)")
        assert conn.startup == "print(1)"
        conn.startup = ""
        assert conn.startup == ""

    def test_DBConnection_is_alive_is_close(self):
        conn = DBConnection()
        assert not conn.is_alive
        assert not conn.is_closed
        conn.connect(host=HOST, port=PORT)
        assert conn.is_alive
        assert not conn.is_closed
        conn.close()
        assert not conn.is_alive
        assert conn.is_closed

    def test_DBConnection_is_busy(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)

        def busy(conn_):
            conn_.run("print(1000*1000)")

        t = Thread(target=busy, args=(conn,))
        assert not conn.is_busy
        t.start()
        assert conn.is_busy
        t.join()
        conn.close()

    def test_DBConnection_run_while_fetch_size_doing(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        block = conn.run("table(1..102400 as a)",fetch_size=9000)
        with pytest.raises(RuntimeError):
            conn.run("true")

    def test_DBConnection_run_while_fetch_size_done(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        block = conn.run("table(1..102400 as a)",fetch_size=9000)
        for i in block:
            pass
        assert conn.run("true")

    def test_DBConnection_exec_while_fetch_size_doing(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        block = conn.exec("table(1..102400 as a)",fetch_size=9000)
        with pytest.raises(RuntimeError):
            conn.exec("true")

    def test_DBConnection_exec_while_fetch_size_done(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        block = conn.exec("table(1..102400 as a)",fetch_size=9000)
        for i in block:
            pass
        assert conn.run("true")

    def test_DBConnection_call_while_fetch_size_doing(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        with pytest.raises(TypeError):
            conn.call("getSessionMemoryStat",fetch_size=9000)

    def test_DBConnection_call_full_parameter(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.exec("def test_parameter(a,b,c){return a+b+c;}")
        assert conn.call("test_parameter", 1, 2, 3) == 6

    def test_DBConnection_run_full_parameter(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.exec("def test_parameter(a,b,c){return a+b+c;}")
        assert conn.run("test_parameter", 1, 2, 3) == 6

    def test_DBConnection_call_empty_parameter(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.exec("def test_parameter(){return 6;}")
        assert conn.call("test_parameter") == 6

    def test_DBConnection_call_partial_parameter(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.exec("def test_parameter(a,b,c){return a+b+c}")
        assert conn.call("test_parameter{1}", 2, 3) == 6

    def test_DBConnection_run_partial_parameter(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.exec("def test_parameter(a,b,c){return a+b+c}")
        assert conn.run("test_parameter{1}", 2, 3) == 6

    def test_DBConnection_call_parameter_error(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.exec("def test_parameter(a,b,c){return a+b+c}")
        with pytest.raises(RuntimeError):
            conn.call("test_parameter", 2, 3)

    def test_DBConnection_run_parameter_error(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.exec("def test_parameter(a,b,c){return a+b+c}")
        with pytest.raises(RuntimeError):
            conn.run("test_parameter", 2, 3)

    def test_DBConnection_run_fetch_size(self):
        conn = DBConnection()
        conn.connect(host=HOST, port=PORT, userid=USER, password=PASSWD)
        blockReader = conn.run(f"table(take(1,{10000 * 10}) as a)", fetch_size=10000)
        total = 0
        for i in blockReader:
            total += len(i)
        else:
            blockReader.skip_all()
        assert total == 10000 * 10
