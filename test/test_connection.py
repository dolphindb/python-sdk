import pytest
from threading import Thread
from dolphindb import DBConnection, ConnectionSetting, ConnectionConfig
from setup.settings import HOST, PORT, USER, PASSWD
from basic_testing.utils import equalPlus
import ast
import inspect
import time
from datetime import timedelta
import os
import typing

from dolphindb.config import ConnectionSetting as _ConnectionSetting
from dolphindb.settings import SqlStd
from pydantic import ValidationError


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

    def test_DBConnection_netTimeout_default(self):
        conn = DBConnection()
        start = time.time()
        conn.connect("192.168.0.55", 1234, USER, PASSWD)
        assert 3 < time.time() - start < 3.5

    def test_DBConnection_netTimeout_0(self):
        conn = DBConnection()
        start = time.time()
        conn.connect("192.168.0.55", 1234, USER, PASSWD, net_timeout=timedelta())
        assert 3 < time.time() - start < 3.5

    def test_DBConnection_netTimeout_5(self):
        conn = DBConnection()
        start = time.time()
        conn.connect("192.168.0.55", 1234, USER, PASSWD, net_timeout=timedelta(seconds=5))
        assert 5 < time.time() - start < 5.5

    def test_DBConnection_timeout_default(self):
        conn = DBConnection()
        conn.connect(HOST, PORT, USER, PASSWD)
        conn.run("sleep(1000)")

    def test_DBConnection_timeout_3(self):
        conn = DBConnection()
        conn.connect(HOST, PORT, USER, PASSWD, timeout=timedelta(seconds=3))
        assert conn.run("sleep(1000);true")
        start = time.time()
        with pytest.raises(RuntimeError):
            conn.run("sleep(5000)")
        assert 3 < time.time() - start < 3.5

    def test_DBConnection_read_timeout(self):
        conn = DBConnection()
        conn.connect(HOST, PORT, USER, PASSWD, read_timeout=3)
        assert conn.run('true')
        start = time.time()
        with pytest.raises(RuntimeError):
            conn.run('sleep(4000);true')
        assert time.time() - start > 3

    @pytest.mark.skip()
    def test_DBConnection_write_timeout(self):
        conn = DBConnection()
        conn.connect(HOST, PORT, USER, PASSWD, write_timeout=1)
        print(os.system("tc qdisc add dev eno1 root netem delay 1000ms 20ms"))
        try:
            start = time.time()
            conn.run("all",conn.run("table(1..100000000 as a)"))
        except Exception as e:
            print(time.time()-start)
        os.system("tc qdisc del dev eno1 root")
        assert 1 < time.time() - start < 1.5


class _OverloadStub(object):

    def __init__(self, annotations, kwdefaults):
        self.annotations = annotations
        self.kwdefaults = kwdefaults


def _decorator_name(node):
    if isinstance(node, ast.Call):
        node = node.func
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


def _overload_stubs_from_source(func):
    path = inspect.getsourcefile(func)
    assert path, f"cannot locate source file of {func!r}, failed to inspect @overload hints"
    with open(path, encoding="utf-8") as f:
        source = f.read()
    tree = ast.parse(source)

    module = inspect.getmodule(func)
    namespace = vars(module) if module is not None else {}

    owner, _, name = func.__qualname__.rpartition(".")
    stubs = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or (owner and node.name != owner):
            continue
        for item in node.body:
            if not isinstance(item, ast.FunctionDef) or item.name != name:
                continue
            if not any(_decorator_name(dec) == "overload" for dec in item.decorator_list):
                continue
            annotations = {}
            for arg in list(item.args.args) + list(item.args.kwonlyargs):
                if arg.annotation is None:
                    continue
                expr = ast.get_source_segment(source, arg.annotation)
                annotations[arg.arg] = eval(expr, dict(namespace))
            kwdefaults = {}
            for arg, default in zip(item.args.kwonlyargs, item.args.kw_defaults):
                if default is None:
                    continue
                kwdefaults[arg.arg] = ast.literal_eval(default)
            stubs.append(_OverloadStub(annotations, kwdefaults))
    return stubs


def _overload_stubs(func):
    getter = getattr(typing, "get_overloads", None)
    if getter is not None:
        return [
            _OverloadStub(typing.get_type_hints(stub), stub.__kwdefaults__ or {})
            for stub in getter(func)
        ]
    return _overload_stubs_from_source(func)


def _sql_std_literal_values_from_overload():
    """Extract the accepted string values of ``sql_std`` from the constructor overload.

    Returns the ``Literal`` members declared in the keyword-only overload of
    ``DBConnection.__init__``. This lets a test assert the public type hint stays
    aligned with the values ``ConnectionSetting`` actually accepts.
    """
    overloads = _overload_stubs(DBConnection.__init__)
    assert overloads, "DBConnection.__init__ should expose at least one overload"

    candidates = set()
    for stub in overloads:
        annotation = stub.annotations.get("sql_std")
        if annotation is None:
            continue
        if typing.get_origin(annotation) is typing.Literal:
            candidates.update(typing.get_args(annotation))
        else:
            # Widen-safe: unwrap Optional[...]/Union[...] if the hint ever changes.
            candidates.update(
                arg
                for sub in typing.get_args(annotation)
                if typing.get_origin(sub) is typing.Literal
                for arg in typing.get_args(sub)
            )
    return candidates


class TestDBConnectionSqlStd(object):
    """Regression tests for APY-2069.

    The keyword-only constructor overload used to advertise
    ``sql_std: Literal["dolphindb", "oracle", "mysql"]`` while
    ``ConnectionSetting`` only accepts ``"ddb"`` / ``"mysql"`` / ``"oracle"``.
    Passing the advertised ``"dolphindb"`` therefore raised a ``ValidationError``
    at runtime. These tests pin the aligned contract.
    """

    @pytest.mark.parametrize("sql_std", ["ddb", "mysql", "oracle"])
    def test_DBConnection_accepts_advertised_sql_std_strings(self, sql_std):
        conn = DBConnection(sql_std=sql_std)
        try:
            assert conn.cpp is not None
        finally:
            conn.close()

    @pytest.mark.parametrize("sql_std", [SqlStd.DolphinDB, SqlStd.MySQL, SqlStd.Oracle])
    def test_DBConnection_accepts_sql_std_enum(self, sql_std):
        conn = DBConnection(sql_std=sql_std)
        try:
            assert conn.cpp is not None
        finally:
            conn.close()

    @pytest.mark.parametrize("sql_std", [0, 1, 2])
    def test_DBConnection_accepts_sql_std_int(self, sql_std):
        conn = DBConnection(sql_std=sql_std)
        try:
            assert conn.cpp is not None
        finally:
            conn.close()

    def test_DBConnection_sql_std_default(self):
        conn = DBConnection()
        try:
            assert conn.cpp is not None
        finally:
            conn.close()
        assert ConnectionSetting().sql_std == SqlStd.DolphinDB

    @pytest.mark.parametrize("sql_std", ["dolphindb", "DolphinDB", "unknown_sql_std"])
    def test_DBConnection_rejects_unaccepted_sql_std_string(self, sql_std):
        # "dolphindb"/"DolphinDB" is exactly the value the old overload advertised.
        with pytest.raises(ValidationError):
            DBConnection(sql_std=sql_std)

    def test_DBConnection_rejects_invalid_sql_std_int(self):
        with pytest.raises(ValidationError):
            DBConnection(sql_std=999)

    def test_DBConnection_sql_std_accepted_by_config_setting(self):
        # Every value the constructor overload advertises must be accepted by
        # ConnectionSetting, otherwise the hint lies about runtime behaviour.
        for value in ("ddb", "mysql", "oracle"):
            setting = _ConnectionSetting(sql_std=value)
            assert isinstance(setting.sql_std, SqlStd)

    def test_sql_std_overload_hint_matches_accepted_values(self):
        literal_values = _sql_std_literal_values_from_overload()
        expected = {"ddb", "mysql", "oracle"}
        assert literal_values == expected, (
            "DBConnection.__init__ sql_std hint {!r} diverges from the values "
            "ConnectionSetting accepts {!r}".format(literal_values, expected)
        )
        assert "dolphindb" not in literal_values

    def test_sql_std_overload_default_matches_config_default(self):
        overloads = _overload_stubs(DBConnection.__init__)
        defaults = next(
            (stub.kwdefaults for stub in overloads if "sql_std" in stub.kwdefaults),
            None,
        )
        assert defaults is not None, "could not locate the keyword-only overload default"

        assert defaults["sql_std"] == "ddb"
        assert ConnectionSetting().sql_std == SqlStd.DolphinDB

    def test_DBConnection_run_array_vector_to_columns(self):
        conn=DBConnection()
        conn.connect(HOST, PORT, USER, PASSWD)
        result = conn.run("table(1..3 as a,array(INT[]).append!([[1,2,3],[4,5,6],[7,8,9]]) as b)", array_vector_to_columns=True)
        except_ = conn.run("table(1..3 as a,[1,4,7] as b1,[2,5,8] as b2,[3,6,9] as b3)")
        assert equalPlus(result,except_)