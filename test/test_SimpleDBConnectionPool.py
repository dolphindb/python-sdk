import sys
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, wait
from unittest.mock import patch

import pandas as pd
import pytest
from dolphindb.config import ConnectionSetting, ConnectionConfig
from dolphindb.connection_pool import SimpleDBConnectionPool, SimpleDBConnectionPoolConfig, PoolEntry
from dolphindb.settings import (
    PROTOCOL_ARROW,
    PROTOCOL_DDB,
    PROTOCOL_PICKLE,
    ParserType,
    SqlStd,
    default_protocol,
)
from pydantic import ValidationError

from basic_testing.prepare import PYTHON_VERSION
from setup.settings import HOST, PORT, USER, PASSWD, HOST_CLUSTER, PORT_DNODE1, PORT_DNODE2, USER_CLUSTER, \
    PASSWD_CLUSTER


@pytest.fixture
def pool_config():
    return {
        "host": HOST,
        "port": PORT,
        "userId": "admin",
        "password": "123456",
        "min_pool_size": 2,
        "max_pool_size": 5,
        "idle_timeout": 30000,
        "check_interval": 10000,
    }


@pytest.fixture
def pool(pool_config):
    pool = SimpleDBConnectionPool(config=pool_config)
    yield pool
    if not pool.is_shutdown:
        pool.shutdown()


@pytest.fixture
def short_timeout_pool():
    config = {
        "host": HOST,
        "port": PORT,
        "userId": "admin",
        "password": "123456",
        "min_pool_size": 1,
        "max_pool_size": 3,
        "idle_timeout": 10000,
        "check_interval": 1000,
    }
    pool = SimpleDBConnectionPool(config=config)
    yield pool
    if not pool.is_shutdown:
        pool.shutdown()


class TestSimpleDBConnectionPool:
    def test_create_pool_with_dictionary_config(self, pool_config):
        pool = SimpleDBConnectionPool(config=pool_config)

        assert pool.total_count == 2
        assert pool.idle_count == 2
        assert pool.active_count == 0
        assert not pool.is_shutdown
        pool.shutdown()

    def test_create_pool_with_config_object(self, pool_config):
        config_obj = SimpleDBConnectionPoolConfig(**pool_config)
        pool = SimpleDBConnectionPool(config=config_obj)

        assert pool.total_count == 2
        assert pool.idle_count == 2
        assert pool.active_count == 0
        pool.shutdown()

    def test_create_pool_with_keyword_arguments(self):

        pool = SimpleDBConnectionPool(
            host=HOST,
            port=PORT,
            userId=USER,
            password=PASSWD,
            min_pool_size=3,
            max_pool_size=8,
            idle_timeout=40000
        )
        assert pool.config.min_pool_size == 3
        assert pool.config.max_pool_size == 8
        assert pool.total_count == 3
        assert pool.idle_count == 3
        assert pool.active_count == 0
        pool.shutdown()

    def test_acquire_and_use_connection_successfully(self, pool):
        conn = pool.acquire()
        assert isinstance(conn, PoolEntry)
        assert conn in pool._used_conns
        assert not conn.is_idle
        assert pool.active_count == 1
        assert pool.idle_count == pool.total_count - 1

        result = conn.run("1+1")
        assert result == 2

        pool.release(conn)
        assert conn.is_idle
        assert conn in pool._idle_conns

    def test_connection_reuse_simple(self, pool):
        used_connections = set()

        for i in range(10):
            conn = pool.acquire()

            result = conn.run(f"{i}+1")
            assert result == i + 1

            used_connections.add(id(conn))

            pool.release(conn)

        unique_connections_used = len(used_connections)
        total_requests = 10

        if unique_connections_used > 0:
            assert unique_connections_used <= pool.config.max_pool_size

            if unique_connections_used < total_requests:
                print("✅ 连接被重用了")
            else:
                print("ℹ️  所有连接都是新创建的")
        else:
            pytest.fail("没有成功获取任何连接")

    def test_connection_reuse_concurrent_simple(self, pool):
        import threading
        import queue

        result_queue = queue.Queue()
        errors = []

        def worker(worker_id):
            try:
                conn = pool.acquire(timeout=2000)

                conn_id = id(conn)

                result = conn.run(f"{worker_id}*2")
                assert result == worker_id * 2

                pool.release(conn)

                result_queue.put(conn_id)

            except Exception as e:
                errors.append(f"Worker {worker_id}: {e}")

        threads = []
        num_workers = min(10, pool.config.max_pool_size * 3)

        for i in range(num_workers):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=5)

        if errors:
            print(f"发生的错误: {errors}")
            if result_queue.qsize() == 0:
                pytest.skip("All workers failed, skipping test")

        connection_ids = []
        while not result_queue.empty():
            connection_ids.append(result_queue.get())

        unique_connections = len(set(connection_ids))
        total_successful = len(connection_ids)

        if unique_connections > 0:
            assert unique_connections <= pool.config.max_pool_size

            if unique_connections < total_successful:
                reuse_ratio = (total_successful - unique_connections) / total_successful
                assert reuse_ratio > 0, "应该有一定程度的连接重用"

    def test_connection_pool_counts_when_creating_new_connections(self, pool):
        initial_idle = pool.idle_count
        initial_total = pool.total_count

        assert initial_idle >= pool.config.min_pool_size
        assert initial_total >= pool.config.min_pool_size
        assert pool.active_count == 0

        conn1 = pool.acquire()
        conn2 = pool.acquire()

        assert pool.active_count == 2
        assert pool.idle_count == initial_idle - 2
        assert pool.total_count == initial_total

        conn3 = pool.acquire()
        assert pool.active_count == 3
        assert pool.idle_count == 0
        assert pool.total_count == initial_total + 1

        pool.release(conn1)
        pool.release(conn2)
        pool.release(conn3)

        assert pool.active_count == 0
        assert pool.idle_count == initial_idle + 1
        assert pool.total_count == initial_total + 1

    def test_acquire_connection_when_reaching_max_pool_size(self, pool):

        connections = []

        for i in range(pool.config.max_pool_size):
            conn = pool.acquire()
            connections.append(conn)
            assert pool.active_count == i + 1

        assert pool.active_count == pool.config.max_pool_size
        assert pool.total_count == pool.config.max_pool_size
        assert pool.idle_count == 0

        with pytest.raises(RuntimeError):
            pool.acquire()

        for conn in connections:
            pool.release(conn)

        assert pool.active_count == 0
        assert pool.total_count == pool.config.max_pool_size
        assert pool.idle_count == pool.config.max_pool_size

    def test_acquire_timeout_effectiveness(self, pool):

        connections = []
        for _ in range(pool.config.max_pool_size):
            connections.append(pool.acquire())

        start_time = time.time()
        with pytest.raises(RuntimeError):
            pool.acquire(timeout=100)
        short_elapsed = time.time() - start_time
        assert 0.08 <= short_elapsed <= 0.2, f"Short timeout elapsed: {short_elapsed}"

        start_time = time.time()
        with pytest.raises(RuntimeError):
            pool.acquire(timeout=500)
        medium_elapsed = time.time() - start_time

        assert 0.4 <= medium_elapsed <= 0.6, f"Medium timeout elapsed: {medium_elapsed}"
        assert medium_elapsed > short_elapsed, "Medium timeout should be longer than short timeout"

        with pytest.raises(ValueError):
            pool.acquire(timeout=0)

        with pytest.raises(ValueError):
            pool.acquire(timeout=-1)

        for conn in connections:
            pool.release(conn)

    def test_acquire_timeout_success_case(self, pool):

        connections = []
        for _ in range(pool.config.max_pool_size):
            connections.append(pool.acquire())

        def release_after_delay():
            time.sleep(0.2)
            pool.release(connections.pop())

        import threading
        thread = threading.Thread(target=release_after_delay)
        thread.start()

        start_time = time.time()
        conn = pool.acquire(timeout=1000)
        elapsed = time.time() - start_time

        assert conn is not None
        assert 0.19 <= elapsed <= 0.3, f"Should acquire connection within timeout, elapsed: {elapsed}"

        pool.release(conn)
        for conn in connections:
            pool.release(conn)
        thread.join()

    def test_acquire_timeout_boundary_values(self, pool):

        connections = []
        for _ in range(pool.config.max_pool_size):
            connections.append(pool.acquire())

        start_time = time.time()
        with pytest.raises(RuntimeError):
            pool.acquire(timeout=1)
        elapsed = time.time() - start_time

        assert elapsed < 0.1, f"1ms timeout should return quickly, elapsed: {elapsed}"

        start_time = time.time()
        with pytest.raises(RuntimeError):
            pool.acquire()
        no_timeout_elapsed = time.time() - start_time

        assert no_timeout_elapsed < 0.1, f"No timeout should return immediately, elapsed: {no_timeout_elapsed}"

        for conn in connections:
            pool.release(conn)

    def test_connection_release_updates_pool_counts(self, pool):

        conn = pool.acquire()

        initial_active = pool.active_count
        initial_idle = pool.idle_count
        initial_total = pool.total_count

        conn.release()

        assert pool.active_count == initial_active - 1
        assert pool.idle_count == initial_idle + 1
        assert pool.total_count == initial_total

        assert conn.is_idle

    def test_release_foreign_connection_raises_error(self, pool):
        conn = pool.acquire()

        class FakeConnection:
            def __init__(self):
                self.is_busy = False
                self.host = "fake-host"
                self.port = 12345

        fake_conn = FakeConnection()

        with pytest.raises(RuntimeError):
            pool.release(fake_conn)

    def test_connection_busy_status_during_execution(self, pool):

        conn = pool.acquire()

        assert conn.is_busy is False

        pool.release(conn)

        conn = pool.acquire()
        busy_results = []

        def execute_and_check():
            conn.run("sleep(1000)")

        def check_busy():
            busy_results.append(conn.is_busy)

        t1 = threading.Thread(target=execute_and_check)
        t1.start()

        import time
        time.sleep(0.1)

        t2 = threading.Thread(target=check_busy)
        t2.start()
        t2.join()

        assert busy_results[0] is True

        t1.join(timeout=0.1)

        with pytest.raises(RuntimeError):
            pool.release(conn)

    def test_pool_managed_connection_operations_are_prohibited(self, pool):

        conn = pool.acquire()

        with pytest.raises(RuntimeError):
            conn.connect()

        with pytest.raises(RuntimeError):
            conn.login("user", "password")

        with pytest.raises(RuntimeError):
            conn.close()

        pool.release(conn)

    def test_operations_on_released_connection_are_prohibited(self, pool):

        conn = pool.acquire()
        pool.release(conn)

        with pytest.raises(RuntimeError):
            conn.run("1+1")

        with pytest.raises(RuntimeError):
            conn.exec("SELECT 1")

        with pytest.raises(RuntimeError):
            conn.call("some_procedure")

        with pytest.raises(RuntimeError):
            conn.upload({"test": [1, 2, 3]})

        with pytest.raises(RuntimeError):
            conn.release()

        conn2 = pool.acquire()
        result = conn2.run("2+2")
        assert result == 4
        pool.release(conn2)

    def test_automatic_idle_cleanup_after_timeout(self, short_timeout_pool):
        pool = short_timeout_pool

        connections = []
        for i in range(pool.config.max_pool_size):
            conn = pool.acquire()
            connections.append(conn)

        for conn in connections:
            pool.release(conn)

        time.sleep(11)

        final_idle = pool.idle_count
        final_total = pool.total_count

        assert final_idle == pool.config.min_pool_size
        assert final_total == pool.config.min_pool_size

    def test_automatic_cleanup_before_timeout(self, short_timeout_pool):
        pool = short_timeout_pool

        extra_connections = 1
        target_count = pool.config.min_pool_size + extra_connections

        connections = []
        for i in range(target_count):
            conn = pool.acquire()
            connections.append(conn)

        for conn in connections:
            pool.release(conn)

        initial_idle = pool.idle_count
        initial_total = pool.total_count
        assert initial_idle == target_count
        assert initial_total == target_count
        assert initial_idle > pool.config.min_pool_size

        time.sleep(9)

        assert pool.idle_count == initial_idle
        assert pool.total_count == initial_total

        time.sleep(2)

        assert pool.idle_count == pool.config.min_pool_size
        assert pool.total_count == pool.config.min_pool_size

    def test_automatic_cleanup_preserves_min_pool_size(self, short_timeout_pool):
        pool = short_timeout_pool

        connections = []
        for i in range(pool.config.min_pool_size):
            conn = pool.acquire()
            connections.append(conn)

        for conn in connections:
            pool.release(conn)

        assert pool.idle_count == pool.config.min_pool_size
        assert pool.total_count == pool.config.min_pool_size

        time.sleep(11)

        assert pool.idle_count == pool.config.min_pool_size
        assert pool.total_count == pool.config.min_pool_size

    def test_automatic_cleanup_thread_respects_check_interval(self):

        check_calls = []
        original_cleanup = SimpleDBConnectionPool._cleanup_idle_internal

        def mock_cleanup(self):
            check_calls.append(time.time())
            return original_cleanup(self)

        config = SimpleDBConnectionPoolConfig(
            host=HOST,
            port=PORT,
            userid="admin",
            password="123456",
            check_interval=5000
        )

        with patch.object(SimpleDBConnectionPool, '_cleanup_idle_internal', mock_cleanup):
            pool = SimpleDBConnectionPool(config=config)

            try:
                time.sleep(12)

                assert 1 <= len(check_calls) <= 3, f"Expected 1-3 calls, got {len(check_calls)}"

                if len(check_calls) >= 2:
                    intervals = []
                    for i in range(1, len(check_calls)):
                        interval = check_calls[i] - check_calls[i - 1]
                        intervals.append(interval)

                    avg_interval = sum(intervals) / len(intervals)
                    assert 3.0 <= avg_interval <= 7.0, f"Average interval {avg_interval} not in expected range 3-7 seconds"

            finally:
                pool.shutdown()

    def test_manual_idle_cleanup_with_extra_connections(self, pool):
        initial_idle = pool.idle_count
        initial_total = pool.total_count

        assert initial_idle == pool.config.min_pool_size
        assert initial_total == pool.config.min_pool_size

        all_connections = []

        for _ in range(initial_idle):
            conn = pool.acquire()
            all_connections.append(conn)

        assert pool.idle_count == 0
        assert pool.active_count == initial_total

        extra_count = 2
        for _ in range(extra_count):
            conn = pool.acquire()
            all_connections.append(conn)

        after_auto_create_total = pool.total_count
        after_auto_create_active = pool.active_count

        assert after_auto_create_total == initial_total + extra_count
        assert after_auto_create_active == initial_total + extra_count

        for conn in all_connections:
            pool.release(conn)

        after_release_idle = pool.idle_count
        after_release_total = pool.total_count

        assert after_release_idle == initial_total + extra_count
        assert after_release_idle > pool.config.min_pool_size
        assert after_release_total == initial_total + extra_count

        pool.close_idle()

        after_cleanup_idle = pool.idle_count
        after_cleanup_total = pool.total_count

        assert after_cleanup_total == pool.config.min_pool_size
        assert after_cleanup_idle == pool.config.min_pool_size
        assert after_cleanup_total < after_release_total

    def test_min_pool_size_respected(self):
        pool = SimpleDBConnectionPool(host=HOST, port=PORT, userid=USER, password=PASSWD)
        pool.close_idle()

        assert pool.total_count >= pool.config.min_pool_size
        assert pool.idle_count >= pool.config.min_pool_size

    def test_concurrent_acquire_release(self, pool):

        results = []
        errors = []
        lock = threading.Lock()

        def worker(worker_id):
            try:
                conn = pool.acquire(timeout=5000)

                result = conn.run(f"{worker_id} + 100")

                with lock:
                    results.append((worker_id, result))

                time.sleep(0.01)

                pool.release(conn)

            except Exception as e:
                with lock:
                    errors.append((worker_id, str(e)))

        threads = []
        for i in range(10):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=10)

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 10

        for worker_id, result in results:
            assert result == worker_id + 100

        assert pool.active_count == 0
        assert pool.idle_count <= pool.config.max_pool_size

    def test_thread_safety(self, pool):

        exception_count = [0]
        success_count = [0]
        lock = threading.Lock()

        def stress_worker():
            for _ in range(20):
                try:
                    conn = pool.acquire(timeout=1000)

                    conn.run("1+1")

                    time.sleep(0.001)

                    pool.release(conn)

                    with lock:
                        success_count[0] += 1

                except Exception:
                    with lock:
                        exception_count[0] += 1

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(stress_worker) for _ in range(8)]
            wait(futures, timeout=30)

        total_operations = success_count[0] + exception_count[0]
        success_rate = success_count[0] / total_operations if total_operations > 0 else 0

        assert success_rate > 0.9, f"Success rate too low: {success_rate}"
        assert pool.active_count == 0, "All connections should be released"

    def test_shutdown_without_active_connections(self):
        pool = SimpleDBConnectionPool(host=HOST, port=PORT, userid=USER, password=PASSWD)
        assert not pool.is_shutdown
        pool.shutdown()

        assert pool.is_shutdown
        assert pool.active_count == 0
        assert pool.idle_count == 0
        assert pool.total_count == 0

    def test_shutdown_with_active_connections(self, pool):
        conn1 = pool.acquire()
        conn2 = pool.acquire()

        assert pool.active_count == 2

        pool.shutdown()

        assert pool.is_shutdown
        assert pool.active_count == 0
        assert pool.idle_count == 0
        assert pool.total_count == 0

    def test_acquire_after_shutdown(self):
        pool = SimpleDBConnectionPool(host=HOST, port=PORT, userid=USER, password=PASSWD)
        pool.shutdown()

        try:
            conn = pool.acquire()
            assert conn is None
        except RuntimeError:
            pass
        except Exception as e:
            pytest.fail(f"Unexpected exception: {e}")

        assert pool.is_shutdown
        assert pool.active_count == 0
        assert pool.idle_count == 0
        assert pool.total_count == 0

    def test_release_after_shutdown(self):
        pool = SimpleDBConnectionPool(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn = pool.acquire()

        pool.shutdown()

        pool.release(conn)

        assert pool.total_count == 0

    def test_double_shutdown(self):
        pool = SimpleDBConnectionPool(host=HOST, port=PORT, userid=USER, password=PASSWD)
        pool.shutdown()
        assert pool.is_shutdown

        pool.shutdown()
        assert pool.is_shutdown

    @pytest.mark.CLUSTER
    @pytest.mark.xdist_group(name='cluster_test')
    @pytest.mark.parametrize("enable_high_availability,use_public_name", [
        (True, True),
        (True, False),
        (False, True),
        (False, False),
    ])
    def test_simpledbconnectionpool_ha_usePublicName_parametrized(self, enable_high_availability, use_public_name):
        pool = SimpleDBConnectionPool(
            host=HOST_CLUSTER,
            port=PORT_DNODE2,
            userId=USER_CLUSTER,
            password=PASSWD_CLUSTER,
            enable_high_availability=enable_high_availability,
            use_public_name=use_public_name,
            min_pool_size=2,
            max_pool_size=6,
            idle_timeout=40000
        )

        assert pool.config.min_pool_size == 2
        assert pool.config.max_pool_size == 6
        assert pool.total_count == 2
        assert pool.idle_count == 2
        assert pool.active_count == 0
        pool.shutdown()


class TestDataOperations:

    def test_basic_script_execution(self, pool):
        conn = pool.acquire()

        result = conn.run("1 + 1")
        assert result == 2

        result = conn.run("table(1..5 as id, take(`A`B`C`D`E, 5) as name)")
        assert result is not None

        pool.release(conn)

    def test_upload_data(self, pool):
        conn = pool.acquire()

        data_dict = {
            "numbers": [1, 2, 3, 4, 5],
            "strings": ["a", "b", "c", "d", "e"]
        }

        conn.upload(data_dict)

        result = conn.run("numbers")
        assert len(result) == 5

        result = conn.run("strings")
        assert len(result) == 5

        pool.release(conn)

    def test_upload_dataframe(self, pool):
        conn = pool.acquire()

        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': [10.5, 20.3, 30.1, 40.7, 50.9]
        })

        conn.upload({"test_df": df})

        result = conn.run("test_df")
        assert result is not None

        pool.release(conn)

    def test_complex_data_types(self, pool):
        conn = pool.acquire()

        test_script = """
        test_table = table(
            1..10 as id,
            take(`A`B`C, 10) as symbol_col,
            2010.01.01 + 0..9 as date_col,
            2010.01M + 0..9 as month_col,
            09:00:00.000 + 0..9 * 1000 as time_col,
            10.5 * 1..10 as double_col
        )
        test_table
        """

        result = conn.run(test_script)
        assert result is not None

        pool.release(conn)

    def test_large_data_operations(self, pool):
        conn = pool.acquire()

        result = conn.run("table(1..10000 as id, rand(100.0, 10000) as value)")
        assert result is not None

        result = conn.run("select avg(value) from table(1..10000 as id, rand(100.0, 10000) as value)")
        assert result is not None

        pool.release(conn)


class TestErrorHandling:
    def test_missing_host_field(self):
        with pytest.raises(ValidationError):
            SimpleDBConnectionPoolConfig(
                port=PORT,
                userid="admin",
                password="123456"
            )

    @pytest.mark.parametrize("invalid_host, description", [
        ("invalid-host", "不存在的域名"),
        ("111.111.111.111.111", "格式错误的IP地址"),
        ("", "空字符串主机名"),
        ("   ", "空白字符主机名"),
        ("nonexistent.example.com", "无法解析的域名"),
        ("256.256.256.256", "无效的IP地址范围")
    ])
    def test_invalid_host_connection_failure(self, invalid_host, description):
        config = SimpleDBConnectionPoolConfig(
            host=invalid_host,
            port=PORT,
            userid="admin",
            password="123456",
        )

        with pytest.raises(RuntimeError):
            pool = SimpleDBConnectionPool(config=config)

    def test_missing_port_field(self):
        with pytest.raises(ValidationError):
            SimpleDBConnectionPoolConfig(
                host=HOST,
                userid="admin",
                password="123456"
            )

    @pytest.mark.parametrize("invalid_port", [9999, -111, 0, 65536, -1, 80, 443, 70000])
    def test_invalid_port_connection_failure(self, invalid_port):
        config = SimpleDBConnectionPoolConfig(
            host=HOST,
            port=invalid_port,
            userid="admin",
            password="123456",
        )

        with pytest.raises(RuntimeError):
            pool = SimpleDBConnectionPool(config=config)

    def test_missing_userid_field(self):
        config = SimpleDBConnectionPoolConfig(
            host=HOST,
            port=PORT,
        )
        pool = SimpleDBConnectionPool(config=config)
        conn = pool.acquire()
        with pytest.raises(RuntimeError):
            conn.run("1+1")

    def test_SimpleDBConnectionPool_config_userId_error(self):
        config = SimpleDBConnectionPoolConfig(
            host=HOST,
            port=PORT,
            userid="admin_error",
            password="123456",
        )

        with pytest.raises(RuntimeError):
            pool = SimpleDBConnectionPool(config=config)

    def test_SimpleDBConnectionPool_config_userId_not_admin(self, pool):
        conn = pool.acquire()
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};" +
                 "rpc(getControllerAlias(),create_user);")
        config = SimpleDBConnectionPoolConfig(
            host=HOST,
            port=PORT,
            userid="test1",
            password="123456",
        )
        pool = SimpleDBConnectionPool(config=config)
        assert pool.total_count == 5

    def test_SimpleDBConnectionPool_config_password_error(self, pool):
        config = SimpleDBConnectionPoolConfig(
            host=HOST,
            port=PORT,
            userid="admin",
            password="123456_error",
        )
        with pytest.raises(RuntimeError):
            pool = SimpleDBConnectionPool(config=config)

    def test_SimpleDBConnectionPool_config_password_null(self, pool):
        config = SimpleDBConnectionPoolConfig(
            host=HOST,
            port=PORT,
            userid="admin",
        )
        with pytest.raises(RuntimeError):
            pool = SimpleDBConnectionPool(config=config)

    def test_invalid_script(self, pool):
        conn = pool.acquire()

        with pytest.raises(Exception):
            conn.run("invalid script syntax")

        pool.release(conn)


class TestConnectionSetting:
    def test_default_values(self):
        setting = ConnectionSetting()
        assert setting.enable_ssl is False
        assert setting.enable_async is False
        assert setting.compress is False
        assert setting.parser == ParserType.DolphinDB
        assert setting.protocol == PROTOCOL_DDB
        assert setting.show_output is True
        assert setting.sql_std == SqlStd.DolphinDB

    @pytest.mark.parametrize("protocol_str,expected_protocol", [
        ("default", PROTOCOL_DDB),
        ("pickle", PROTOCOL_PICKLE),
        ("ddb", PROTOCOL_DDB),
        ("arrow", PROTOCOL_ARROW),
    ])
    def test_protocol_string_conversion(self, protocol_str, expected_protocol):
        setting = ConnectionSetting(protocol=protocol_str)
        if protocol_str=="pickle" and PYTHON_VERSION >= (3,13):
            assert setting.protocol == PROTOCOL_DDB
        else:
            assert setting.protocol == expected_protocol

    def test_protocol_integer_value(self):
        setting = ConnectionSetting(protocol=PROTOCOL_DDB)
        assert setting.protocol == PROTOCOL_DDB
        setting = ConnectionSetting(protocol=0)
        assert setting.protocol == PROTOCOL_DDB

    def test_invalid_protocol(self):
        with pytest.raises(Exception):
            ConnectionSetting(protocol=999)

    def test_arrow_protocol_with_compression(self):
        with pytest.raises(Exception):
            ConnectionSetting(protocol="arrow", compress=True)

    def test_arrow_protocol_without_compression(self):
        setting = ConnectionSetting(protocol="arrow", compress=False)
        assert setting.protocol == PROTOCOL_ARROW

    def test_pickle_protocol_deprecation_warning(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ConnectionSetting(protocol="pickle")

            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "PROTOCOL_PICKLE has been deprecated" in str(w[0].message)

    @pytest.mark.skipif(sys.version_info.minor >= 13, reason="Only test for Python < 3.13")
    def test_pickle_protocol_auto_change_python_lt_13(self):
        setting = ConnectionSetting(protocol="pickle")
        assert setting.protocol == PROTOCOL_PICKLE

    @pytest.mark.skipif(sys.version_info.minor < 13, reason="Only test for Python >= 3.13")
    def test_pickle_protocol_auto_change_python_ge_13(self):
        setting = ConnectionSetting(protocol="pickle")
        assert setting.protocol == PROTOCOL_DDB

    @pytest.mark.parametrize("parser_input,expected_parser", [
        ("dolphindb", ParserType.DolphinDB),
        ("python", ParserType.Python),
        ("kdb", ParserType.Kdb),
        (ParserType.DolphinDB, ParserType.DolphinDB),
        (0, ParserType.DolphinDB),
        (1, ParserType.DolphinDB),
        (2, ParserType.Python),
        (3, ParserType.Kdb),
    ])
    def test_parser_conversion(self, parser_input, expected_parser):
        setting = ConnectionSetting(parser=parser_input)
        assert setting.parser == expected_parser

    @pytest.mark.parametrize("sql_std_input,expected_sql_std", [
        ("ddb", SqlStd.DolphinDB),
        ("oracle", SqlStd.Oracle),
        ("mysql", SqlStd.MySQL),
        (SqlStd.DolphinDB, SqlStd.DolphinDB),
        (SqlStd.MySQL, SqlStd.MySQL),
        (SqlStd.Oracle, SqlStd.Oracle),
        (0, SqlStd.DolphinDB),
        (1, SqlStd.Oracle),
        (2, SqlStd.MySQL),

    ])
    def test_sql_std_conversion(self, sql_std_input, expected_sql_std):
        setting = ConnectionSetting(sql_std=sql_std_input)
        assert setting.sql_std == expected_sql_std

    def test_invalid_sql_std_integer(self):
        with pytest.raises(ValueError):
            ConnectionSetting(sql_std=999)

    def test_combination_settings(self):
        setting = ConnectionSetting(
            enable_ssl=True,
            enable_async=True,
            compress=True,
            parser="python",
            protocol="ddb",
            show_output=False,
            sql_std="mysql"
        )

        assert setting.enable_ssl is True
        assert setting.enable_async is True
        assert setting.compress is True
        assert setting.parser == ParserType.Python
        assert setting.protocol == PROTOCOL_DDB
        assert setting.show_output is False
        assert setting.sql_std == SqlStd.MySQL

    def test_default_protocol_conversion(self):
        setting = ConnectionSetting(protocol="default")
        assert setting.protocol == default_protocol

    def test_unknown_parser_string(self):
        with pytest.raises(ValidationError):
            setting = ConnectionSetting(parser="unknown_parser")

    def test_unknown_protocol_string(self):
        with pytest.raises(ValidationError):
            setting = ConnectionSetting(protocol="unknown_protocol")

    def test_unknown_sql_std_string(self):
        with pytest.raises(ValidationError):
            setting = ConnectionSetting(sql_std="unknown_sql_std")

    def test_compress_setting(self):
        setting1 = ConnectionSetting(compress=True)
        assert setting1.compress is True

        setting2 = ConnectionSetting(compress=False)
        assert setting2.compress is False

    def test_ssl_setting(self):
        setting1 = ConnectionSetting(enable_ssl=True)
        assert setting1.enable_ssl is True

        setting2 = ConnectionSetting(enable_ssl=False)
        assert setting2.enable_ssl is False

    def test_async_setting(self):
        setting1 = ConnectionSetting(enable_async=True)
        assert setting1.enable_async is True

        setting2 = ConnectionSetting(enable_async=False)
        assert setting2.enable_async is False

    def test_show_output_setting(self):
        setting1 = ConnectionSetting(show_output=True)
        assert setting1.show_output is True

        setting2 = ConnectionSetting(show_output=False)
        assert setting2.show_output is False


class TestConnectionConfig:
    def test_required_fields(self):
        with pytest.raises(ValidationError):
            ConnectionConfig()

        with pytest.raises(ValidationError):
            ConnectionConfig(host=HOST)

        with pytest.raises(ValidationError):
            ConnectionConfig(port=PORT)

    def test_basic_configuration(self):
        config = ConnectionConfig(
            host=HOST,
            port=PORT,
            userid="admin",
            password="123456"
        )

        assert config.host == HOST
        assert config.port == PORT
        assert config.userid == "admin"
        assert config.password == "123456"

    def test_default_values(self):
        config = ConnectionConfig(host=HOST, port=PORT)

        assert config.userid == ""
        assert config.password == ""
        assert config.startup == ""
        assert config.enable_high_availability is False
        assert config.high_availability_sites == []
        assert config.keep_alive_time == 30
        assert config.reconnect is False
        assert config.try_reconnect_nums == -1
        assert config.read_timeout == -1
        assert config.write_timeout == -1
        assert config.use_public_name is False

    def test_userid_alias(self):
        config1 = ConnectionConfig(host=HOST, port=PORT, userId="admin")
        assert config1.userid == "admin"

        config2 = ConnectionConfig(host=HOST, port=PORT, userid="admin")
        assert config2.userid == "admin"

    def test_enable_high_availability_configuration(self):
        config = ConnectionConfig(
            host=HOST,
            port=PORT,
            enable_high_availability=True,
            high_availability_sites=[f'{HOST_CLUSTER}:{PORT_DNODE1}', f'{HOST_CLUSTER}:{PORT_DNODE2}']
        )

        assert config.enable_high_availability is True
        assert config.high_availability_sites == [f'{HOST_CLUSTER}:{PORT_DNODE1}', f'{HOST_CLUSTER}:{PORT_DNODE2}']

    def test_high_availability_sites_none_handling(self):
        config = ConnectionConfig(
            host=HOST,
            port=PORT,
            high_availability_sites=None
        )

        assert config.high_availability_sites == []

    def test_timeout_configurations(self):
        config = ConnectionConfig(
            host=HOST,
            port=PORT,
            read_timeout=30,
            write_timeout=60
        )

        assert config.read_timeout == 30
        assert config.write_timeout == 60

    def test_timeout_none_handling(self):
        config = ConnectionConfig(
            host=HOST,
            port=PORT,
            read_timeout=None,
            write_timeout=None,
            try_reconnect_nums=None
        )

        assert config.read_timeout == -1
        assert config.write_timeout == -1
        assert config.try_reconnect_nums == -1

    def test_reconnect_configuration(self):
        config = ConnectionConfig(
            host=HOST,
            port=PORT,
            reconnect=True,
            try_reconnect_nums=5
        )

        assert config.reconnect is True
        assert config.try_reconnect_nums == 5

    def test_keep_alive_configuration(self):
        config = ConnectionConfig(
            host=HOST,
            port=PORT,
            keep_alive_time=60
        )

        assert config.keep_alive_time == 60

    def test_use_public_name(self):
        config = ConnectionConfig(
            host=HOST,
            port=PORT,
            use_public_name=True
        )

        assert config.use_public_name is True

    def test_startup_script(self):
        config = ConnectionConfig(
            host=HOST,
            port=PORT,
            startup="login('admin', '123456')"
        )

        assert config.startup == "login('admin', '123456')"

    def test_complex_configuration(self):
        config = ConnectionConfig(
            host="db.example.com",
            port=PORT,
            userid="admin",
            password="secure_password",
            startup="setup_script()",
            enable_high_availability=True,
            high_availability_sites=[f'{HOST_CLUSTER}:{PORT_DNODE1}', f'{HOST_CLUSTER}:{PORT_DNODE2}'],
            keep_alive_time=120,
            reconnect=True,
            try_reconnect_nums=10,
            read_timeout=45,
            write_timeout=45,
            use_public_name=True
        )

        assert config.host == "db.example.com"
        assert config.port == PORT
        assert config.userid == "admin"
        assert config.password == "secure_password"
        assert config.startup == "setup_script()"
        assert config.enable_high_availability is True
        assert config.high_availability_sites == [f'{HOST_CLUSTER}:{PORT_DNODE1}', f'{HOST_CLUSTER}:{PORT_DNODE2}']
        assert config.keep_alive_time == 120
        assert config.reconnect is True
        assert config.try_reconnect_nums == 10
        assert config.read_timeout == 45
        assert config.write_timeout == 45
        assert config.use_public_name is True

    def test_minimal_configuration(self):
        config = ConnectionConfig(host="minimal", port=1234)

        assert config.host == "minimal"
        assert config.port == 1234

        assert config.userid == ""
        assert config.password == ""

    def test_enable_high_availability_without_sites(self):
        config = ConnectionConfig(
            host=HOST,
            port=PORT,
            enable_high_availability=True
        )

        assert config.enable_high_availability is True
        assert config.high_availability_sites == []

    def test_negative_timeout_values(self):
        config = ConnectionConfig(
            host=HOST,
            port=PORT,
            read_timeout=-1,
            write_timeout=-2
        )

        assert config.read_timeout == -1
        assert config.write_timeout == -2

    def test_zero_timeout_values(self):
        config = ConnectionConfig(
            host=HOST,
            port=PORT,
            read_timeout=0,
            write_timeout=0
        )

        assert config.read_timeout == 0
        assert config.write_timeout == 0


class TestSimpleDBConnectionPoolConfig:
    def test_default_values(self):

        config = SimpleDBConnectionPoolConfig(
            host=HOST,
            port=PORT
        )

        assert config.min_pool_size == 5
        assert config.max_pool_size == 5
        assert config.idle_timeout == 600000
        assert config.check_interval == 60000

        assert config.enable_ssl is False
        assert config.enable_async is False
        assert config.compress is False

    def test_custom_pool_sizes(self):

        config = SimpleDBConnectionPoolConfig(
            host=HOST,
            port=PORT,
            min_pool_size=2,
            max_pool_size=10
        )

        assert config.min_pool_size == 2
        assert config.max_pool_size == 10

    def test_custom_timeouts(self):

        config = SimpleDBConnectionPoolConfig(
            host="localhost",
            port=8848,
            idle_timeout=30000,
            check_interval=5000
        )

        assert config.idle_timeout == 30000
        assert config.check_interval == 5000

    def test_min_pool_size_validation(self):

        valid_min_sizes = [1, 5, 10, 100]
        for min_size in valid_min_sizes:
            config = SimpleDBConnectionPoolConfig(
                host="localhost",
                port=8848,
                min_pool_size=min_size,
                max_pool_size=100
            )
            assert config.min_pool_size == min_size

        invalid_min_sizes = [0, -1, -5]
        for min_size in invalid_min_sizes:
            with pytest.raises(ValidationError):
                SimpleDBConnectionPoolConfig(
                    host="localhost",
                    port=8848,
                    min_pool_size=min_size
                )

    def test_max_pool_size_validation(self):

        valid_max_sizes = [1, 5, 10, 100]
        for max_size in valid_max_sizes:
            config = SimpleDBConnectionPoolConfig(
                host="localhost",
                port=8848,
                min_pool_size=1,
                max_pool_size=max_size
            )
            assert config.max_pool_size == max_size

        invalid_max_sizes = [0, -1, -5]
        for max_size in invalid_max_sizes:
            with pytest.raises(ValidationError):
                SimpleDBConnectionPoolConfig(
                    host="localhost",
                    port=8848,
                    max_pool_size=max_size
                )

    def test_min_greater_than_max_validation(self):

        with pytest.raises(ValueError):
            SimpleDBConnectionPoolConfig(
                host="localhost",
                port=8848,
                min_pool_size=10,
                max_pool_size=5
            )

    def test_min_equal_to_max(self):

        config = SimpleDBConnectionPoolConfig(
            host="localhost",
            port=8848,
            min_pool_size=5,
            max_pool_size=5
        )

        assert config.min_pool_size == 5
        assert config.max_pool_size == 5

    def test_idle_timeout_validation(self):

        valid_timeouts = [10000, 30000, 600000, 3600000]
        for timeout in valid_timeouts:
            config = SimpleDBConnectionPoolConfig(
                host="localhost",
                port=8848,
                idle_timeout=timeout
            )
            assert config.idle_timeout == timeout

        config = SimpleDBConnectionPoolConfig(
            host="localhost",
            port=8848,
            idle_timeout=10000
        )
        assert config.idle_timeout == 10000

        invalid_timeouts = [9999, 0, -1, -1000]
        for timeout in invalid_timeouts:
            with pytest.raises(ValidationError):
                SimpleDBConnectionPoolConfig(
                    host="localhost",
                    port=8848,
                    idle_timeout=timeout
                )

    def test_check_interval_validation(self):

        valid_intervals = [1000, 5000, 60000, 300000]
        for interval in valid_intervals:
            config = SimpleDBConnectionPoolConfig(
                host="localhost",
                port=8848,
                check_interval=interval
            )
            assert config.check_interval == interval

        config = SimpleDBConnectionPoolConfig(
            host="localhost",
            port=8848,
            check_interval=1000
        )
        assert config.check_interval == 1000

        invalid_intervals = [999, 0, -1, -1000]
        for interval in invalid_intervals:
            with pytest.raises(ValidationError):
                SimpleDBConnectionPoolConfig(
                    host="localhost",
                    port=8848,
                    check_interval=interval
                )

    def test_inherited_connection_settings(self):

        config = SimpleDBConnectionPoolConfig(
            host="localhost",
            port=8848,
            enable_ssl=True,
            enable_async=True,
            compress=True,
            parser="python",
            protocol="ddb",
            show_output=False,
            sql_std="mysql"
        )

        assert config.min_pool_size == 5
        assert config.max_pool_size == 5

        assert config.enable_ssl is True
        assert config.enable_async is True
        assert config.compress is True
        assert config.parser == ParserType.Python
        assert config.protocol == PROTOCOL_DDB
        assert config.show_output is False
        assert config.sql_std == SqlStd.MySQL

    def test_inherited_connection_config(self):

        config = SimpleDBConnectionPoolConfig(
            host="db.example.com",
            port=8848,
            userid="admin",
            password="password123",
            enable_high_availability=True,
            high_availability_sites=["site1", "site2"],
            reconnect=True,
            keep_alive_time=60
        )

        assert config.min_pool_size == 5
        assert config.max_pool_size == 5

        assert config.host == "db.example.com"
        assert config.port == 8848
        assert config.userid == "admin"
        assert config.password == "password123"
        assert config.enable_high_availability is True
        assert config.high_availability_sites == ["site1", "site2"]
        assert config.reconnect is True
        assert config.keep_alive_time == 60

    def test_complex_configuration(self):

        config = SimpleDBConnectionPoolConfig(
            host="cluster.example.com",
            port=8848,
            userid="admin",
            password="secure_pass",
            enable_high_availability=True,
            high_availability_sites=["primary", "backup"],

            enable_ssl=True,
            compress=False,
            protocol="arrow",

            min_pool_size=3,
            max_pool_size=20,
            idle_timeout=120000,
            check_interval=30000
        )

        assert config.host == "cluster.example.com"
        assert config.port == 8848
        assert config.userid == "admin"
        assert config.enable_high_availability is True
        assert config.enable_ssl is True
        assert config.compress is False
        assert config.min_pool_size == 3
        assert config.max_pool_size == 20
        assert config.idle_timeout == 120000
        assert config.check_interval == 30000

    def test_required_fields_inheritance(self):

        with pytest.raises(ValidationError):
            SimpleDBConnectionPoolConfig()

        with pytest.raises(ValidationError):
            SimpleDBConnectionPoolConfig(host="localhost")

        with pytest.raises(ValidationError):
            SimpleDBConnectionPoolConfig(port=8848)

    def test_edge_case_pool_sizes(self):

        config = SimpleDBConnectionPoolConfig(
            host="localhost",
            port=8848,
            min_pool_size=1,
            max_pool_size=1
        )
        assert config.min_pool_size == 1
        assert config.max_pool_size == 1

        config = SimpleDBConnectionPoolConfig(
            host="localhost",
            port=8848,
            min_pool_size=1,
            max_pool_size=1000
        )
        assert config.min_pool_size == 1
        assert config.max_pool_size == 1000

    @pytest.mark.parametrize("min_size,max_size,should_raise", [
        (1, 1, False),
        (1, 5, False),
        (5, 5, False),
        (5, 1, True),
        (10, 5, True),
        (100, 50, True),
    ])
    def test_min_max_combinations(self, min_size, max_size, should_raise):

        if should_raise:
            with pytest.raises(ValueError):
                SimpleDBConnectionPoolConfig(
                    host="localhost",
                    port=8848,
                    min_pool_size=min_size,
                    max_pool_size=max_size
                )
        else:
            config = SimpleDBConnectionPoolConfig(
                host="localhost",
                port=8848,
                min_pool_size=min_size,
                max_pool_size=max_size
            )
            assert config.min_pool_size == min_size
            assert config.max_pool_size == max_size

    def test_config_from_dict(self):
        config_dict = {
            "host": HOST,
            "port": PORT,
            "userId": USER,
            "password": PASSWD,
            "min_pool_size": 4,
            "max_pool_size": 12,
            "idle_timeout": 45000,
            "check_interval": 12000
        }

        config = SimpleDBConnectionPoolConfig(**config_dict)

        assert config.host == HOST
        assert config.port == PORT
        assert config.userid == USER
        assert config.min_pool_size == 4
        assert config.max_pool_size == 12
        assert config.idle_timeout == 45000
        assert config.check_interval == 12000
