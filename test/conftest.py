import dolphindb as ddb
import pytest
import platform
from _pytest.outcomes import Skipped

from setup.settings import HOST_REPORT, PORT_REPORT, USER_REPORT, PASSWD_REPORT, REPORT
from basic_testing.prepare import PYTHON_VERSION

if REPORT:
    conn = ddb.Session(HOST_REPORT, PORT_REPORT, USER_REPORT, PASSWD_REPORT)

    DATABASE_NAME="dfs://py_api_auto_test_report"
    TABLE_NAME=f"py_api_auto_test_report_{platform.system()}_{platform.machine()}_{PYTHON_VERSION[0]}{PYTHON_VERSION[1]}"
    SHARE_TABLE=f"py_api_auto_test_{platform.system()}_{platform.machine()}_{PYTHON_VERSION[0]}{PYTHON_VERSION[1]}"

    total_test_count = 0

    def pytest_collection_modifyitems(items):
        global total_test_count
        total_test_count = len(items)

    @pytest.fixture(scope="session", autouse=True)
    def create_share_table(request):
        worker_id = request.config.workerinput['workerid']
        if worker_id=='gw0':
            conn.run(
                f"share table([]$STRING as case_name,[]$STRING as case_result,[]$BLOB as case_info,[]$BLOB as case_trace,[]$DOUBLE as case_time) as {SHARE_TABLE}")
        else:
            conn.run(f"""
                do {{
                    if ("{SHARE_TABLE}" in objs(true)['name']){{
                        break
                    }} else {{
                        sleep(500)
                    }}
                }}while(true);
            """)
        yield
        if worker_id == 'gw0':
            global total_test_count
            conn.run(f"""
                do {{
                    size_=select count(*) from {SHARE_TABLE} where case_result!='running'
                    if (size_['count'][0]<{total_test_count}){{
                        sleep(500)
                    }} else {{
                        break
                    }}
                }}while(true);
                if (not existsDatabase('{DATABASE_NAME}')){{
                    database('{DATABASE_NAME}',VALUE,1 5 10,engine='TSDB')
                }}
                db=database('{DATABASE_NAME}')
                if (not existsTable('{DATABASE_NAME}','{TABLE_NAME}')){{
                    db.createTable({SHARE_TABLE},'{TABLE_NAME}',sortColumns='case_name')
                }}
                truncate('{DATABASE_NAME}','{TABLE_NAME}')
                t=loadTable('{DATABASE_NAME}','{TABLE_NAME}')
                t.append!({SHARE_TABLE})
                undef(`{SHARE_TABLE},SHARED)
            """)
        else:
            conn.run(f"""
                do {{
                    if ("{SHARE_TABLE}" in objs(true)['name']){{
                        sleep(500)
                    }} else {{
                        break
                    }}
                }}while(true);
            """)

    def pytest_runtest_makereport(item, call):
        if call.when == "setup":
            if hasattr(call.excinfo,'type') and call.excinfo.type is Skipped:
                conn.run(f"insert into {SHARE_TABLE} values('{item.name}','skipped','','',0)")
            else:
                conn.run(f"insert into {SHARE_TABLE} values('{item.name}','running','','',0)")
        elif call.when == "call":
            if call.excinfo is None:
                conn.run(f"update {SHARE_TABLE} set case_result='success',case_time={call.duration} where case_name='{item.name}'")
            elif call.excinfo.type is Skipped:
                conn.run(f"update {SHARE_TABLE} set case_result='skipped' where case_name='{item.name}'")
            else:
                case_info = f'{call.excinfo.value}'.replace("'", '"')
                case_trace = f'{call.excinfo.traceback}'.replace("'", '"').replace('\\','\\\\')
                conn.run(
                    f"update {SHARE_TABLE} set case_result='fail',case_time={call.duration},case_info='{case_info}',case_trace='{case_trace}' where case_name='{item.name}'")
