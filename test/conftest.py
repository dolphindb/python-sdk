import platform

import dolphindb as ddb
import pytest
from _pytest.outcomes import Skipped

from basic_testing.prepare import PYTHON_VERSION, FREE_THREADING
from setup.settings import HOST_REPORT, PORT_REPORT, USER_REPORT, PASSWD_REPORT, REPORT


if REPORT:
    conn = ddb.Session(HOST_REPORT, PORT_REPORT, USER_REPORT, PASSWD_REPORT)

    DATABASE_NAME = "dfs://py_report"
    TABLE_NAME = f"{platform.system()}_{platform.machine()}_{PYTHON_VERSION[0]}{PYTHON_VERSION[1]}{'t' if FREE_THREADING else ''}".lower()
    SHARE_TABLE = f"py_{platform.system()}_{platform.machine()}_{PYTHON_VERSION[0]}{PYTHON_VERSION[1]}{'t' if FREE_THREADING else ''}".lower()
    SHARE_DICT = f"py_{platform.system()}_{platform.machine()}_{PYTHON_VERSION[0]}{PYTHON_VERSION[1]}{'t' if FREE_THREADING else ''}_dict".lower()


    @pytest.fixture(scope="session", autouse=True)
    def create_share_table(request):
        worker_id = getattr(request.config, 'workerinput', {}).get('workerid',"gw0").lower()
        worker_count = getattr(request.config, 'workerinput', {}).get('workercount', 1)
        conn.run(
            f"share table([]$STRING as worker_id,[]$STRING as case_name,[]$STRING as case_result,[]$BLOB as case_info,[]$BLOB as case_trace,[]$DOUBLE as case_time,[]$TIMESTAMP as run_time) as {SHARE_TABLE}_{worker_id}",
            priority=1)
        if worker_id == "gw0":
            conn.run(f"""
                if (not existsDatabase('{DATABASE_NAME}')){{
                    partitionScheme_=[]$STRING
                    for (i in 0..63){{
                        partitionScheme_.append!("gw"+string(i))
                    }}
                    database('{DATABASE_NAME}',VALUE,partitionScheme_,engine='TSDB')
                }}
                db=database('{DATABASE_NAME}')
                if (not existsTable('{DATABASE_NAME}','{TABLE_NAME}')){{
                    db.createPartitionedTable({SHARE_TABLE}_{worker_id},'{TABLE_NAME}','worker_id',sortColumns='case_name')
                }}
                truncate('{DATABASE_NAME}','{TABLE_NAME}')
                syncDict(STRING,BOOL,`{SHARE_DICT})
                go
                {SHARE_DICT}[`{worker_id}]=00b
            """, priority=1)
        else:
            conn.run(f"""
                do {{
                    if ("{SHARE_DICT}" in objs(true)['name']){{
                        break
                    }} else {{
                        sleep(500)
                    }}
                }}while(true);
            """, priority=4)
            conn.run(f"{SHARE_DICT}[`{worker_id}]=00b", priority=1)
        yield
        if worker_id == 'gw0':
            conn.run(f"""
                {SHARE_DICT}[`{worker_id}]=false
                do {{
                    sleep(500)
                }} while({SHARE_DICT}.size()!={worker_count});
                do {{
                    keys_={SHARE_DICT}.keys()
                    values_={SHARE_DICT}.values()
                    if (all(values_==true)){{
                        break
                    }}
                    for (key in keys_){{
                        check_ = {SHARE_DICT}[key]
                        if (isNull(check_)){{
                            continue
                        }} else if (!check_){{
                            t=loadTable('{DATABASE_NAME}','{TABLE_NAME}')
                            t.append!(objByName(`{SHARE_TABLE}_ + key))
                            undef(`{SHARE_TABLE}_ + key,SHARED)
                            {SHARE_DICT}[key]=true
                        }} else {{
                            continue
                        }}
                    }}
                    sleep(500)
                }}while(true);
                undef(`{SHARE_DICT},SHARED)
            """, priority=1)
        else:
            conn.run(f"{SHARE_DICT}[`{worker_id}]=false", priority=1)


    def pytest_runtest_makereport(item, call):
        worker_id = getattr(item.config, 'workerinput', {}).get('workerid', "gw0").lower()
        if call.when == "setup":
            if hasattr(call.excinfo, 'type') and call.excinfo.type is Skipped:
                conn.run(
                    f"insert into {SHARE_TABLE}_{worker_id} values(\"{worker_id}\",\"{item.name}\",\"skipped\",\"\",\"\",0,now())",
                    priority=1)
            else:
                conn.run(
                    f"insert into {SHARE_TABLE}_{worker_id} values(\"{worker_id}\",\"{item.name}\",\"running\",\"\",\"\",0,now())",
                    priority=1)
        elif call.when == "call":
            if call.excinfo is None:
                conn.run(
                    f"update {SHARE_TABLE}_{worker_id} set case_result=\"success\",case_time={call.duration} where case_name=\"{item.name}\"",
                    priority=1)
            elif call.excinfo.type is Skipped:
                conn.run(
                    f"update {SHARE_TABLE}_{worker_id} set case_result=\"skipped\" where case_name=\"{item.name}\"",
                    priority=1)
            else:
                case_info = f'{call.excinfo.value}'.replace('"', "'")
                case_trace = f'{call.excinfo.traceback}'.replace('"', "'").replace('\\', '\\\\')
                conn.run(
                    f"update {SHARE_TABLE}_{worker_id} set case_result=\"fail\",case_time={call.duration},case_info=\"{case_info}\",case_trace=\"{case_trace}\" where case_name=\"{item.name}\"",
                    priority=1)
