# from memory_profiler import profile
import dolphindb as ddb
from dolphindb import tableAppender,TableUpserter,PartitionedTableAppender,MultithreadedTableWriter,DBConnectionPool
import random
import pandas as pd
import numpy as np
import time
import dolphindb.settings as keys
import decimal
import gc
import sys, datetime
import warnings

warnings.filterwarnings("ignore")


RUN_TIME = int(sys.argv[1])


# @profile
def tableAppender_task(config, db, tab):
    for _ in range(RUN_TIME):
        internalConn = ddb.session(*config, enablePickle=False)
        appender = tableAppender(db, tab, internalConn)
        df_to_append = get_random_df(100)
        appender.append(df_to_append)
        internalConn.close()
    del appender, df_to_append, internalConn


# @profile
def tableUpserter_task(config, db, tab):
    for _ in range(RUN_TIME):
        internalConn = ddb.session(*config, enablePickle=False)
        up = TableUpserter(db, tab, internalConn, random.randint(0,1), ['cdate', 'cint'])
        df_to_append = get_random_df(100)
        up.upsert(df_to_append)
        internalConn.close()
    del up,df_to_append,internalConn

# @profile
def mtw_task(config, db, tab):
    for _ in range(RUN_TIME):
        mtwriter = MultithreadedTableWriter(*config, db, tab, threadCount=5, partitionCol='cint')
        rawdata = get_random_df(100).values
        for row in rawdata:
            err = mtwriter.insert(*row)
            if err.hasError():
                print(err)
                break
        mtwriter.waitForThreadCompletion()
    del mtwriter,rawdata

# @profile
def pta_task(config, db, tab):
    for _ in range(RUN_TIME):
        pool= DBConnectionPool(config[0], config[1], 10, config[2], config[3])
        pta = PartitionedTableAppender(db, tab, 'cint', pool)
        df_to_append = get_random_df(100)
        pta.append(df_to_append)
        pool.shutDown()
    del pta,df_to_append,pool


# @profile
def session_run_task(config, db, tab):
    for _ in range(RUN_TIME):
        internalConn= ddb.session(*config, enablePickle=False)
        df_to_append = get_random_df(100)
        internalConn.run(f"append!{{loadTable('{db}', `{tab}),}}", df_to_append)
        res = internalConn.run(f"select * from loadTable('{db}', `{tab})")
        internalConn.close()
    del res,df_to_append, internalConn

# @profile
def session_upload_task(config):
    for _ in range(RUN_TIME):
        internalConn= ddb.session(*config, enablePickle=False)
        df_to_append = get_random_df(100)
        internalConn.upload({"df":df_to_append})
        assert internalConn.run("`df in objs().name")
        internalConn.close()
    del df_to_append,internalConn


def get_random_df(n):
    df = pd.DataFrame({
        'cbool': np.array([random.sample([True, False],1)[0] for _ in range(n)], dtype=np.bool_),
        'cchar': np.array([random.sample([1, -1], 1)[0] for _ in range(n)], dtype=np.int8),
        'cshort': np.array([random.sample([-10, 1000], 1)[0] for _ in range(n)], dtype=np.int16),
        'cint': np.array([random.sample([1,2,3,4,5,6,7,8,9,10], 1)[0] for _ in range(n)], dtype=np.int32),
        'clong': np.array([random.sample([-100000000, 10000000000], 1)[0] for _ in range(n)], dtype=np.int64),
        'cdate': np.array([random.sample(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], 1)[0] for _ in range(n)], dtype="datetime64[D]"),
        'ctime': np.array([random.sample(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], 1)[0] for _ in range(n)], dtype="datetime64[ms]"),
        'cminute': np.array([random.sample(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], 1)[0] for _ in range(n)], dtype="datetime64[m]"),
        'csecond': np.array([random.sample(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], 1)[0] for _ in range(n)], dtype="datetime64[s]"),
        'cdatetime': np.array([random.sample(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], 1)[0] for _ in range(n)], dtype="datetime64[s]"),
        'cdatehour': np.array([random.sample(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], 1)[0] for _ in range(n)], dtype="datetime64[h]"),
        'ctimestamp': np.array([random.sample(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], 1)[0] for _ in range(n)], dtype="datetime64[ms]"),
        'cnanotime': np.array([random.sample(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], 1)[0] for _ in range(n)], dtype="datetime64[ns]"),
        'cnanotimestamp': np.array([random.sample(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], 1)[0] for _ in range(n)], dtype="datetime64[ns]"),
        'cfloat': np.array([random.sample([2.2134500, np.nan], 1)[0] for _ in range(n)], dtype='float32'),
        'cdouble': np.array([random.sample([3.214, np.nan], 1)[0] for _ in range(n)], dtype='float64'),
        'csymbol': np.array([random.sample(['sym1','sym2' ], 1)[0] for _ in range(n)], dtype='object'),
        'cstring': np.array([random.sample(['str1', 'str2'], 1)[0] for _ in range(n)], dtype='object'),
        'cipaddr': np.array([random.sample(["192.168.1.1", "0.0.0.0"], 1)[0] for _ in range(n)], dtype='object'),
        'cuuid': np.array([random.sample(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"], 1)[0] for _ in range(n)], dtype='object'),
        'cint128': np.array([random.sample(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"], 1)[0] for _ in range(n)], dtype='object'),
        'cblob': np.array([random.sample(['blob1', 'blob2'], 1)[0] for _ in range(n)], dtype='object'),
        'cdecimal32': np.array([random.sample([decimal.Decimal("0.00000000"), decimal.Decimal("-1.500")], 1)[0] for _ in range(n)], dtype='object'),
        'cdecimal64': np.array([random.sample([decimal.Decimal("0.000000000000"), decimal.Decimal("NaN")], 1)[0] for _ in range(n)], dtype='object')
    })
    df.__DolphinDB_Type__ = {
        'cbool': keys.DT_BOOL,
        'cchar': keys.DT_CHAR,
        'cshort': keys.DT_SHORT,
        'cint': keys.DT_INT,
        'clong': keys.DT_LONG,
        'cdate': keys.DT_DATE,
        'cmonth': keys.DT_MONTH,
        'ctime': keys.DT_TIME,
        'cminute': keys.DT_MINUTE,
        'csecond': keys.DT_SECOND,
        'cdatetime': keys.DT_DATETIME,
        'ctimestamp': keys.DT_TIMESTAMP,
        'cnanotime': keys.DT_NANOTIME,
        'cnanotimestamp': keys.DT_NANOTIMESTAMP,
        'cfloat': keys.DT_FLOAT,
        'cdouble': keys.DT_DOUBLE,
        'csymbol': keys.DT_SYMBOL,
        'cstring': keys.DT_STRING,
        'cipaddr': keys.DT_IPADDR,
        'cuuid': keys.DT_UUID,
        'cint128': keys.DT_INT128,
        'cblob': keys.DT_BLOB,
        'cdecimal32':keys.DT_DECIMAL32,
        'cdecimal64':keys.DT_DECIMAL64
    }

    return df




if __name__ == '__main__':
    PARAMS = ["192.168.100.9", 13812, "admin", "123456"]
    conn = ddb.session(*PARAMS, enablePickle=False)
    db = "dfs://test_db"
    tab = "test_tab"
    conn.run(f"""
        dbPath = '{db}'
        tab = '{tab}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:1, `cbool`cchar`cshort`cint`clong`cdate`ctime`cminute`csecond`cdatetime`cdatehour`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cipaddr`cuuid`cint128`cblob`cdecimal32`cdecimal64, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB,DECIMAL32(9),DECIMAL64(12)])
        db=database(dbPath,RANGE,1..20,,'TSDB')
        pt = db.createPartitionedTable(t, tab, `cint,,`cint)
    """)
    conn.close()
    conn = None

    gc.collect()
    time.sleep(2)

    print(datetime.datetime.now(), "tableAppender接口测试开始...")
    tableAppender_task(PARAMS, db, tab)
    gc.collect()
    time.sleep(1)

    print(datetime.datetime.now(), "tableUpserter接口测试开始...")
    tableUpserter_task(PARAMS, db, tab)
    gc.collect()
    time.sleep(1)

    print(datetime.datetime.now(), "MultithreadedTableWriter接口测试开始...")
    mtw_task(PARAMS, db, tab)
    gc.collect()
    time.sleep(1)

    print(datetime.datetime.now(), "PartitionedTableAppender接口测试开始...")
    pta_task(PARAMS, db, tab)
    gc.collect()
    time.sleep(1)

    print(datetime.datetime.now(), "session run下载测试开始...")
    session_run_task(PARAMS, db, tab)
    gc.collect()
    time.sleep(1)

    print(datetime.datetime.now(), "session upload上传测试开始...")
    session_upload_task(PARAMS)
    gc.collect()
    time.sleep(1)
