import dolphindb as ddb
from multiprocessing import Queue, Process
import random
import numpy as np
import pandas as pd
import os


HOSTS = ["192.168.100.9"]
PORTS = [13812]
HA_SITES = ["192.168.100.7:13812", "192.168.100.8:13812", "192.168.100.9:13812"]
USER = "admin"
PASSWD = "123456"

DATA_DB = "dfs://py_perf_data"
DATA_TB = "concurrent_query"

host = HOSTS[random.randint(0, len(HOSTS) - 1)]
port = PORTS[random.randint(0, len(PORTS) - 1)]
GLOBAL_CONN = ddb.session(host, port, USER, PASSWD)


def executeSQL(processName, sql, sqlNum, countQue, costQue):
    s = ddb.session()
    ddbIP = HOSTS[random.randint(0, len(HOSTS) - 1)]
    ddbPort = PORTS[random.randint(0, len(PORTS) - 1)]
    s.connect(host=ddbIP, port=ddbPort, userid=USER, password=PASSWD)
    print(
        f"The query process #{processName}(pid:{os.getpid()}) begins, the start time is: {pd.Timestamp.now()},"
        + "IP:'{ip}'".format(ip=ddbIP),
        flush=True,
    )
    count = 0
    cost = 0
    for _ in range(sqlNum):
        startTime = pd.Timestamp.now()
        data = s.run(sql)
        costTime = (pd.Timestamp.now() - startTime) / np.timedelta64(1, "ms")
        cost += costTime
        count += len(data)
    countQue.put(count)
    costQue.put(cost)
    s.close()



def test_concurrent_query(sql, parallel, sqlNum, dbName, tbName):
    """Query test from loadTable(dbName, tbName)

    Args:
        sql (str): sql to execute, with format para 'db' and 'tb'
        parallel (int): concurrent query process num
        sqlNum (int): query time for each process
        dbName (str): name of database which you want to test
        tbName (str): name of partitionedTable which you want to test

    Returns:
        List: rawData to insert to dataDB
    """
    countQueMax = parallel * sqlNum
    countQue = Queue()
    costQue = Queue()
    sql = sql.format(db=dbName, tb=tbName)

    print(
        "并发查询进程数是{0}，单个进程的循环查询次数是{1}，计划查询总次数是{2}。".format(parallel, sqlNum, countQueMax)
    )
    taskList = []
    for i in range(parallel):
        testTask = Process(
            target=executeSQL,
            args=(
                "Process" + str(i),
                sql,
                sqlNum,
                countQue,
                costQue,
            ),
        )
        taskList.append(testTask)
    startTime = pd.Timestamp.now()
    print("\n==============================\n开始执行并行查询的任务的时间是{0}".format(startTime))
    for p in taskList:
        p.start()
    for p in taskList:
        p.join()
    endTime = pd.Timestamp.now()
    sumCount = []
    while countQue.empty() == False:
        data = countQue.get()
        sumCount.append(data)
    sumCount = np.sum(sumCount)
    sumCost = []
    while costQue.empty() == False:
        data = costQue.get()
        sumCost.append(data)
    sumCost = np.sum(sumCost)

    avgTimeCostPerProcess = sumCost / sqlNum / parallel
    rps = sumCount / ((endTime - startTime) / np.timedelta64(1, "s"))
    qps = countQueMax / ((endTime - startTime) / np.timedelta64(1, "s"))
    avgTimeCostPerQuery = (
        (endTime - startTime) / np.timedelta64(1, "ms")
    ) / countQueMax

    print("结束执行并行查询的任务的时间是{0}".format(endTime))
    print("并发查询任务总耗时是{0}毫秒".format((endTime - startTime) / np.timedelta64(1, "ms")))
    print("总查询表行数是{0}".format(sumCount))
    print("查询平均用时是{0}毫秒".format(avgTimeCostPerProcess))
    print("并发查询性能RPS是{0} r/s".format(rps))
    print("并发查询性能QPS是{0} q/s".format(qps))
    print("每次查询的平均耗时是{0}毫秒".format(avgTimeCostPerQuery))
    return [
        ddb.__version__,
        sql,
        parallel,
        sqlNum,
        avgTimeCostPerProcess,
        rps,
        qps,
        avgTimeCostPerQuery,
    ]


def check_dataDB():
    check_dataDB = f"""
        dbpath = '{DATA_DB}'
        tb = '{DATA_TB}'
        schema = keyedTable(`version`sql, 1:0, `version`sql`processNum`queryPerProcess`avgTimeCostPerProcess`RPS`QPS`avgTimeCostPerQuery, [STRING, STRING, INT, INT, DOUBLE, DOUBLE, DOUBLE, DOUBLE])
        if (not existsDatabase(dbpath)){{
            db = database(dbpath, VALUE, [`1.30.22.3, `1.30.22.2, `1.30.21.2, `1.30.19.2])
        }}
        if (not existsTable(dbpath, tb)){{
            createPartitionedTable(db, schema, tb, `version)
        }}

        """
    GLOBAL_CONN.run(check_dataDB)


def uploadTestData(data):
    mtw = ddb.MultithreadedTableWriter(
        host,
        port,
        USER,
        PASSWD,
        DATA_DB,
        DATA_TB,
        enableHighAvailability=True,
        highAvailabilitySites=HA_SITES,
        partitionCol="version",
    )
    err = mtw.insert(*data)
    if err.hasError():
        raise RuntimeError(err.errorInfo)
    mtw.waitForThreadCompletion()
    if len(mtw.getUnwrittenData()) > 0:
        raise RuntimeError("Data upload failed")
    return


if __name__ == "__main__":
    import sys
    check_dataDB()
    # parallel 是指定并发数（并发查询进程数）
    parallel = int(sys.argv[1])
    # 单个进程执行多少次循环查询
    sqlNum = int(sys.argv[2])

    dbName = "dfs://secondFactorDB"
    tbName = "secondFactorTB"

    file_path = (
        "/hdd/hdd0/yzou/sqls.txt"
    )
    with open(file_path, "r") as file:
        lines = file.readlines()

    sqls = lines[0].split(';')
    sqls.pop()

    for sql in sqls:
        print(sql)
        row_data = test_concurrent_query(sql, parallel, sqlNum, dbName, tbName)
        uploadTestData(row_data)
