import sys
from threading import Thread, Event
import time
import dolphindb as ddb
import pandas as pd


def arrayVectorToTable(vec):
    cols = ['col'+str(i) for i in range(len(vec)-1)]
    df = pd.DataFrame([vec[:-1]])
    df.columns = cols
    return df


def basic_sub(host, port, table, action, target_db, target_dfs_table):
    conn = ddb.session(host, port, "admin", "123456")
    conn.enableStreaming(0)

    def handler(msg):
        s = ddb.session(host, port, "admin", "123456")
        appender = ddb.tableAppender(target_db, target_dfs_table, s)
        succeeded = False
        while not succeeded:
            try:
                appender.append(msg)
                succeeded = True
            except Exception as e:
                print("A exception was throwed out, which is \n", str(e))
                print("retrying...")
                time.sleep(0.1)
        rows = s.run("(exec count(*) from loadTable('" +
                     target_db + "','" + target_dfs_table + "'))[0]")
        print("total saved rows from subscription: ", rows)
        s.close()
    conn.subscribe(host, port,  handler, table, action, 0, True, None, True, 1)
    Event().wait()


def deserilizer_sub(host, port, table, action, target_db, target_dfs_table):
    conn = ddb.session(host, port, "admin", "123456")
    conn.enableStreaming(0)
    db1 = target_db[0]
    db2 = target_db[1]
    dfstable1 = target_dfs_table[0]
    dfstable2 = target_dfs_table[1]

    def handler(msgs):
        s = ddb.session(host, port, "admin", "123456")
        db = ""
        tab = ""
        for msg in msgs:
            if msg[-1] == "msg1":
                db = db1
                tab = dfstable1
            elif msg[-1] == "msg2":
                db = db2
                tab = dfstable2
            appender = ddb.tableAppender(db, tab, s)
            succeeded = False
            t = arrayVectorToTable(msg)
            while not succeeded:
                try:
                    appender.append(t)
                    succeeded = True
                except Exception as e:
                    print("A exception was throwed out, which is \n", str(e))
                    print("retrying...")
                    time.sleep(0.1)
            appender = None
        rows = s.run("(exec count(*) from loadTable('" +
                     db + "','" + tab + "'))[0]")
        print("total saved rows from subscription: ", rows)
        s.close()
    sdsp = ddb.streamDeserializer({
        'msg1': "t1_trades_stream8_d_py",
        'msg2': "t2_trades_stream8_d_py",
    }, session=conn)

    conn.subscribe(host, port,  handler, table,
                   action, 0, True, None, False, 1, 1, 'admin', '123456', sdsp)
    Event().wait()


if __name__ == "__main__":
    # normal
    if sys.argv[1] == 'normal':
        sub_to_host = "192.168.100.9"
        port = 13802
        tableName = "trades_stream9_py"
        action = "test_normal_sub_py"
        dfs = "dfs://test_7_normal_sub_py"
        dfs_table = "trades7"
        p1 = Thread(target=basic_sub, args=(
            sub_to_host, port, tableName, action, dfs, dfs_table))
        p1.start()
        p1.join()
    # HA
    elif sys.argv[1] == 'HA':
        sub_to_host = "192.168.100.7"
        port = 13802
        tableName = "trades_stream_HA_py"
        action = "test_HA_sub_py"
        dfs = "dfs://test_8_HA_sub_py"
        dfs_table = "trades8_HA"
        p1 = Thread(target=basic_sub, args=(
            sub_to_host, port, tableName, action, dfs, dfs_table))
        p1.start()
        p1.join()

    # deserilizer
    elif sys.argv[1] == 'd':
        sub_to_host = "192.168.100.8"
        port = 13802
        tableName = "trades_stream8_d_py"
        action = "test_d_sub_py"
        dfs = ["dfs://test_9_d_sub_py_1", "dfs://test_9_d_sub_py_2"]
        dfs_table = ["trades9", "trades9"]
        p1 = Thread(target=deserilizer_sub, args=(
            sub_to_host, port, tableName, action, dfs, dfs_table))
        p1.start()
        p1.join()
