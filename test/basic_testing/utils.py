import math
import re
from decimal import Decimal
from time import sleep

import numpy as np
import pandas as pd


def equalPlus(dataA, dataB):
    """
    bool/int/str/None/Decimal:==
    float:nan==nan
    list/tuple/set:for recursion
    dict:for recursion
    np.datetime64:nat==nat,dtype==dtype,value==value
    np.array:for recursion
    pd.DataFrame|pd.Series:
    """
    if not isinstance(dataA, dataB.__class__):
        return False
    if isinstance(dataA, (bool, int, str, bytes)) or dataA is None:
        return dataA == dataB
    elif isinstance(dataA, Decimal):
        return str(dataA) == str(dataB)
    elif isinstance(dataA, float):
        if math.isnan(dataA):
            return math.isnan(dataB)
        elif math.isinf(dataA):
            return math.isinf(dataB)
        else:
            return abs(dataA - dataB) < 0.000001
    elif isinstance(dataA, (list, tuple)):
        if len(dataA) != len(dataB):
            return False
        for _dataA, _dataB in zip(dataA, dataB):
            if not equalPlus(_dataA, _dataB):
                return False
        else:
            return True
    elif isinstance(dataA, set):
        if len(dataA) != len(dataB):
            return False
        for i in dataA:
            if i not in dataB:
                # nan
                if isinstance(i, float) and math.isnan(i):
                    x = False
                    for _i in dataB:
                        if isinstance(_i, float) and math.isnan(_i):
                            x = True
                    else:
                        if x:
                            continue
                        else:
                            return False
                return False
        else:
            return True
    elif isinstance(dataA, dict):
        if len(dataA) != len(dataB):
            return False
        else:
            for k in dataA:
                if k not in dataB:
                    return False
                elif not equalPlus(dataA[k], dataB[k]):
                    return False
            else:
                return True
    elif isinstance(dataA, (np.bool_, np.int8, np.int16, np.int32, np.int64)):
        return np.equal(dataA, dataB)
    elif isinstance(dataA, (np.float32, np.float64)):
        if np.isnan(dataA):
            return np.isnan(dataB)
        else:
            return dataA == dataB
    elif isinstance(dataA, np.datetime64):
        if np.isnat(dataA):
            return np.isnat(dataB) and dataA.dtype == dataB.dtype
        else:
            return dataA == dataB and dataA.dtype == dataB.dtype
    elif isinstance(dataA, np.ndarray):
        if dataA.shape != dataB.shape:
            return False
        for index, v in np.ndenumerate(dataA):
            if not equalPlus(dataA[index], dataB[index]):
                return False
        else:
            return True
    elif isinstance(dataA, pd.NaT.__class__):
        return isinstance(dataB, pd.NaT.__class__)
    elif isinstance(dataA, pd.Timestamp):
        return dataA == dataB
    elif isinstance(dataA, pd.Series):
        if dataA.shape != dataB.shape or dataA.dtype != dataB.dtype:
            return False
        for index, v in enumerate(dataA):
            if not equalPlus(dataA[index], dataB[index]):
                return False
        else:
            return True
    elif isinstance(dataA, pd.DataFrame):
        if dataA.shape != dataB.shape:  # or dataA.dtypes!=dataB.dtypes
            return False
        for key in dataA.columns:
            if not equalPlus(dataA[key], dataB[key]):
                return False
        else:
            return True
    else:
        raise RuntimeError(f"{type(dataA)} not support")


def assertPlus(data):
    """
    assert
    """
    if isinstance(data, (bool, int, float, np.datetime64, np.bool_)):
        assert data
    elif isinstance(data, (list, tuple)):
        for i in data:
            assertPlus(i)
    elif isinstance(data, dict):
        for v in data.values():
            assertPlus(v)
    elif isinstance(data, np.ndarray):
        assert data.all()
    elif isinstance(data, pd.Series):
        for i in data.values:
            assertPlus(i)
    elif isinstance(data, pd.DataFrame):
        for key in data.columns:
            assertPlus(data[key])
    else:
        assert False, f'{type(data)} is not support'


# def operateNodes(conn, nodes, operate):
#     conn.run("""
#         def check_nodes(state_,nodes){
#             return all(exec state==state_ from getClusterPerf(true) where name in nodes)
#         }
#     """)
#     while True:
#         if operate.upper() == "STOP":
#             conn.run("stopDataNode", nodes)
#             result = conn.run("check_nodes", 0, nodes)
#         else:
#             conn.run("startDataNode", nodes)
#             result = conn.run("check_nodes", 1, nodes)
#         if result:
#             break
#         else:
#             sleep(0.5)

def operateNodes(conn, nodes, operate):
    conn.run("""
        def check_nodes(state_,nodes){
            return all(exec state==state_ from rpc(getControllerAlias(),getClusterPerf) where name in nodes)
        }
        def check_nodes_start(nodes){
            for (i in nodes){
                do {
                    try{
                        startDataNode(nodes)
                    }catch(ex){}
                    try {
                        result = rpc(i,check_nodes,1,[i])
                    } catch (ex) {
                        sleep(500)
                        continue
                    }
                    if (result) {
                        break
                    } else {
                        sleep(500)
                        continue
                    }
                } while (true);
            }
        }
    """)
    if operate.upper() == "STOP":
        while True:
            try:
                conn.run("stopDataNode", nodes)
            except Exception:
                pass
            result = conn.run("check_nodes", 0, nodes)
            if result:
                break
            else:
                sleep(0.5)
    else:
        conn.run("check_nodes_start", nodes)
    sleep(3)


def get_dolphindb_version(session):
    version_str = session.run("version()")
    match = re.search(r'(\d+)\.(\d+)\.(\d+)', version_str)
    if match:
        return tuple(map(int, match.groups()))
    return (0, 0, 0)
