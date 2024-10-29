import math
from decimal import Decimal

import numpy as np
import pandas as pd


# todo:assert_frame_equal
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
    if type(dataA) != type(dataB):
        return False
    if isinstance(dataA, (bool, int, str,)) or dataA is None:
        return dataA == dataB
    elif isinstance(dataA, Decimal):
        return str(dataA) == str(dataB)
    elif isinstance(dataA, float):
        if math.isnan(dataA):
            return math.isnan(dataB)
        else:
            return dataA == dataB
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
        if dataA.shape != dataB.shape or dataA.dtype != dataB.dtype:
            return False
        for index, v in np.ndenumerate(dataA):
            if not equalPlus(dataA[index], dataB[index]):
                return False
        else:
            return True
    elif isinstance(dataA, pd._libs.tslibs.nattype.NaTType):
        return isinstance(dataB, pd._libs.tslibs.nattype.NaTType)
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
    '''
    assert
    '''
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


if __name__ == '__main__':
    # import dolphindb as ddb
    # conn = ddb.Session('192.168.0.54', 8902, 'admin', '123456', show_output=True, enablePickle=False)
    # x = conn.run("set([float('nan'),NULL])")
    x = {None, float('nan')}
    b = {None, float('nan')}
    print(equalPlus(x, b))
