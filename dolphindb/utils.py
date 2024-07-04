import uuid

from typing import List, Union


def _generate_tablename(tableName=None):
    if tableName is None:
        return "TMP_TBL_" + uuid.uuid4().hex[:8]
    else:
        return tableName + "_TMP_TBL_" + uuid.uuid4().hex[:8]


def _generate_dbname():
    return "TMP_DB_" + uuid.uuid4().hex[:8]+"DB"


def _getFuncName(f):
    if isinstance(f, str):
        return f
    else:
        return f.__name__


def _isVariableCandidate(word: str) -> bool:
    if len(word) < 1:
        raise RuntimeError("The column name cannot be empty.")
    cur = word[0]
    if not cur.isalpha():
        return False
    for cur in word:
        cur: str
        if not cur.isalpha() and not cur.isdigit() and cur != '_':
            return False
    return True


def __raise_or_not(errMsg: str = ""):
    if errMsg != "":
        raise TypeError(errMsg)


def _convertToConstant(value: str, errMsg: str = ""):
    if not isinstance(value, str):
        __raise_or_not(errMsg)
    return str(value)


def _convertToString(value: str, errMsg: str = ""):
    if not isinstance(value, str):
        __raise_or_not(errMsg)
    return f"`{value}"


def _convertToStringVector(value: List[str], errMsg: str = ""):
    if not isinstance(value, list):
        __raise_or_not(errMsg)
    if len(value) == 0:
        return "string([])"
    return "`" + "`".join(value)


def _convertToStringOrStringVector(value: Union[str, List[str]], errMsg: str = ""):
    if isinstance(value, str):
        return _convertToString(value)
    elif isinstance(value, list):
        return _convertToStringVector(value)
    __raise_or_not(errMsg)


def _convertToDict(value: dict, keyConverter: callable, valConverter: callable, errMsg: str = ""):
    if not isinstance(value, dict):
        __raise_or_not(errMsg)
    items = []
    for key, val in value.items():
        items.append(f"{keyConverter(key, errMsg=errMsg)}:{valConverter(val, errMsg=errMsg)}")
    return "{" + ",".join(items) + "}"


def _convertToBool(value: bool, errMsg: str = ""):
    if not isinstance(value, bool):
        __raise_or_not(errMsg)
    return str(bool(value)).lower()


class month(object):
    """Temporal type - Month

    Args:
        year : Year number of type month
        month : Month number of type month
    """
    def __init__(self, year: int, month: int):
        self.__year = year
        self.__month = month

    def __str__(self):
        if self.__month > 9:
            return str(self.__year) + '-' + str(self.__month) + 'M'
        return str(self.__year) + '-0' + str(self.__month) + 'M'
