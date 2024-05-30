import uuid


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
