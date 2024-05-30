import sys
from dolphindb.settings import DF_SCALAR, DF_VECTOR
from dolphindb.settings import DBNAN, ARRAY_TYPE_BASE
from dolphindb.settings import DT_BOOL, DT_CHAR, DT_SHORT, DT_INT, DT_LONG
from dolphindb.settings import DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATEHOUR
from dolphindb.settings import DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP
from dolphindb.settings import DT_FLOAT, DT_DOUBLE
from dolphindb.settings import DT_SYMBOL, DT_STRING, DT_BLOB, DT_UUID, DT_INT128, DT_IPADDR
from dolphindb.settings import DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128
from dolphindb.settings import getCategory, DATA_CATEGORY


_SUPPORT_DATA_FORM_DICT = {
    "Scalar": DF_SCALAR,
    "Vector": DF_VECTOR,
}

_SUPPORT_DATA_TYPE_SCALAR = [
    DT_BOOL, DT_CHAR, DT_SHORT, DT_INT, DT_LONG,
    DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND,
    DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR,
    DT_FLOAT, DT_DOUBLE,
    DT_STRING, DT_BLOB, DT_UUID, DT_INT128, DT_IPADDR,
    DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128,
]

_SUPPORT_DATA_TYPE_VECTOR = [
    DT_BOOL, DT_CHAR, DT_SHORT, DT_INT, DT_LONG,
    DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND,
    DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR,
    DT_FLOAT, DT_DOUBLE,
    DT_SYMBOL, DT_STRING, DT_BLOB, DT_UUID, DT_INT128, DT_IPADDR,
    DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128,
]


__all__ = [
    "_DATA_FORM",
    "Scalar",
    "Vector",
]


def _check_python_above_36():
    return sys.version_info.major >= 3 and sys.version_info.minor >= 7


def _create_new_type_class(name, data_type, exparam):
    if name not in _SUPPORT_DATA_FORM_DICT.keys():
        raise ValueError("Invalid data form.")
    if name in ["Scalar", "Vector"] and data_type >= ARRAY_TYPE_BASE:
        raise ValueError("Invalid data type. ArrayVector is not supported.")
    if name in ["Scalar", "Vector"] and \
       getCategory(data_type) == DATA_CATEGORY.DENARY and \
       exparam == DBNAN[DT_INT]:
        raise ValueError("Invalid data type. Must specify exparam for DECIMAL.")
    if name == "Scalar" and data_type not in _SUPPORT_DATA_TYPE_SCALAR:
        raise ValueError("Invalid data type.")
    if name == "Vector" and data_type not in _SUPPORT_DATA_TYPE_VECTOR:
        raise ValueError("Invalid data type.")
    return _DATA_FORM(
        name,
        (),
        {
            "_data_type": data_type,
            "_data_form": _SUPPORT_DATA_FORM_DICT[name],
            "_exparam": exparam,
        },
    )


def _parse_hint_fields(item):
    if isinstance(item, tuple):
        data_type, exparam = item
    else:
        data_type, exparam = item, DBNAN[DT_INT]
    return {
        "data_type": data_type,
        "exparam": exparam,
    }


class _MetaType(type):
    def __init__(self, *args):
        if _check_python_above_36():
            delattr(self, "__getitem__")
        super().__init__(*args)


class _DATA_FORM(type, metaclass=_MetaType):
    def __getitem__(self, item):
        kwargs = _parse_hint_fields(item)
        return _create_new_type_class(self.__name__, **kwargs)


class _TYPE_IMPL:
    def __class_getitem__(cls, item):
        kwargs = _parse_hint_fields(item)
        return _create_new_type_class(cls.__name__, **kwargs)


class Scalar(_TYPE_IMPL, metaclass=_DATA_FORM):
    pass


class Vector(_TYPE_IMPL, metaclass=_DATA_FORM):
    pass
