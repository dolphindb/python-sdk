import uuid
import warnings
import inspect
from inspect import Signature

import pandas as pd

from packaging.version import Version

from ._hints import List, Union, Dict, Callable
from .settings import TYPE_STRING, ParserType


from ._core import DolphinDBRuntime
ddbcpp = DolphinDBRuntime()._ddbcpp


def deprecated(obj=None, *, instead="", msg=""):
    if obj is None:
        return lambda o: deprecated(o, instead=instead, msg=msg)

    if inspect.isclass(obj):
        return cls_deprecated(obj, instead=instead, msg=msg)

    def wrapper(*args, **kwargs):
        warning_msg = f"The function '{obj.__name__}' is deprecated." if not msg else msg
        if instead:
            warning_msg += f" Please use '{instead}' instead."
        warnings.warn(
            warning_msg,
            DeprecationWarning, stacklevel=2,
        )
        return obj(*args, **kwargs)
    return wrapper


def cls_deprecated(cls=None, *, instead="", msg=""):
    if cls is None:
        return lambda c: cls_deprecated(c, instead=instead, msg=msg)

    orig_init = cls.__init__

    def new_init(self, *args, **kwargs):
        warning_msg = f"The class '{cls.__name__}' is deprecated." if not msg else msg
        if instead:
            warning_msg += f" Please use '{instead}' instead."
        warnings.warn(
            warning_msg,
            DeprecationWarning, stacklevel=2,
        )
        orig_init(self, *args, **kwargs)

    cls.__init__ = new_init
    return cls


def params_deprecated(func=None, /, params=None, *, reason=None):
    if func is None:
        return lambda f: params_deprecated(f, params=params, reason=reason)

    def wrapper(*args, **kwargs):
        if params:
            for param in params:
                if param in kwargs:
                    warning_msg = f"The parameter '{param}' is deprecated, please use '{params[param]}' instead."
                    if reason and param in reason:
                        warning_msg += f" {reason[param]}"
                    warnings.warn(
                        warning_msg,
                        DeprecationWarning, stacklevel=2,
                    )
        return func(*args, **kwargs)

    return wrapper


def standard_deprecated(msg: str):
    warnings.warn(
        msg,
        DeprecationWarning,
        stacklevel=2,
    )


def dispatcher(func):
    registery_signature: List[Signature] = []
    registery_functions: List[Callable] = []

    def register(reg_func: Callable):
        sig = inspect.signature(reg_func)
        registery_signature.append(sig)
        registery_functions.append(reg_func)
        return reg_func

    def wrapper_function(*args, **kwargs):
        for sig, reg_func in zip(registery_signature, registery_functions):
            try:
                sig.bind(*args, **kwargs)
                return reg_func(*args, **kwargs)
            except TypeError:
                continue
        return func(*args, **kwargs)

    wrapper_function.register = register
    return wrapper_function


def _version_check(src: str, op, tgt: str) -> bool:
    ver_src = Version(src)
    ver_tgt = Version(tgt)
    return op(ver_src, ver_tgt)


def _helper_check_parser(parser, python: bool = False):
    if parser == ParserType.Default:
        parser = ParserType.Python if python else ParserType.DolphinDB
    elif python:
        raise RuntimeError("The parameter parser must not be specified when python=true")

    if not isinstance(parser, ParserType):
        if isinstance(parser, int):
            parser = ParserType(parser)
        else:
            parser = ParserType.parse_from_str(parser)

    if parser == ParserType.Default:
        parser = ParserType.DolphinDB

    return parser


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


def get_types_from_schema(df_schema_info: pd.DataFrame) -> Dict[str, List]:
    """
    Extracts column names, types, and extra information from df_schema_info into a dictionary.

    Args:
        df_schema_info (pd.DataFrame): A pandas DataFrame containing schema information.
            It must include at least the following columns:
                - 'name': The name of the column.
                - 'typeInt': The integer representing the DolphinDB data type.
            Optionally, it can include:
                - 'extra': A number indicating the precision of DECIMAL data.

    Returns:
        dict: A dictionary where keys are column names and values are lists containing:
            - DolphinDB type as a string.
            - Optional extra value (integer) or None if not applicable.
    """
    ddb_type_dict = {}
    has_extra = 'extra' in df_schema_info.columns

    for _, row in df_schema_info.iterrows():
        name = row['name']
        typeInt = row['typeInt']
        typeString = TYPE_STRING[typeInt]

        if has_extra:
            extra = row['extra']
            if pd.notna(extra):
                try:
                    extra = int(extra)
                except (ValueError, TypeError):
                    raise ValueError(f"Invalid type for 'extra'. Expected an int, but received a value of type {type(extra)}.")
                ddb_type_dict[name] = [typeString, extra]
            else:
                ddb_type_dict[name] = [typeString, None]
        else:
            ddb_type_dict[name] = [typeString, None]

    return ddb_type_dict


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


def hash_bucket(obj, n: int) -> int:
    """Hash map, which maps DolphinDB objects into hash buckets of size n.

    Args:
        obj : the DolphinDB object to be hashed.
        n : hash bucket size.

    Returns:
        hash of the DolphinDB object.
    """
    if not isinstance(n, int) or n <= 0:
        raise ValueError("n must be a positive integer")
    return ddbcpp._util_hash_bucket(obj, n)


def convert_datetime64(datetime64_list):
    length = len(str(datetime64_list[0]))
    # date and month
    if length == 10 or length == 7:
        listStr = '['
        for dt64 in datetime64_list:
            s = str(dt64).replace('-', '.')
            if len(str(dt64)) == 7:
                s += 'M'
            listStr += s + ','
        listStr = listStr.rstrip(',')
        listStr += ']'
    else:
        listStr = 'datehour(['
        for dt64 in datetime64_list:
            s = str(dt64).replace('-', '.').replace('T', ' ')
            ldt = len(str(dt64))
            if ldt == 13:
                s += ':00:00'
            elif ldt == 16:
                s += ':00'
            listStr += s + ','
        listStr = listStr.rstrip(',')
        listStr += '])'
    return listStr


def convert_database(database_list):
    listStr = '['
    for db in database_list:
        listStr += db._getDbName()
        listStr += ','
    listStr = listStr.rstrip(',')
    listStr += ']'
    return listStr
