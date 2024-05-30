from typing import Union, Type
from pandas import Series
from numpy import ndarray



class Vector(object):
    """Columns of a DolphinDB table.

    Args:
        name : specifies the name of the Vector. Defaults to None.
        data : data (list or series). Defaults to None.
        s : connected Session object. Defaults to None.
        tableName : the table name. Defaults to None.
    """
    def __init__(self, name: str = None, data: Union[list, Series] = None, s=None, tableName: str = None):
        """Constructor of Vector."""
        self.__name = name
        self.__tableName = tableName
        self.__session = s  # type : Session
        if isinstance(data, Series):
            self.__vec = data
        elif isinstance(data, list):
            self.__vec = Series(data)
        elif isinstance(data, ndarray):
            self.__vec = Series(data)
        else:
            self.__vec = None

    def name(self):
        return self.__name

    def tableName(self):
        return self.__tableName

    def as_series(self, useCache=False):
        if useCache is True and self.__vec is not None:
            return self.__vec
        self.__vec = Series(self.__session.run('.'.join((self.__tableName, self.__name))))
        return self.__vec

    def __str__(self):
        return self.__name

    def __lt__(self, other):
        return FilterCond(self.__name, '<', str(other))

    def __rlt__(self, other):
        return FilterCond(other, '<', str(self.__name))

    def __le__(self, other):
        return FilterCond(self.__name, '<=', str(other))

    def __rle__(self, other):
        return FilterCond(other, '<=', str(self.__name))

    def __gt__(self, other):
        return FilterCond(self.__name, '>', str(other))

    def __rgt__(self, other):
        return FilterCond(other, '>', str(self.__name))

    def __ge__(self, other):
        return FilterCond(self.__name, '>=', str(other))

    def __rge__(self, other):
        return FilterCond(other, '>=', str(self.__name))

    def __eq__(self, other):
        return FilterCond(self.__name, '==', str(other))

    def __req__(self, other):
        return FilterCond(other, '==', str(self.__name))

    def __ne__(self, other):
        return FilterCond(self.__name, '!=', str(other))

    def __rne__(self, other):
        return FilterCond(other, '!=', str(self.__name))

    def __add__(self, other):
        return FilterCond(self.__name, '+', str(other))

    def __radd__(self, other):
        return FilterCond(other, '+', str(self.__name))

    def __sub__(self, other):
        return FilterCond(self.__name, '-', str(other))

    def __rsub__(self, other):
        return FilterCond(other, '-', str(self.__name))

    def __mul__(self, other):
        return FilterCond(self.__name, '*', str(other))

    def __rmul__(self, other):
        return FilterCond(other, '*', str(self.__name))

    def __truediv__(self, other):
        return FilterCond(self.__name, '/', str(other))

    def __rtruediv__(self, other):
        return FilterCond(other, '/', str(self.__name))

    def __mod__(self, other):
        return FilterCond(self.__name, '%', str(other))

    def __rmod__(self, other):
        return FilterCond(other, '%', str(self.__name))

    def __lshift__(self, other):
        return FilterCond(self.__name, '<<', str(other))

    def __rlshift__(self, other):
        return FilterCond(other, '<<', str(self.__name))

    def __rshift__(self, other):
        return FilterCond(self.__name, '>>', str(other))

    def __rrshift__(self, other):
        return FilterCond(other, '>>', str(self.__name))

    def __floordiv__(self, other):
        return FilterCond(self.__name, '//', str(other))

    def __rfloordiv__(self, other):
        return FilterCond(other, '//', str(self.__name))


class FilterCond(object):
    """Filtering conditions."""
    def __init__(self, lhs: Union[str, "FilterCond"], op: str, rhs: Union[str, "FilterCond"]):
        self.__lhs = lhs
        self.__op = op
        self.__rhs = rhs

    def __str__(self):
        return '(' + str(self.__lhs) + ' ' + str(self.__op) + ' ' + str(self.__rhs) + ')'

    def __or__(self, other):
        return FilterCond(str(self), 'or', str(other))

    def __ror__(self, other):
        return FilterCond(str(other), 'or', str(self))

    def __and__(self, other):
        return FilterCond(str(self), 'and', str(other))

    def __rand__(self, other):
        return FilterCond(str(other), 'and', str(self))

    def __lt__(self, other):
        return FilterCond(str(self), '<', str(other))

    def __rlt__(self, other):
        return FilterCond(str(other), '<', str(self))

    def __le__(self, other):
        return FilterCond(str(self), '<=', str(other))

    def __rle__(self, other):
        return FilterCond(str(other), '<=', str(self))

    def __gt__(self, other):
        return FilterCond(str(self), '>', str(other))

    def __rgt__(self, other):
        return FilterCond(str(other), '>', str(self))

    def __ge__(self, other):
        return FilterCond(str(self), '>=', str(other))

    def __rge__(self, other):
        return FilterCond(str(other), '>=', str(self))

    def __eq__(self, other):
        return FilterCond(str(self), '==', str(other))

    def __req__(self, other):
        return FilterCond(str(other), '==', str(self))

    def __ne__(self, other):
        return FilterCond(str(self), '!=', str(other))

    def __rne__(self, other):
        return FilterCond(str(self), '!=', str(other))

    def __add__(self, other):
        return FilterCond(str(self), '+', str(other))

    def __radd__(self, other):
        return FilterCond(str(other), '+', str(self))

    def __sub__(self, other):
        return FilterCond(str(self), '-', str(other))

    def __rsub__(self, other):
        return FilterCond(str(other), '-', str(self))

    def __mul__(self, other):
        return FilterCond(str(self), '*', str(other))

    def __rmul__(self, other):
        return FilterCond(str(other), '*', str(self))

    def __truediv__(self, other):
        return FilterCond(str(self), '/', str(other))

    def __rtruediv__(self, other):
        return FilterCond(str(other), '/', str(self))

    def __mod__(self, other):
        return FilterCond(str(self), '%', str(other))

    def __rmod__(self, other):
        return FilterCond(str(other), '%', str(self))

    def __lshift__(self, other):
        return FilterCond(str(self), '<<', str(other))

    def __rlshift__(self, other):
        return FilterCond(str(other), '<<', str(self))

    def __rshift__(self, other):
        return FilterCond(str(self), '>>', str(other))

    def __rrshift__(self, other):
        return FilterCond(str(other), '>>', str(self))

    def __floordiv__(self, other):
        return FilterCond(str(self), '//', str(other))

    def __rfloordiv__(self, other):
        return FilterCond(str(other), '//', str(self))
