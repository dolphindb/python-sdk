from dolphindb import Session
import pickle
import numpy as np
import os
import functools
import dbm
import collections
import sys
import copyreg
import weakref


class ExtensionSaver:
    # Remember current registration for code (if any), and remove it (if
    # there is one).
    def __init__(self, code):
        self.code = code
        if code in copyreg._inverted_registry:
            self.pair = copyreg._inverted_registry[code]
            copyreg.remove_extension(self.pair[0], self.pair[1], code)
        else:
            self.pair = None

    # Restore previous registration for code.
    def restore(self):
        code = self.code
        curpair = copyreg._inverted_registry.get(code)
        if curpair is not None:
            copyreg.remove_extension(curpair[0], curpair[1], code)
        pair = self.pair
        if pair is not None:
            copyreg.add_extension(pair[0], pair[1], code)


class C:
    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class D(C):
    def __init__(self, arg):
        pass


class E(C):
    def __getinitargs__(self):
        return ()


# Simple mutable object.
class Object:
    pass


# Hashable immutable key object containing unheshable mutable data.
class K:
    def __init__(self, value):
        self.value = value

    def __reduce__(self):
        # Shouldn't support the recursion itself
        return K, (self.value,)


def identity(x):
    return x


import __main__

__main__.C = C
C.__module__ = "__main__"
__main__.D = D
D.__module__ = "__main__"
__main__.E = E
E.__module__ = "__main__"


class myint(int):
    def __init__(self, x):
        self.str = str(x)


class initarg(C):

    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __getinitargs__(self):
        return self.a, self.b


class metaclass(type):
    pass


class use_metaclass(object, metaclass=metaclass):
    pass


class pickling_metaclass(type):
    def __eq__(self, other):
        return (type(self) == type(other) and
                self.reduce_args == other.reduce_args)

    def __reduce__(self):
        return (create_dynamic_class, self.reduce_args)


def create_dynamic_class(name, bases):
    result = pickling_metaclass(name, bases, dict())
    result.reduce_args = (name, bases)
    return result


class ZeroCopyBytes(bytes):
    readonly = True
    c_contiguous = True
    f_contiguous = True
    zero_copy_reconstruct = True

    def __reduce_ex__(self, protocol):
        if protocol >= 5:
            return type(self)._reconstruct, (pickle.PickleBuffer(self),), None
        else:
            return type(self)._reconstruct, (bytes(self),)

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, bytes(self))

    __str__ = __repr__

    @classmethod
    def _reconstruct(cls, obj):
        with memoryview(obj) as m:
            obj = m.obj
            if type(obj) is cls:
                # Zero-copy
                return obj
            else:
                return cls(obj)


class ZeroCopyBytearray(bytearray):
    readonly = False
    c_contiguous = True
    f_contiguous = True
    zero_copy_reconstruct = True

    def __reduce_ex__(self, protocol):
        if protocol >= 5:
            return type(self)._reconstruct, (pickle.PickleBuffer(self),), None
        else:
            return type(self)._reconstruct, (bytes(self),)

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, bytes(self))

    __str__ = __repr__

    @classmethod
    def _reconstruct(cls, obj):
        with memoryview(obj) as m:
            obj = m.obj
            if type(obj) is cls:
                # Zero-copy
                return obj
            else:
                return cls(obj)


DATA0 = (
    b'(lp0\nL0L\naL1L\naF2.0\n'
    b'ac__builtin__\ncomple'
    b'x\np1\n(F3.0\nF0.0\ntp2\n'
    b'Rp3\naL1L\naL-1L\naL255'
    b'L\naL-255L\naL-256L\naL'
    b'65535L\naL-65535L\naL-'
    b'65536L\naL2147483647L'
    b'\naL-2147483647L\naL-2'
    b'147483648L\na(Vabc\np4'
    b'\ng4\nccopy_reg\n_recon'
    b'structor\np5\n(c__main'
    b'__\nC\np6\nc__builtin__'
    b'\nobject\np7\nNtp8\nRp9\n'
    b'(dp10\nVfoo\np11\nL1L\ns'
    b'Vbar\np12\nL2L\nsbg9\ntp'
    b'13\nag13\naL5L\na.'
)

DATA1 = (
    b']q\x00(K\x00K\x01G@\x00\x00\x00\x00\x00\x00\x00c__'
    b'builtin__\ncomplex\nq\x01'
    b'(G@\x08\x00\x00\x00\x00\x00\x00G\x00\x00\x00\x00\x00\x00\x00\x00t'
    b'q\x02Rq\x03K\x01J\xff\xff\xff\xffK\xffJ\x01\xff\xff\xffJ'
    b'\x00\xff\xff\xffM\xff\xffJ\x01\x00\xff\xffJ\x00\x00\xff\xffJ\xff\xff'
    b'\xff\x7fJ\x01\x00\x00\x80J\x00\x00\x00\x80(X\x03\x00\x00\x00ab'
    b'cq\x04h\x04ccopy_reg\n_reco'
    b'nstructor\nq\x05(c__main'
    b'__\nC\nq\x06c__builtin__\n'
    b'object\nq\x07Ntq\x08Rq\t}q\n('
    b'X\x03\x00\x00\x00fooq\x0bK\x01X\x03\x00\x00\x00bar'
    b'q\x0cK\x02ubh\ttq\rh\rK\x05e.'
)

DATA2 = (
    b'\x80\x02]q\x00(K\x00K\x01G@\x00\x00\x00\x00\x00\x00\x00c'
    b'__builtin__\ncomplex\n'
    b'q\x01G@\x08\x00\x00\x00\x00\x00\x00G\x00\x00\x00\x00\x00\x00\x00\x00'
    b'\x86q\x02Rq\x03K\x01J\xff\xff\xff\xffK\xffJ\x01\xff\xff\xff'
    b'J\x00\xff\xff\xffM\xff\xffJ\x01\x00\xff\xffJ\x00\x00\xff\xffJ\xff'
    b'\xff\xff\x7fJ\x01\x00\x00\x80J\x00\x00\x00\x80(X\x03\x00\x00\x00a'
    b'bcq\x04h\x04c__main__\nC\nq\x05'
    b')\x81q\x06}q\x07(X\x03\x00\x00\x00fooq\x08K\x01'
    b'X\x03\x00\x00\x00barq\tK\x02ubh\x06tq\nh'
    b'\nK\x05e.'
)

DATA3 = (
    b'\x80\x03]q\x00(K\x00K\x01G@\x00\x00\x00\x00\x00\x00\x00c'
    b'builtins\ncomplex\nq\x01G'
    b'@\x08\x00\x00\x00\x00\x00\x00G\x00\x00\x00\x00\x00\x00\x00\x00\x86q\x02'
    b'Rq\x03K\x01J\xff\xff\xff\xffK\xffJ\x01\xff\xff\xffJ\x00\xff'
    b'\xff\xffM\xff\xffJ\x01\x00\xff\xffJ\x00\x00\xff\xffJ\xff\xff\xff\x7f'
    b'J\x01\x00\x00\x80J\x00\x00\x00\x80(X\x03\x00\x00\x00abcq'
    b'\x04h\x04c__main__\nC\nq\x05)\x81q'
    b'\x06}q\x07(X\x03\x00\x00\x00barq\x08K\x02X\x03\x00'
    b'\x00\x00fooq\tK\x01ubh\x06tq\nh\nK\x05'
    b'e.'
)

DATA4 = (
    b'\x80\x04\x95\xa8\x00\x00\x00\x00\x00\x00\x00]\x94(K\x00K\x01G@'
    b'\x00\x00\x00\x00\x00\x00\x00\x8c\x08builtins\x94\x8c\x07'
    b'complex\x94\x93\x94G@\x08\x00\x00\x00\x00\x00\x00G'
    b'\x00\x00\x00\x00\x00\x00\x00\x00\x86\x94R\x94K\x01J\xff\xff\xff\xffK'
    b'\xffJ\x01\xff\xff\xffJ\x00\xff\xff\xffM\xff\xffJ\x01\x00\xff\xffJ'
    b'\x00\x00\xff\xffJ\xff\xff\xff\x7fJ\x01\x00\x00\x80J\x00\x00\x00\x80('
    b'\x8c\x03abc\x94h\x06\x8c\x08__main__\x94\x8c'
    b'\x01C\x94\x93\x94)\x81\x94}\x94(\x8c\x03bar\x94K\x02\x8c'
    b'\x03foo\x94K\x01ubh\nt\x94h\x0eK\x05e.'
)

# set([1,2]) pickled from 2.x with protocol 2
DATA_SET = b'\x80\x02c__builtin__\nset\nq\x00]q\x01(K\x01K\x02e\x85q\x02Rq\x03.'

# xrange(5) pickled from 2.x with protocol 2
DATA_XRANGE = b'\x80\x02c__builtin__\nxrange\nq\x00K\x00K\x05K\x01\x87q\x01Rq\x02.'

# a SimpleCookie() object pickled from 2.x with protocol 2
DATA_COOKIE = (b'\x80\x02cCookie\nSimpleCookie\nq\x00)\x81q\x01U\x03key'
               b'q\x02cCookie\nMorsel\nq\x03)\x81q\x04(U\x07commentq\x05U'
               b'\x00q\x06U\x06domainq\x07h\x06U\x06secureq\x08h\x06U\x07'
               b'expiresq\th\x06U\x07max-ageq\nh\x06U\x07versionq\x0bh\x06U'
               b'\x04pathq\x0ch\x06U\x08httponlyq\rh\x06u}q\x0e(U\x0b'
               b'coded_valueq\x0fU\x05valueq\x10h\x10h\x10h\x02h\x02ubs}q\x11b.')

# set([3]) pickled from 2.x with protocol 2
DATA_SET2 = b'\x80\x02c__builtin__\nset\nq\x00]q\x01K\x03a\x85q\x02Rq\x03.'


def create_data():
    c = C()
    c.foo = 1
    c.bar = 2
    x = [0, 1, 2.0, 3.0 + 0j]
    # Append some integer test cases at cPickle.c's internal size
    # cutoffs.
    uint1max = 0xff
    uint2max = 0xffff
    int4max = 0x7fffffff
    x.extend([1, -1,
              uint1max, -uint1max, -uint1max - 1,
              uint2max, -uint2max, -uint2max - 1,
              int4max, -int4max, -int4max - 1])
    y = ('abc', 'abc', c, c)
    x.append(y)
    x.append(y)
    x.append(5)
    return x


class REX_one(object):
    """No __reduce_ex__ here, but inheriting it from object"""
    _reduce_called = 0

    def __reduce__(self):
        self._reduce_called = 1
        return REX_one, ()


class REX_two(object):
    """No __reduce__ here, but inheriting it from object"""
    _proto = None

    def __reduce_ex__(self, proto):
        self._proto = proto
        return REX_two, ()


class REX_three(object):
    _proto = None

    def __reduce_ex__(self, proto):
        self._proto = proto
        return REX_two, ()

    def __reduce__(self):
        raise RuntimeError("This __reduce__ shouldn't be called")


class REX_four(object):
    """Calling base class method should succeed"""
    _proto = None

    def __reduce_ex__(self, proto):
        self._proto = proto
        return object.__reduce_ex__(self, proto)


class REX_five(object):
    """This one used to fail with infinite recursion"""
    _reduce_called = 0

    def __reduce__(self):
        self._reduce_called = 1
        return object.__reduce__(self)


class REX_six(object):
    """This class is used to check the 4th argument (list iterator) of
    the reduce protocol.
    """

    def __init__(self, items=None):
        self.items = items if items is not None else []

    def __eq__(self, other):
        return type(self) is type(other) and self.items == other.items

    def append(self, item):
        self.items.append(item)

    def __reduce__(self):
        return type(self), (), None, iter(self.items), None


class REX_seven(object):
    """This class is used to check the 5th argument (dict iterator) of
    the reduce protocol.
    """

    def __init__(self, table=None):
        self.table = table if table is not None else {}

    def __eq__(self, other):
        return type(self) is type(other) and self.table == other.table

    def __setitem__(self, key, value):
        self.table[key] = value

    def __reduce__(self):
        return type(self), (), None, None, iter(self.table.items())


class REX_state(object):
    """This class is used to check the 3th argument (state) of
    the reduce protocol.
    """

    def __init__(self, state=None):
        self.state = state

    def __eq__(self, other):
        return type(self) is type(other) and self.state == other.state

    def __setstate__(self, state):
        self.state = state

    def __reduce__(self):
        return type(self), (), self.state


# Test classes for newobj

class MyInt(int):
    sample = 1


class MyFloat(float):
    sample = 1.0


class MyComplex(complex):
    sample = 1.0 + 0.0j


class MyStr(str):
    sample = "hello"


class MyUnicode(str):
    sample = "hello \u1234"


class MyTuple(tuple):
    sample = (1, 2, 3)


class MyList(list):
    sample = [1, 2, 3]


class MyDict(dict):
    sample = {"a": 1, "b": 2}


class MySet(set):
    sample = {"a", "b"}


class MyFrozenSet(frozenset):
    sample = frozenset({"a", "b"})


myclasses = [MyInt, MyFloat,
             MyComplex,
             MyStr, MyUnicode,
             MyTuple, MyList, MyDict, MySet, MyFrozenSet]


class MyIntWithNew(int):
    def __new__(cls, value):
        raise AssertionError


class MyIntWithNew2(MyIntWithNew):
    __new__ = int.__new__


class SlotList(MyList):
    __slots__ = ["foo"]


class SimpleNewObj(int):
    def __init__(self, *args, **kwargs):
        # raise an error, to make sure this isn't called
        raise TypeError("SimpleNewObj.__init__() didn't expect to get called")

    def __eq__(self, other):
        return int(self) == int(other) and self.__dict__ == other.__dict__


class ComplexNewObj(SimpleNewObj):
    def __getnewargs__(self):
        return ('%X' % self, 16)


class ComplexNewObjEx(SimpleNewObj):
    def __getnewargs_ex__(self):
        return ('%X' % self,), {'base': 16}


class BadGetattr:
    def __getattr__(self, key):
        self.foo


REDUCE_A = 'reduce_A'


class AAA(object):
    def __reduce__(self):
        return str, (REDUCE_A,)


class BBB(object):
    def __init__(self):
        # Add an instance attribute to enable state-saving routines at pickling
        # time.
        self.a = "some attribute"

    def __setstate__(self, state):
        self.a = "BBB.__setstate__"


class TestPickleUnmarshal:
    _testdata = create_data()

    def load(self, obj, protocol=4):
        if os.path.exists("test.pkl"):
            os.remove("test.pkl")
        if os.path.exists("test.pkl_s"):
            os.remove("test.pkl_s")
        with open("test.pkl", "wb") as f:
            pickle.dump(obj, f, protocol=protocol)
        s = Session()
        return s.cpp.loadPickleFile("test.pkl")

    def loads(self, buf):
        if os.path.exists("test.pkl"):
            os.remove("test.pkl")
        if os.path.exists("test.pkl_s"):
            os.remove("test.pkl_s")
        with open("test.pkl", "wb") as f:
            f.write(buf)
        s = Session()
        return s.cpp.loadPickleFile("test.pkl")

    def assertListEqual(self, a, b, msg=None):
        for x, y in zip(a, b):
            assert x == y

    def assertEqual(self, a, b, msg=None):
        assert a == b

    def assertNotEqual(self, a, b, msg=None):
        assert a != b

    def assertIs(self, a, b, msg=None):
        assert a is b

    def assertIsNot(self, a, b, msg=None):
        assert a is not b

    def assertIsNone(self, a, msg=None):
        assert a is None

    def assertDictEqual(self, a: dict, b: dict, msg=None):
        for (ka, va), (kb, vb) in zip(a.items(), b.items()):
            assert ka == kb
            assert va == vb

    def assertIsInstance(self, a, b, msg=None):
        assert isinstance(a, b)

    def assert_is_copy(self, obj, objcopy, msg=None):
        """Utility method to verify if two objects are copies of each others.
        """
        if msg is None:
            msg = "{!r} is not a copy of {!r}".format(obj, objcopy)
        self.assertEqual(obj, objcopy, msg=msg)
        self.assertIs(type(obj), type(objcopy), msg=msg)
        if hasattr(obj, '__dict__'):
            self.assertDictEqual(obj.__dict__, objcopy.__dict__, msg=msg)
            self.assertIsNot(obj.__dict__, objcopy.__dict__, msg=msg)
        if hasattr(obj, '__slots__'):
            self.assertListEqual(obj.__slots__, objcopy.__slots__, msg=msg)
            for slot in obj.__slots__:
                self.assertEqual(
                    hasattr(obj, slot), hasattr(objcopy, slot), msg=msg)
                self.assertEqual(getattr(obj, slot, None),
                                 getattr(objcopy, slot, None), msg=msg)

    def check_unmarshal_error(self, data):
        re = self.loads(data)
        assert (isinstance(re, dict) and re['errorInfo'] == "unmarshall failed")

    def check_unmarshal_typeerror(self, data):
        try:
            self.loads(data)
        except TypeError:
            ...
        except SystemError:
            ...
        else:
            ...

    def check_unpickling_error(self, errors, data):
        re = self.loads(data)
        if isinstance(re, dict) and re['errorInfo'] == "unmarshall failed":
            pass
        else:
            raise AssertionError(data, errors)

    def test_load_proto(self):
        goodpickles = [(b'\x80\x04\x80\x00S""\n.', ''),
                       (b'\x80\x04\x80\x01S""\n.', ''),
                       (b'\x80\x04\x80\x02S""\n.', ''),
                       (b'\x80\x04\x80\x03S""\n.', ''),
                       (b'\x80\x04\x80\x04S""\n.', '')]
        for p, expected in goodpickles:
            self.assertEqual(self.loads(p), expected)

        self.check_unmarshal_typeerror(b'\x80\x04\x80\x05S""\n.')

    def test_load_from_data0(self):
        # self.assert_is_copy(self._testdata, self.loads(DATA0))
        self.check_unmarshal_error(DATA0)

    def test_load_from_data1(self):
        # self.assert_is_copy(self._testdata, self.loads(DATA1))
        self.check_unmarshal_error(DATA1)

    def test_load_from_data2(self):
        # self.assert_is_copy(self._testdata, self.loads(DATA2))
        self.check_unmarshal_error(DATA2)

    def test_load_from_data3(self):
        # self.assert_is_copy(self._testdata, self.loads(DATA3))
        self.check_unmarshal_error(DATA3)

    def test_load_from_data4(self):
        self.assert_is_copy(self._testdata, self.loads(DATA4))

    def test_load_inst(self):
        X, args = (C, ())
        xname = X.__name__.encode('ascii')
        pickle0 = (b"\x80\x04i__main__\n"
                   b"X\n"
                   b"p0\n"
                   b"(dp1\nb.").replace(b'X', xname)
        self.check_unmarshal_typeerror(pickle0)

        pickle1 = (b"\x80\x04(i__main__").replace(b'X', xname)
        self.check_unmarshal_typeerror(pickle1)

        pickle2 = (b"\x80\x04(i\n").replace(b'X', xname)
        self.check_unmarshal_typeerror(pickle2)

        pickle3 = (b"\x80\x04(i\x96\n").replace(b'X', xname)
        self.check_unmarshal_typeerror(pickle3)

        pickle4 = (b"\x80\x04(i__main__\n"
                   b"\n"
                   b"p0\n"
                   b"(dp1\nb.").replace(b'X', xname)
        self.check_unmarshal_typeerror(pickle4)

        pickle5 = (b"\x80\x04(i__main__\n"
                   b"\x96\n"
                   b"p0\n"
                   b"(dp1\nb.").replace(b'X', xname)
        self.check_unmarshal_typeerror(pickle5)

    def test_load_classic_instance(self):
        # See issue5180.  Test loading 2.x pickles that
        # contain an instance of old style class.
        for X, args in [(C, ()), (D, ('x',)), (E, ())]:
            xname = X.__name__.encode('ascii')
            # Protocol 0 (text mode pickle):
            """
             0: (    MARK
             1: i        INST       '__main__ X' (MARK at 0)
            13: p    PUT        0
            16: (    MARK
            17: d        DICT       (MARK at 16)
            18: p    PUT        1
            21: b    BUILD
            22: .    STOP
            """
            pickle0 = (b"\x80\x04(i__main__\n"
                       b"X\n"
                       b"p0\n"
                       b"(dp1\nb.").replace(b'X', xname)
            self.assert_is_copy(X(*args), self.loads(pickle0))

            # Protocol 1 (binary mode pickle)
            """
             0: (    MARK
             1: c        GLOBAL     '__main__ X'
            13: q        BINPUT     0
            15: o        OBJ        (MARK at 0)
            16: q    BINPUT     1
            18: }    EMPTY_DICT
            19: q    BINPUT     2
            21: b    BUILD
            22: .    STOP
            """
            pickle1 = (b'\x80\x04\x80\x01(c__main__\n'
                       b'X\n'
                       b'q\x00oq\x01}q\x02b.').replace(b'X', xname)
            self.assert_is_copy(X(*args), self.loads(pickle1))

            # Protocol 2 (pickle2 = b'\x80\x02' + pickle1)
            """
             0: \x80 PROTO      2
             2: (    MARK
             3: c        GLOBAL     '__main__ X'
            15: q        BINPUT     0
            17: o        OBJ        (MARK at 2)
            18: q    BINPUT     1
            20: }    EMPTY_DICT
            21: q    BINPUT     2
            23: b    BUILD
            24: .    STOP
            """
            pickle2 = (b'\x80\x04\x80\x02(c__main__\n'
                       b'X\n'
                       b'q\x00oq\x01}q\x02b.').replace(b'X', xname)
            self.assert_is_copy(X(*args), self.loads(pickle2))

            data = X(*args)
            self.assert_is_copy(data, self.load(data))

    def test_maxint64(self):
        maxint64 = (1 << 63) - 1
        got = self.load(maxint64)
        self.assert_is_copy(maxint64, got)

    def test_constants(self):
        self.assertIsNone(self.load(None))
        self.assertIs(self.load(True), True)
        self.assertIs(self.load(False), False)

    def test_empty_bytestring(self):
        # issue 11286
        empty = self.load('')
        self.assertEqual(empty, '')

    def test_short_binbytes(self):
        self.assertEqual(self.load(b'\xe2\x82\xac\x00'), b'\xe2\x82\xac\x00')

    def test_binbytes(self):
        self.assertEqual(self.load(b'\xe2\x82\xac\x00'), b'\xe2\x82\xac\x00')

    def test_short_binunicode(self):
        dumped = b'\x80\x04\x8c\x04\xe2\x82\xac\x00.'
        self.assertEqual(self.loads(dumped), '\u20ac\x00')

    def test_binbytes8(self):
        dumped = b'\x80\x04\x8e\4\0\0\0\0\0\0\0\xe2\x82\xac\x00.'
        self.assertEqual(self.loads(dumped), b'\xe2\x82\xac\x00')

    def test_binunicode8(self):
        dumped = b'\x80\x04\x8d\4\0\0\0\0\0\0\0\xe2\x82\xac\x00.'
        self.assertEqual(self.loads(dumped), '\u20ac\x00')

    def test_bytearray8(self):
        self.assertEqual(self.load(bytearray(b'xxx')), bytearray(b'xxx'))

    def test_get(self):
        pickled = b'\x80\x04((lp100000\ng100000\nt.'
        unpickled = self.loads(pickled)
        self.assertEqual(unpickled, ([],) * 2)
        self.assertIs(unpickled[0], unpickled[1])

    def test_binget(self):
        pickled = b'\x80\x04(]q\xffh\xfft.'
        unpickled = self.loads(pickled)
        self.assertEqual(unpickled, ([],) * 2)
        self.assertIs(unpickled[0], unpickled[1])

    def test_long_binget(self):
        pickled = b'\x80\x04(]r\x00\x00\x01\x00j\x00\x00\x01\x00t.'
        unpickled = self.loads(pickled)
        self.assertEqual(unpickled, ([],) * 2)
        self.assertIs(unpickled[0], unpickled[1])

    def test_dup(self):
        pickled = b'\x80\04((l2t.'
        unpickled = self.loads(pickled)
        self.assertEqual(unpickled, ([],) * 2)
        self.assertIs(unpickled[0], unpickled[1])

    def test_negative_put(self):
        # Issue #12847
        dumped = b'\x80\x04Va\np-1\n.'
        self.check_unmarshal_typeerror(dumped)

    def test_badly_escaped_string(self):
        self.check_unmarshal_typeerror(b"\x80\x04S'\\'\n.")

    def test_badly_quoted_string(self):
        # Issue #17710
        badpickles = [b"\x80\x04S'\n.",
                      b'\x80\x04S"\n.',
                      b'\x80\x04S\' \n.',
                      b'\x80\x04Saa\n.',
                      b'\x80\x04S" \n.',
                      b'\x80\x04S\'"\n.',
                      b'\x80\x04S"\'\n.',
                      b"\x80\x04S' ' \n.",
                      b'\x80\x04S" " \n.',
                      b"\x80\x04S ''\n.",
                      b'\x80\x04S ""\n.',
                      b'\x80\x04S \n.',
                      b'\x80\x04S\n.',
                      b'\x80\x04S.']
        for p in badpickles:
            # self.check_unmarshal_error(p)
            self.check_unmarshal_typeerror(p)
        # self.check_unpickling_error(pickle.UnpicklingError, p)

    def test_correctly_quoted_string(self):
        goodpickles = [(b"\x80\x04S''\n.", ''),
                       (b'\x80\x04S""\n.', ''),
                       (b'\x80\x04S"\\n"\n.', '\n'),
                       (b"\x80\x04S'\\n'\n.", '\n')]
        for p, expected in goodpickles:
            self.assertEqual(self.loads(p), expected)

    def test_truncated_data(self):
        # self.check_unpickling_error(EOFError, b'')
        # self.check_unpickling_error(EOFError, b'N')
        badpickles = [
            b'B',  # BINBYTES
            b'B\x03\x00\x00',
            b'B\x03\x00\x00\x00',
            b'B\x03\x00\x00\x00ab',
            b'C',  # SHORT_BINBYTES
            b'C\x03',
            b'C\x03ab',
            b'F',  # FLOAT
            b'F0.0',
            b'F0.00',
            b'G',  # BINFLOAT
            b'G\x00\x00\x00\x00\x00\x00\x00',
            b'I',  # INT
            b'I0',
            b'J',  # BININT
            b'J\x00\x00\x00',
            b'K',  # BININT1
            b'L',  # LONG
            b'L0',
            b'L10',
            b'L0L',
            b'L10L',
            b'M',  # BININT2
            b'M\x00',
            # b'P',                       # PERSID
            # b'Pabc',
            b'S',  # STRING
            b"S'abc'",
            b'T',  # BINSTRING
            b'T\x03\x00\x00',
            b'T\x03\x00\x00\x00',
            b'T\x03\x00\x00\x00ab',
            b'U',  # SHORT_BINSTRING
            b'U\x03',
            b'U\x03ab',
            b'V',  # UNICODE
            b'Vabc',
            b'X',  # BINUNICODE
            b'X\x03\x00\x00',
            b'X\x03\x00\x00\x00',
            b'X\x03\x00\x00\x00ab',
            b'(c',  # GLOBAL
            b'(cbuiltins',
            b'(cbuiltins\n',
            b'(cbuiltins\nlist',
            b'Ng',  # GET
            b'Ng0',
            b'(i',  # INST
            b'(ibuiltins',
            b'(ibuiltins\n',
            b'(ibuiltins\nlist',
            b'Nh',  # BINGET
            b'Nj',  # LONG_BINGET
            b'Nj\x00\x00\x00',
            b'Np',  # PUT
            b'Np0',
            b'Nq',  # BINPUT
            b'Nr',  # LONG_BINPUT
            b'Nr\x00\x00\x00',
            b'\x80',  # PROTO
            b'\x82',  # EXT1
            b'\x83',  # EXT2
            b'\x84\x01',
            b'\x84',  # EXT4
            b'\x84\x01\x00\x00',
            b'\x8a',  # LONG1
            b'\x8b',  # LONG4
            b'\x8b\x00\x00\x00',
            b'\x8c',  # SHORT_BINUNICODE
            b'\x8c\x03',
            b'\x8c\x03ab',
            b'\x8d',  # BINUNICODE8
            b'\x8d\x03\x00\x00\x00\x00\x00\x00',
            b'\x8d\x03\x00\x00\x00\x00\x00\x00\x00',
            b'\x8d\x03\x00\x00\x00\x00\x00\x00\x00ab',
            b'\x8e',  # BINBYTES8
            b'\x8e\x03\x00\x00\x00\x00\x00\x00',
            b'\x8e\x03\x00\x00\x00\x00\x00\x00\x00',
            b'\x8e\x03\x00\x00\x00\x00\x00\x00\x00ab',
            b'\x96',  # BYTEARRAY8
            b'\x96\x03\x00\x00\x00\x00\x00\x00',
            b'\x96\x03\x00\x00\x00\x00\x00\x00\x00',
            b'\x96\x03\x00\x00\x00\x00\x00\x00\x00ab',
            b'\x95',  # FRAME
            b'\x95\x02\x00\x00\x00\x00\x00\x00',
            b'\x95\x02\x00\x00\x00\x00\x00\x00\x00',
            b'\x95\x02\x00\x00\x00\x00\x00\x00\x00N',
        ]
        for p in badpickles:
            self.check_unmarshal_typeerror(b"\x80\x04" + p)
        # self.check_unpickling_error(self.truncated_errors, "\x80\x04" + p)

    def test_frame_readline(self):
        # pickled = b'\x80\x04\x80\x04\x95\x00\x00\x00\x00\x00\x00\x00\x05I42\n.'
        pickled = b'\x80\x04\x80\x04\x95\x00\x00\x00\x00\x00\x00\x00\x00I42\n.'
        #    0: \x80 PROTO      4
        #    2: \x95 FRAME      5
        #   11: I    INT        42
        #   15: .    STOP
        self.assertEqual(self.loads(pickled), 42)

    def test_load_int(self):
        pickled = b'\x80\x04\x80\x04\x95\x00\x00\x00\x00\x00\x00\x00\x00I00\n.'
        self.assertIs(self.loads(pickled), False)

        pickled = b'\x80\x04\x80\x04\x95\x00\x00\x00\x00\x00\x00\x00\x00I01\n.'
        self.assertIs(self.loads(pickled), True)

        pickled = b'\x80\x04\x80\x04\x95\x00\x00\x00\x00\x00\x00\x00\x00I\n.'
        self.check_unmarshal_typeerror(pickled)

        pickled = b'\x80\x04\x80\x04\x95\x00\x00\x00\x00\x00\x00\x00\x00I02\n.'
        self.assertEqual(self.loads(pickled), 2)

        pickled = b'\x80\x04\x80\x04\x95\x00\x00\x00\x00\x00\x00\x00\x00I123456\n.'
        self.assertEqual(self.loads(pickled), 123456)

        pickled = b'\x80\x04\x80\x04\x95\x00\x00\x00\x00\x00\x00\x00\x00I123a\n.'
        self.check_unmarshal_typeerror(pickled)

        pickled = b'\x80\x04\x80\x04\x95\x00\x00\x00\x00\x00\x00\x00\x00I123.'
        self.check_unmarshal_typeerror(pickled)

        pickled = b'\x80\x04\x80\x04\x95\x00\x00\x00\x00\x00\x00\x00\x00I123\0\n.'
        self.assertEqual(self.loads(pickled), 123)

    def test_load_long(self):
        pickled = b'\x80\x04\x80\x04L11111L\n.'
        self.assertEqual(self.loads(pickled), 11111)

        pickled = b'\x80\x04\x80\x04L11111\n.'
        self.assertEqual(self.loads(pickled), 11111)

        pickled = b'\x80\x04\x80\x04L011111L\n.'
        self.check_unmarshal_typeerror(pickled)

        pickled = b'\x80\x04\x80\x04L\n.'
        self.check_unmarshal_typeerror(pickled)

    def test_load_float(self):
        pickled = b'\x80\x04\x80\x04F0.00\n.'
        self.assertEqual(self.loads(pickled), 0.00)

        pickled = b'\x80\x04\x80\x04F0.0012232333333333333\n.'
        self.check_unmarshal_typeerror(pickled)

        pickled = b'\x80\x04\x80\x04F0.0a\n.'
        self.check_unmarshal_typeerror(pickled)

        pickled = b'\x80\x04\x80\x04Fa\n.'
        self.check_unmarshal_typeerror(pickled)

        pickled = b'\x80\x04\x80\x04F\n.'
        self.check_unmarshal_typeerror(pickled)

    def test_load_unicode(self):
        pickled = b'\x80\x04\x80\x04Vabc\n.'
        self.assertEqual(self.loads(pickled), "abc")

        pickled = b'\x80\x04\x80\x04V\n.'
        self.check_unmarshal_typeerror(pickled)

    def test_compat_unpickle(self):
        # xrange(1, 7)
        unpickled = self.load(range(1, 7))
        self.assertIs(type(unpickled), range)
        self.assertEqual(unpickled, range(1, 7))
        self.assertEqual(list(unpickled), [1, 2, 3, 4, 5, 6])
        # reduce
        self.assertIs(self.load(functools.reduce), functools.reduce)
        # whichdb.whichdb
        self.assertIs(self.load(dbm.whichdb), dbm.whichdb)
        # Exception()
        unpickled = self.load(Exception("ugh"))
        self.assertIs(type(unpickled), Exception)
        self.assertEqual(str(unpickled), 'ugh')
        # UserDict.UserDict({1: 2})
        unpickled = self.load(collections.UserDict({1: 2}))
        self.assertIs(type(unpickled), collections.UserDict)
        self.assertEqual(unpickled, collections.UserDict({1: 2}))

    def test_misc(self):
        x = myint(4)
        y = self.load(x)
        self.assert_is_copy(x, y)

        x = (1, ())
        y = self.load(x)
        self.assert_is_copy(x, y)

        x = initarg(1, x)
        y = self.load(x)
        self.assert_is_copy(x, y)

    def test_roundtrip_equality(self):
        expected = self._testdata
        got = self.load(expected)
        self.assert_is_copy(expected, got)

    def _test_recursive_list(self, cls, aslist=identity, minprotocol=4):
        # List containing itself.
        l = cls()
        l.append(l)
        x = self.load(l)
        self.assertIsInstance(x, cls)
        y = aslist(x)
        self.assertEqual(len(y), 1)
        self.assertIs(y[0], x)

    def test_recursive_list(self):
        self._test_recursive_list(list)

    def test_recursive_list_subclass(self):
        self._test_recursive_list(MyList, minprotocol=2)

    def test_recursive_list_like(self):
        self._test_recursive_list(REX_six, aslist=lambda x: x.items)

    def _test_recursive_tuple_and_list(self, cls, aslist=identity, minprotocol=0):
        # Tuple containing a list containing the original tuple.
        t = (cls(),)
        t[0].append(t)

        x = self.load(t)
        self.assertIsInstance(x, tuple)
        self.assertEqual(len(x), 1)
        self.assertIsInstance(x[0], cls)
        y = aslist(x[0])
        self.assertEqual(len(y), 1)
        self.assertIs(y[0], x)

        # List containing a tuple containing the original list.
        t, = t

        x = self.load(t)
        self.assertIsInstance(x, cls)
        y = aslist(x)
        self.assertEqual(len(y), 1)
        self.assertIsInstance(y[0], tuple)
        self.assertEqual(len(y[0]), 1)
        self.assertIs(y[0][0], x)

    def test_recursive_tuple_and_list(self):
        self._test_recursive_tuple_and_list(list)

    def test_recursive_tuple_and_list_subclass(self):
        self._test_recursive_tuple_and_list(MyList, minprotocol=2)

    def test_recursive_tuple_and_list_like(self):
        self._test_recursive_tuple_and_list(REX_six, aslist=lambda x: x.items)

    def _test_recursive_dict(self, cls, asdict=identity, minprotocol=0):
        # Dict containing itself.
        d = cls()
        d[1] = d

        x = self.load(d)
        self.assertIsInstance(x, cls)
        y = asdict(x)
        self.assertEqual(list(y.keys()), [1])
        self.assertIs(y[1], x)

    def test_recursive_dict(self):
        self._test_recursive_dict(dict)

    def test_recursive_dict_subclass(self):
        self._test_recursive_dict(MyDict, minprotocol=2)

    def test_recursive_dict_like(self):
        self._test_recursive_dict(REX_seven, asdict=lambda x: x.table)

    def _test_recursive_tuple_and_dict(self, cls, asdict=identity, minprotocol=0):
        # Tuple containing a dict containing the original tuple.
        t = (cls(),)
        t[0][1] = t

        x = self.load(t)
        self.assertIsInstance(x, tuple)
        self.assertEqual(len(x), 1)
        self.assertIsInstance(x[0], cls)
        y = asdict(x[0])
        self.assertEqual(list(y), [1])
        self.assertIs(y[1], x)

        # Dict containing a tuple containing the original dict.
        t, = t
        x = self.load(t)
        self.assertIsInstance(x, cls)
        y = asdict(x)
        self.assertEqual(list(y), [1])
        self.assertIsInstance(y[1], tuple)
        self.assertEqual(len(y[1]), 1)
        self.assertIs(y[1][0], x)

    def test_recursive_tuple_and_dict(self):
        self._test_recursive_tuple_and_dict(dict)

    def test_recursive_tuple_and_dict_subclass(self):
        self._test_recursive_tuple_and_dict(MyDict, minprotocol=2)

    def test_recursive_tuple_and_dict_like(self):
        self._test_recursive_tuple_and_dict(REX_seven, asdict=lambda x: x.table)

    def _test_recursive_dict_key(self, cls, asdict=identity, minprotocol=0):
        # Dict containing an immutable object (as key) containing the original
        # dict.
        d = cls()
        d[K(d)] = 1

        x = self.load(d)
        self.assertIsInstance(x, cls)
        y = asdict(x)
        self.assertEqual(len(y.keys()), 1)
        self.assertIsInstance(list(y.keys())[0], K)
        self.assertIs(list(y.keys())[0].value, x)

    def test_recursive_dict_key(self):
        self._test_recursive_dict_key(dict)

    def test_recursive_dict_subclass_key(self):
        self._test_recursive_dict_key(MyDict, minprotocol=2)

    def test_recursive_dict_like_key(self):
        self._test_recursive_dict_key(REX_seven, asdict=lambda x: x.table)

    def _test_recursive_tuple_and_dict_key(self, cls, asdict=identity, minprotocol=0):
        # Tuple containing a dict containing an immutable object (as key)
        # containing the original tuple.
        t = (cls(),)
        t[0][K(t)] = 1

        x = self.load(t)
        self.assertIsInstance(x, tuple)
        self.assertEqual(len(x), 1)
        self.assertIsInstance(x[0], cls)
        y = asdict(x[0])
        self.assertEqual(len(y), 1)
        self.assertIsInstance(list(y.keys())[0], K)
        self.assertIs(list(y.keys())[0].value, x)

        # Dict containing an immutable object (as key) containing a tuple
        # containing the original dict.
        t, = t

        x = self.load(t)
        self.assertIsInstance(x, cls)
        y = asdict(x)
        self.assertEqual(len(y), 1)
        self.assertIsInstance(list(y.keys())[0], K)
        self.assertIs(list(y.keys())[0].value[0], x)

    def test_recursive_tuple_and_dict_key(self):
        self._test_recursive_tuple_and_dict_key(dict)

    def test_recursive_tuple_and_dict_subclass_key(self):
        self._test_recursive_tuple_and_dict_key(MyDict, minprotocol=2)

    def test_recursive_tuple_and_dict_like_key(self):
        self._test_recursive_tuple_and_dict_key(REX_seven, asdict=lambda x: x.table)

    def test_recursive_set(self):
        # Set containing an immutable object containing the original set.
        y = set()
        y.add(K(y))

        x = self.load(y)
        self.assertIsInstance(x, set)
        self.assertEqual(len(x), 1)
        self.assertIsInstance(list(x)[0], K)
        self.assertIs(list(x)[0].value, x)

        # Immutable object containing a set containing the original object.
        y, = y

        x = self.load(y)
        self.assertIsInstance(x, K)
        self.assertIsInstance(x.value, set)
        self.assertEqual(len(x.value), 1)
        self.assertIs(list(x.value)[0], x)

    def test_recursive_inst(self):
        # Mutable object containing itself.
        i = Object()
        i.attr = i
        x = self.load(i)
        self.assertIsInstance(x, Object)
        self.assertEqual(dir(x), dir(i))
        self.assertIs(x.attr, x)

    def test_recursive_multi(self):
        l = []
        d = {1: l}
        i = Object()
        i.attr = d
        l.append(i)
        x = self.load(l)
        self.assertIsInstance(x, list)
        self.assertEqual(len(x), 1)
        self.assertEqual(dir(x[0]), dir(i))
        self.assertEqual(list(x[0].attr.keys()), [1])
        self.assertIs(x[0].attr[1], x)

    def _test_recursive_collection_and_inst(self, factory):
        # Mutable object containing a collection containing the original
        # object.
        o = Object()
        o.attr = factory([o])
        t = type(o.attr)
        x = self.load(o)
        self.assertIsInstance(x.attr, t)
        self.assertEqual(len(x.attr), 1)
        self.assertIsInstance(list(x.attr)[0], Object)
        self.assertIs(list(x.attr)[0], x)

        # Collection containing a mutable object containing the original
        # collection.
        o = o.attr
        x = self.load(o)
        self.assertIsInstance(x, t)
        self.assertEqual(len(x), 1)
        self.assertIsInstance(list(x)[0], Object)
        self.assertIs(list(x)[0].attr, x)

    def test_recursive_list_and_inst(self):
        self._test_recursive_collection_and_inst(list)

    def test_recursive_tuple_and_inst(self):
        self._test_recursive_collection_and_inst(tuple)

    def test_recursive_dict_and_inst(self):
        self._test_recursive_collection_and_inst(dict.fromkeys)

    def test_recursive_set_and_inst(self):
        self._test_recursive_collection_and_inst(set)

    def test_recursive_frozenset_and_inst(self):
        self._test_recursive_collection_and_inst(frozenset)

    def test_recursive_list_subclass_and_inst(self):
        self._test_recursive_collection_and_inst(MyList)

    def test_recursive_tuple_subclass_and_inst(self):
        self._test_recursive_collection_and_inst(MyTuple)

    def test_recursive_dict_subclass_and_inst(self):
        self._test_recursive_collection_and_inst(MyDict.fromkeys)

    def test_recursive_set_subclass_and_inst(self):
        self._test_recursive_collection_and_inst(MySet)

    def test_recursive_frozenset_subclass_and_inst(self):
        self._test_recursive_collection_and_inst(MyFrozenSet)

    def test_recursive_inst_state(self):
        # Mutable object containing itself.
        y = REX_state()
        y.state = y
        x = self.load(y)
        self.assertIsInstance(x, REX_state)
        self.assertIs(x.state, x)

    def test_recursive_tuple_and_inst_state(self):
        # Tuple containing a mutable object containing the original tuple.
        t = (REX_state(),)
        t[0].state = t
        x = self.load(t)
        self.assertIsInstance(x, tuple)
        self.assertEqual(len(x), 1)
        self.assertIsInstance(x[0], REX_state)
        self.assertIs(x[0].state, x)

        # Mutable object containing a tuple containing the object.
        t, = t
        x = self.load(t)
        self.assertIsInstance(x, REX_state)
        self.assertIsInstance(x.state, tuple)
        self.assertEqual(len(x.state), 1)
        self.assertIs(x.state[0], x)

    def test_unicode(self):
        endcases = ['', '<\\u>', '<\\\u1234>', '<\n>',
                    '<\\>', '<\\\U00012345>', ]
        for u in endcases:
            u2 = self.load(u)
            self.assert_is_copy(u, u2)

    def test_unicode_high_plane(self):
        t = '\U00012345'
        t2 = self.load(t)
        self.assert_is_copy(t, t2)

    def test_bytes(self):
        for s in b'', b'xyz', b'xyz' * 100:
            self.assert_is_copy(s, self.load(s))
        for s in [bytes([i]) for i in range(256)]:
            self.assert_is_copy(s, self.load(s))
        for s in [bytes([i, i]) for i in range(256)]:
            self.assert_is_copy(s, self.load(s))

    def test_bytearray(self):
        for s in b'', b'xyz', b'xyz' * 100:
            b = bytearray(s)
            bb = self.load(b)
            self.assertIsNot(bb, b)
            self.assert_is_copy(b, bb)

    def test_ints(self):
        n = sys.maxsize
        while n:
            for expected in (-n, n):
                n2 = self.load(expected)
                self.assert_is_copy(expected, n2)
            n = n >> 1

    def test_long(self):
        # 256 bytes is where LONG4 begins.
        for nbits in 1, 8, 8 * 254, 8 * 255, 8 * 256, 8 * 257:
            nbase = 1 << nbits
            for npos in nbase - 1, nbase, nbase + 1:
                for n in npos, -npos:
                    got = self.load(n)
                    self.assert_is_copy(n, got)
        # Try a monster.  This is quadratic-time in protos 0 & 1, so don't
        # bother with those.
        nbase = int("deadbeeffeedface", 16)
        nbase += nbase << 1000000
        for n in nbase, -nbase:
            got = self.load(n)
            # assert_is_copy is very expensive here as it precomputes
            # a failure message by computing the repr() of n and got,
            # we just do the check ourselves.
            self.assertIs(type(got), int)
            self.assertEqual(n, got)

    def test_float(self):
        test_values = [0.0, 4.94e-324, 1e-310, 7e-308, 6.626e-34, 0.1, 0.5,
                       3.14, 263.44582062374053, 6.022e23, 1e30]
        test_values = test_values + [-x for x in test_values]
        for value in test_values:
            got = self.load(value)
            self.assert_is_copy(value, got)

    def test_reduce(self):
        inst = AAA()
        loaded = self.load(inst)
        self.assertEqual(loaded, REDUCE_A)

    def test_getinitargs(self):
        inst = initarg(1, 2)
        loaded = self.load(inst)
        self.assert_is_copy(inst, loaded)

    def test_metaclass(self):
        a = use_metaclass()
        b = self.load(a)
        self.assertEqual(a.__class__, b.__class__)

    def test_dynamic_class(self):
        a = create_dynamic_class("my_dynamic_class", (object,))
        copyreg.pickle(pickling_metaclass, pickling_metaclass.__reduce__)
        b = self.load(a)
        self.assertEqual(a, b)
        self.assertIs(type(a), type(b))

    def test_structseq(self):
        import time
        import os

        t = time.localtime()
        u = self.load(t)
        self.assert_is_copy(t, u)
        t = os.stat(os.curdir)
        u = self.load(t)
        self.assert_is_copy(t, u)
        if hasattr(os, "statvfs"):
            t = os.statvfs(os.curdir)
            u = self.load(t)
            self.assert_is_copy(t, u)

    def test_ellipsis(self):
        u = self.load(...)
        self.assertIs(..., u)

    def test_notimplemented(self):
        u = self.load(NotImplemented)
        self.assertIs(NotImplemented, u)

    def test_singleton_types(self):
        # Issue #6477: Test that types of built-in singletons can be pickled.
        singletons = [None, ..., NotImplemented]
        for singleton in singletons:
            u = self.load(type(singleton))
            self.assertIs(type(singleton), u)

    def test_long1(self):
        x = 12345678910111213141516178920
        y = self.load(x)
        self.assert_is_copy(x, y)

    def test_long4(self):
        x = 12345678910111213141516178920 << (256 * 8)
        y = self.load(x)
        self.assert_is_copy(x, y)

    def test_short_tuples(self):
        a = ()
        b = (1,)
        c = (1, 2)
        d = (1, 2, 3)
        e = (1, 2, 3, 4)
        for x in a, b, c, d, e:
            y = self.load(x)
            self.assert_is_copy(x, y)

    def test_newobj_tuple(self):
        x = MyTuple([1, 2, 3])
        x.foo = 42
        x.bar = "hello"
        y = self.load(x)
        self.assert_is_copy(x, y)

    def test_newobj_list(self):
        x = MyList([1, 2, 3])
        x.foo = 42
        x.bar = "hello"
        y = self.load(x)
        self.assert_is_copy(x, y)

    def test_newobj_generic(self):
        for C in myclasses:
            B = C.__base__
            x = C(C.sample)
            x.foo = 42
            y = self.load(x)
            detail = (4, C, B, x, y, type(y))
            self.assert_is_copy(x, y)  # XXX revisit
            self.assertEqual(B(x), B(y), detail)
            self.assertEqual(x.__dict__, y.__dict__, detail)

    def test_newobj_proxies(self):
        # NEWOBJ should use the __class__ rather than the raw type
        classes = myclasses[:]
        # Cannot create weakproxies to these classes
        for c in (MyInt, MyTuple):
            classes.remove(c)
        for C in classes:
            B = C.__base__
            x = C(C.sample)
            x.foo = 42
            p = weakref.proxy(x)
            y = self.load(p)
            self.assertEqual(type(y), type(x))  # rather than type(p)
            detail = (4, C, B, x, y, type(y))
            self.assertEqual(B(x), B(y), detail)
            self.assertEqual(x.__dict__, y.__dict__, detail)

    def test_newobj_overridden_new(self):
        # Test that Python class with C implemented __new__ is pickleable
        x = MyIntWithNew2(1)
        x.foo = 42
        y = self.load(x)
        self.assertIs(type(y), MyIntWithNew2)
        self.assertEqual(int(y), 1)
        self.assertEqual(y.foo, 42)

    def produce_global_ext(self, extcode, opcode):
        e = ExtensionSaver(extcode)
        try:
            copyreg.add_extension(__name__, "MyList", extcode)
            x = MyList([1, 2, 3])
            x.foo = 42
            x.bar = "hello"

            y = self.load(x)
            self.assert_is_copy(x, y)
        finally:
            e.restore()

    def test_global_ext1(self):
        self.produce_global_ext(0x00000001, pickle.EXT1)  # smallest EXT1 code
        self.produce_global_ext(0x000000ff, pickle.EXT1)  # largest EXT1 code

    def test_global_ext2(self):
        self.produce_global_ext(0x00000100, pickle.EXT2)  # smallest EXT2 code
        self.produce_global_ext(0x0000ffff, pickle.EXT2)  # largest EXT2 code
        self.produce_global_ext(0x0000abcd, pickle.EXT2)  # check endianness

    def test_global_ext4(self):
        self.produce_global_ext(0x00010000, pickle.EXT4)  # smallest EXT4 code
        self.produce_global_ext(0x7fffffff, pickle.EXT4)  # largest EXT4 code
        self.produce_global_ext(0x12abcdef, pickle.EXT4)  # check endianness

    def test_list_chunking(self):
        n = 10  # too small to chunk
        x = list(range(n))
        y = self.load(x)
        self.assert_is_copy(x, y)

        n = 2500  # expect at least two chunks when proto > 0
        x = list(range(n))
        y = self.load(x)
        self.assert_is_copy(x, y)

    def test_dict_chunking(self):
        n = 10  # too small to chunk
        x = dict.fromkeys(range(n))
        y = self.load(x)
        self.assert_is_copy(x, y)

        n = 2500  # expect at least two chunks when proto > 0
        x = dict.fromkeys(range(n))
        y = self.load(x)
        self.assert_is_copy(x, y)

    def test_set_chunking(self):
        n = 10  # too small to chunk
        x = set(range(n))
        y = self.load(x)
        self.assert_is_copy(x, y)

        n = 2500  # expect at least two chunks when proto >= 4
        x = set(range(n))
        y = self.load(x)
        self.assert_is_copy(x, y)

    def test_simple_newobj(self):
        x = SimpleNewObj.__new__(SimpleNewObj, 0xface)  # avoid __init__
        x.abc = 666
        y = self.load(x)
        self.assert_is_copy(x, y)

    def test_complex_newobj(self):
        x = ComplexNewObj.__new__(ComplexNewObj, 0xface)  # avoid __init__
        x.abc = 666
        y = self.load(x)
        self.assert_is_copy(x, y)

    def test_complex_newobj_ex(self):
        x = ComplexNewObjEx.__new__(ComplexNewObjEx, 0xface)  # avoid __init__
        x.abc = 666
        y = self.load(x)
        self.assert_is_copy(x, y)

    def test_newobj_list_slots(self):
        x = SlotList([1, 2, 3])
        x.foo = 42
        x.bar = "hello"
        y = self.load(x)
        self.assert_is_copy(x, y)

    def test_reduce_overrides_default_reduce_ex(self):
        x = REX_one()
        self.assertEqual(x._reduce_called, 0)
        y = self.load(x)
        self.assertEqual(y._reduce_called, 0)

    def test_reduce_ex_called(self):
        x = REX_two()
        self.assertEqual(x._proto, None)
        y = self.load(x)
        self.assertEqual(y._proto, None)

    def test_reduce_ex_overrides_reduce(self):
        x = REX_three()
        self.assertEqual(x._proto, None)
        y = self.load(x)
        self.assertEqual(y._proto, None)

    def test_reduce_ex_calls_base(self):
        x = REX_four()
        self.assertEqual(x._proto, None)
        y = self.load(x)
        self.assertEqual(y._proto, 4)

    def test_reduce_calls_base(self):
        x = REX_five()
        self.assertEqual(x._reduce_called, 0)
        y = self.load(x)
        self.assertEqual(y._reduce_called, 1)

    def test_many_puts_and_gets(self):
        # Test that internal data structures correctly deal with lots of
        # puts/gets.
        keys = ("aaa" + str(i) for i in range(100))
        large_dict = dict((k, [4, 5, 6]) for k in keys)
        obj = [dict(large_dict), dict(large_dict), dict(large_dict)]
        loaded = self.load(obj)
        self.assert_is_copy(obj, loaded)

    def test_attribute_name_interning(self):
        # Test that attribute names of pickled objects are interned when
        # unpickling.
        x = C()
        x.foo = 42
        x.bar = "hello"
        y = self.load(x)
        x_keys = sorted(x.__dict__)
        y_keys = sorted(y.__dict__)
        for x_key, y_key in zip(x_keys, y_keys):
            self.assertIs(x_key, y_key)

    def test_large_pickles(self):
        # Test the correctness of internal buffering routines when handling
        # large data.
        data = (1, min, b'xy' * (30 * 1024), len)
        loaded = self.load(data)
        self.assertEqual(len(loaded), len(data))
        self.assertEqual(loaded, data)

    def _check_pickling_with_opcode(self, obj, opcode, proto=4):
        unpickled = self.load(obj)
        self.assertEqual(obj, unpickled)

    def test_appends_on_non_lists(self):
        # Issue #17720
        obj = REX_six([1, 2, 3])
        self._check_pickling_with_opcode(obj, pickle.APPENDS)

    def test_setitems_on_non_dicts(self):
        obj = REX_seven({1: -1, 2: -2, 3: -3})
        self._check_pickling_with_opcode(obj, pickle.SETITEMS)

    FRAME_SIZE_MIN = 4
    FRAME_SIZE_TARGET = 64 * 1024

    def test_framing_large_objects(self):
        N = 1024 * 1024
        small_items = [[i] for i in range(10)]
        obj = [b'x' * N, *small_items, b'y' * N, 'z' * N]
        unpickled = self.load(obj)
        # More informative error message in case of failure.
        self.assertEqual([len(x) for x in obj],
                         [len(x) for x in unpickled])
        # Perform full equality check if the lengths match.
        self.assertEqual(obj, unpickled)

    def test_nested_names(self):
        global Nested

        class Nested:
            class A:
                class B:
                    class C:
                        pass

        for obj in [Nested.A, Nested.A.B, Nested.A.B.C]:
            unpickled = self.load(obj)
            self.assertIs(obj, unpickled)

    def test_recursive_nested_names(self):
        global Recursive

        class Recursive:
            pass

        Recursive.mod = sys.modules[Recursive.__module__]
        Recursive.__qualname__ = 'Recursive.mod.Recursive'
        unpickled = self.load(Recursive)
        self.assertIs(unpickled, Recursive)
        del Recursive.mod  # break reference loop

    def test_py_methods(self):
        global PyMethodsTest

        class PyMethodsTest:
            @staticmethod
            def cheese():
                return "cheese"

            @classmethod
            def wine(cls):
                assert cls is PyMethodsTest
                return "wine"

            def biscuits(self):
                assert isinstance(self, PyMethodsTest)
                return "biscuits"

            class Nested:
                "Nested class"

                @staticmethod
                def ketchup():
                    return "ketchup"

                @classmethod
                def maple(cls):
                    assert cls is PyMethodsTest.Nested
                    return "maple"

                def pie(self):
                    assert isinstance(self, PyMethodsTest.Nested)
                    return "pie"

        py_methods = (
            PyMethodsTest.cheese,
            PyMethodsTest.wine,
            PyMethodsTest().biscuits,
            PyMethodsTest.Nested.ketchup,
            PyMethodsTest.Nested.maple,
            PyMethodsTest.Nested().pie
        )
        py_unbound_methods = (
            (PyMethodsTest.biscuits, PyMethodsTest),
            (PyMethodsTest.Nested.pie, PyMethodsTest.Nested)
        )
        for method in py_methods:
            unpickled = self.load(method)
            self.assertEqual(method(), unpickled())
        for method, cls in py_unbound_methods:
            obj = cls()
            unpickled = self.load(method)
            self.assertEqual(method(obj), unpickled(obj))

    def test_c_methods(self):
        global Subclass

        class Subclass(tuple):
            class Nested(str):
                pass

        c_methods = (
            # bound built-in method
            ("abcd".index, ("c",)),
            # unbound built-in method
            (str.index, ("abcd", "c")),
            # bound "slot" method
            ([1, 2, 3].__len__, ()),
            # unbound "slot" method
            (list.__len__, ([1, 2, 3],)),
            # bound "coexist" method
            ({1, 2}.__contains__, (2,)),
            # unbound "coexist" method
            (set.__contains__, ({1, 2}, 2)),
            # built-in class method
            (dict.fromkeys, (("a", 1), ("b", 2))),
            # built-in static method
            (bytearray.maketrans, (b"abc", b"xyz")),
            # subclass methods
            (Subclass([1, 2, 2]).count, (2,)),
            (Subclass.count, (Subclass([1, 2, 2]), 2)),
            (Subclass.Nested("sweet").count, ("e",)),
            (Subclass.Nested.count, (Subclass.Nested("sweet"), "e")),
        )
        for method, args in c_methods:
            unpickled = self.load(method)
            self.assertEqual(method(*args), unpickled(*args))

    def test_compat_pickle(self):
        tests = [
            (range(1, 7), '__builtin__', 'xrange'),
            (map(int, '123'), 'itertools', 'imap'),
            (functools.reduce, '__builtin__', 'reduce'),
            (dbm.whichdb, 'whichdb', 'whichdb'),
            (Exception(), 'exceptions', 'Exception'),
            (collections.UserDict(), 'UserDict', 'IterableUserDict'),
            (collections.UserList(), 'UserList', 'UserList'),
            (collections.defaultdict(), 'collections', 'defaultdict'),
        ]
        for val, mod, name in tests:
            self.assertIs(type(self.load(val)), type(val))

    def test_buffers_numpy(self):
        def check_no_copy(x, y):
            np.testing.assert_equal(x, y)
            self.assertEqual(x.ctypes.data, y.ctypes.data)

        def check_copy(x, y):
            np.testing.assert_equal(x, y)
            self.assertNotEqual(x.ctypes.data, y.ctypes.data)

        def check_array(arr):
            # In-band
            new = self.load(arr)
            check_copy(arr, new)

        # 1-D
        arr = np.arange(6)
        check_array(arr)
        # 1-D, non-contiguous
        check_array(arr[::2])
        # 2-D, C-contiguous
        arr = np.arange(12).reshape((3, 4))
        check_array(arr)
        # 2-D, F-contiguous
        check_array(arr.T)
        # 2-D, non-contiguous
        check_array(arr[::2])
