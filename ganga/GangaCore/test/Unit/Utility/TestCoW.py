from GangaCore.GPIDev.Base.CoW import (
    CoW,
    ProxyDict,
    ProxyStr,
    ProxyList,
    ProxyTuple,
    ProxySet
    )

from copy import copy
import unittest
import gc


class SampleClass(CoW):
    pass


class TestBool(unittest.TestCase):

    def test_bool_basic(self):
        t = SampleClass()

        t.i = True
        self.assertTrue(t.i)

        # bool is ignored
        self.assertEqual(len(t._flyweight_cache), 0)

        t2 = copy(t)
        self.assertEqual(t2.i, t.i)

        t.i &= False
        self.assertFalse(t.i)
        self.assertTrue(t2.i)
        self.assertEqual(len(t._flyweight_cache), 0)
        t._flyweight_cache.clear()


class TestString(unittest.TestCase):

    def test_string_basic(self):
        t = SampleClass()

        t.s = "test"
        self.assertEqual(t.s, "test")

        self.assertEqual(len(t._flyweight_cache), 0)

        t2 = copy(t)
        self.assertEqual(t2.s, t.s)

        t.s += "q"
        self.assertEqual(t.s, "testq")
        self.assertEqual(t2.s, "test")
        self.assertEqual(len(t._flyweight_cache), 0)
        t._flyweight_cache.clear()


class TestNone(unittest.TestCase):

    def test_none_basic(self):
        t = SampleClass()

        t.i = None
        self.assertEqual(t.i, None)

        # None is ignored
        self.assertEqual(len(t._flyweight_cache), 0)

        t2 = copy(t)
        self.assertEqual(t2.i, t.i)

        t.i = 1
        self.assertEqual(t.i, 1)
        self.assertEqual(t2.i, None)
        self.assertEqual(len(t._flyweight_cache), 0)
        t._flyweight_cache.clear()


class TestInt(unittest.TestCase):

    def test_int_basic(self):

        t = SampleClass()

        t.i = 12
        self.assertEqual(t.i, 12)

        # Int is ignored
        self.assertEqual(len(t._flyweight_cache), 0)

        t2 = copy(t)
        self.assertEqual(t2.i, t.i)

        t.i = 4
        self.assertEqual(t.i, 4)
        self.assertEqual(t2.i, 12)
        self.assertEqual(len(t._flyweight_cache), 0)

        t.i += 1
        self.assertEqual(t.i, 5)
        self.assertEqual(t2.i, 12)
        t._flyweight_cache.clear()


class TestFloat(unittest.TestCase):

    def test_float_basic(self):
        t = SampleClass()

        t.i = 12.5
        self.assertEqual(t.i, 12.5)

        # float is ignored
        self.assertEqual(len(t._flyweight_cache), 0)
        t2 = copy(t)
        self.assertEqual(t2.i, t.i)

        t.i = 4.2
        self.assertEqual(t.i, 4.2)
        self.assertEqual(t2.i, 12.5)
        self.assertEqual(len(t._flyweight_cache), 0)
        t.i += 1
        self.assertEqual(t.i, 5.2)
        self.assertEqual(t2.i, 12.5)
        t._flyweight_cache.clear()