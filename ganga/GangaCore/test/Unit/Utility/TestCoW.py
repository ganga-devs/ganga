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


class TestSet(unittest.TestCase):

    def test_set_complicated_nested(self):
        t = SampleClass()

        l = [1, 2, 3, {4: set([5])}]
        t.l = l
        t2 = copy(t)
        t.l[-1][4].add(6)

        self.assertEqual(t2.l, [1, 2, 3, {4: set([5])}])
        self.assertEqual(t.l, [1, 2, 3, {4: set([5, 6])}])
        t._flyweight_cache.clear()

    def test_set_add_hash(self):
        t = SampleClass()

        t.l = set([1, 2, 3, 4])
        old_hash = hash(t.l)
        self.assertEqual(t.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 1)

        t.l.add(5)
        new_hash = hash(t.l)

        self.assertNotEqual(old_hash, new_hash)
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 1)

        self.assertIn(new_hash, t._flyweight_cache[ProxySet])
        t._flyweight_cache.clear()

    def test_set_symmetric_update(self):
        t = SampleClass()

        t.l = set([1, 2, 3, 4])
        self.assertEqual(t.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 1)

        t2 = copy(t)
        self.assertEqual(t2.l, t.l)

        t.l.update(set([5, 6, 7]))
        self.assertEqual(t.l, set([1, 2, 3, 4, 5, 6, 7]))
        self.assertEqual(t2.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 2)
        t._flyweight_cache.clear()

    def test_set_symmetric_difference_update(self):
        t = SampleClass()

        t.l = set([1, 2, 3, 4])
        self.assertEqual(t.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 1)

        t2 = copy(t)
        self.assertEqual(t2.l, t.l)

        t.l.symmetric_difference_update(set([1, 3, 4]))
        self.assertEqual(t.l, set([2]))
        self.assertEqual(t2.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 2)
        t._flyweight_cache.clear()

    def test_set_remove(self):
        t = SampleClass()

        t.l = set([1, 2, 3, 4])
        self.assertEqual(t.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 1)

        t2 = copy(t)
        self.assertEqual(t2.l, t.l)

        t.l.remove(3)
        self.assertEqual(t.l, set([1, 2, 4]))
        self.assertEqual(t2.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 2)
        t._flyweight_cache.clear()

    def test_set_pop(self):
        t = SampleClass()

        t.l = set([1])
        self.assertEqual(t.l, set([1]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 1)

        t2 = copy(t)
        self.assertEqual(t2.l, t.l)

        self.assertEqual(t.l.pop(), 1)
        self.assertEqual(t.l, set())
        self.assertEqual(t2.l, set([1]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 2)
        t._flyweight_cache.clear()

    def test_set_intersection_update(self):
        t = SampleClass()

        t.l = set([1, 2, 3, 4])
        self.assertEqual(t.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 1)
        t2 = copy(t)
        self.assertEqual(t2.l, t.l)

        t.l.intersection_update(set([1, 4, 5]))
        self.assertEqual(t.l, set([1, 4]))
        self.assertEqual(t2.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 2)
        t._flyweight_cache.clear()

    def test_set_discard(self):
        t = SampleClass()

        t.l = set([1, 2, 3, 4])
        self.assertEqual(t.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 1)

        t2 = copy(t)
        self.assertEqual(t2.l, t.l)

        t.l.discard(2)

        self.assertEqual(t.l, set([1, 3, 4]))
        self.assertEqual(t2.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 2)

        t._flyweight_cache.clear()

    def test_set_difference_update(self):
        t = SampleClass()

        t.l = set([1, 2, 3, 4])
        self.assertEqual(t.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 1)

        t2 = copy(t)
        self.assertEqual(t2.l, t.l)

        t.l.difference_update(set([1, 2]))
        self.assertEqual(t.l, set([3, 4]))
        self.assertEqual(t2.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 2)
        t._flyweight_cache.clear()

    def test_set_clear(self):
        t = SampleClass()

        t.l = set([1, 2, 3, 4])
        self.assertEqual(t.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 1)

        t2 = copy(t)
        self.assertEqual(t2.l, t.l)

        t.l.clear()
        self.assertEqual(t.l, set())
        self.assertEqual(t2.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 2)
        t._flyweight_cache.clear()

    def test_set_add(self):
        t = SampleClass()

        t.l = set([1, 2, 3, 4])
        self.assertEqual(t.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 1)

        t2 = copy(t)
        self.assertEqual(t2.l, t.l)

        t.l.add(5)
        self.assertEqual(t.l, set([1, 2, 3, 4, 5]))
        self.assertEqual(t2.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 2)
        t._flyweight_cache.clear()

    def test_set_basic(self):
        t = SampleClass()

        t.l = set([1, 2, 3, 4])
        self.assertEqual(t.l, set([1, 2, 3, 4]))

        self.assertEqual(len(t._flyweight_cache[ProxySet]), 1)

        t2 = copy(t)
        self.assertEqual(t2.l, t.l)

        t.l = set([5])
        self.assertEqual(t.l, set([5]))
        self.assertEqual(t2.l, set([1, 2, 3, 4]))
        self.assertEqual(len(t._flyweight_cache[ProxySet]), 2)
        t._flyweight_cache.clear()


class TestTuple(unittest.TestCase):

    def test_tuple_setitem_error(self):
        t = SampleClass()

        t.l = (1, 2, 3, 4, 1, 1)

        with self.assertRaises(TypeError):
            t.l[0] = 1
        t._flyweight_cache.clear()

    def test_tuple_index(self):
        t = SampleClass()

        t.l = (1, 2, 3, 4, 1, 1)
        self.assertEqual(t.l, (1, 2, 3, 4, 1, 1))
        self.assertEqual(len(t._flyweight_cache[ProxyTuple]), 1)
        self.assertEqual(t.l.index(4), 3)
        t._flyweight_cache.clear()

    def test_tuple_count(self):
        t = SampleClass()

        t.l = (1, 2, 3, 4, 1, 1)
        self.assertEqual(t.l, (1, 2, 3, 4, 1, 1))
        self.assertEqual(len(t._flyweight_cache[ProxyTuple]), 1)
        self.assertEqual(t.l.count(1), 3)
        t._flyweight_cache.clear()

    def test_tuple_basic(self):
        t = SampleClass()

        t.l = (1, 2, 3, 4)
        self.assertEqual(t.l, (1, 2, 3, 4))

        self.assertEqual(len(t._flyweight_cache[ProxyTuple]), 1)

        t2 = copy(t)
        self.assertEqual(t2.l, t.l)

        t.l += (5,)
        self.assertEqual(t.l, (1, 2, 3, 4, 5))
        self.assertEqual(t2.l, (1, 2, 3, 4))
        self.assertEqual(len(t._flyweight_cache[ProxyTuple]), 2)
        t._flyweight_cache.clear()


class TestList(unittest.TestCase):

    def test_list_append_non_cow(self):
        t = SampleClass()

        t.l = [1, 2, 3]
        t.l.append([1, 2, 3])
        self.assertEqual(t.l, [1, 2, 3, [1, 2, 3]])
        t._flyweight_cache.clear()

    def test_list_dedup_var_separately(self):
        t = SampleClass()
        t2 = SampleClass()

        t.l = [1, 2, 3]
        t2.l = [1, 2, 3]

        self.assertEqual(t.l, t2.l)
        t._flyweight_cache.clear()

    def test_list_sublist_append(self):
        t = SampleClass()

        t.l = [1, 2, 3, [4, 5, 6]]
        t.l[-1].append(7)
        self.assertEqual(t.l, [1, 2, 3, [4, 5, 6, 7]])

        t.l = [1, 2, 3, [4, 5, 6, [7, 8, 9]]]
        t.l[-1][-1].append(10)
        self.assertEqual(t.l, [1, 2, 3, [4, 5, 6, [7, 8, 9, 10]]])

        t2 = copy(t)
        t2.l[-1][-1].append(11)
        self.assertEqual(t2.l, [1, 2, 3, [4, 5, 6, [7, 8, 9, 10, 11]]])
        self.assertEqual(t.l, [1, 2, 3, [4, 5, 6, [7, 8, 9, 10]]])
        t._flyweight_cache.clear()

    def test_list_recursive_types(self):
        t = SampleClass()

        d = {1: 'one', 2: 'two'}
        t.l = [1, 2, 3, d]
        self.assertEqual(t.l[:3], [1, 2, 3])
        self.assertEqual(t.l[-1], d)
        t._flyweight_cache.clear()

    def test_list_subupdate(self):
        t = SampleClass()

        t.l = [1, 2, 3]
        self.assertEqual(t.l, [1, 2, 3])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)
        t2 = copy(t)

        t.l.append(t2)

        # Make sure we're copying correctly
        self.assertEqual(t.l[-1], t.l[-1])
        self.assertEqual(t.l[-1], t2)

        # Try appending to it, make sure it gets back up
        t.l[-1].l.append(4)
        self.assertEqual(t.l[-1].l, [1, 2, 3, 4])
        self.assertEqual(t.l[:-1], [1, 2, 3])
        self.assertEqual(len(t.l), 4)

        # Setitem
        t.l[-1].l[2] = 5
        self.assertEqual(t.l[:-1], [1, 2, 3])
        self.assertEqual(len(t.l), 4)
        self.assertEqual(t.l[-1].l, [1, 2, 5, 4])

        # Update higher in list
        t.l.append(5)
        self.assertEqual(t.l[:3], [1, 2, 3])
        self.assertEqual(len(t.l), 5)
        self.assertEqual(t.l[-1], 5)
        self.assertEqual(t.l[-2].l, [1, 2, 5, 4])

        t._flyweight_cache.clear()

    def test_list_subcow(self):
        """Adding a CoW inside the CoW"""
        t = SampleClass()

        t.l = [1, 2, 3, 4]
        self.assertEqual(t.l, [1, 2, 3, 4])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)
        t2 = SampleClass()
        t2.l = [5, 6, 7, 8]

        t.l.append(t2)

        # Make sure we're copying correctly
        self.assertEqual(t.l[-1], t.l[-1])
        self.assertEqual(t.l[-1], t2)

        # Try appending to it, make sure it gets back up
        t.l[-1].l.append(9)
        self.assertEqual(t.l[-1].l, [5, 6, 7, 8, 9])
        t._flyweight_cache.clear()

    def test_list_setitem_hash(self):
        t = SampleClass()

        t.l = [1, 2, 3, 4]
        self.assertEqual(t.l, [1, 2, 3, 4])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)

        old_hash = hash(t.l)
        t.l[1] = 5
        new_hash = hash(t.l)

        self.assertNotEqual(old_hash, new_hash)
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)
        self.assertIn(new_hash, t._flyweight_cache[ProxyList])
        t._flyweight_cache.clear()

    def test_list_append_hash(self):
        t = SampleClass()

        t.l = [1, 2, 3, 4]
        self.assertEqual(t.l, [1, 2, 3, 4])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)

        old_hash = hash(t.l)
        t.l.append(5)
        new_hash = hash(t.l)

        self.assertNotEqual(old_hash, new_hash)
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)
        self.assertIn(new_hash, t._flyweight_cache[ProxyList])
        t._flyweight_cache.clear()

    def test_list_setitem(self):
        t = SampleClass()

        t.l = [1, 2, 3, 4]
        self.assertEqual(t.l, [1, 2, 3, 4])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)

        t2 = copy(t)

        self.assertEqual(t2.l, t.l)

        t.l[2] = 10
        self.assertEqual(t.l, [1, 2, 10, 4])
        self.assertEqual(t2.l, [1, 2, 3, 4])
        t._flyweight_cache.clear()

    def test_list_sort(self):
        t = SampleClass()

        t.l = [4, 2, 3, 1]
        self.assertEqual(t.l, [4, 2, 3, 1])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)

        t2 = copy(t)

        self.assertEqual(t2.l, t.l)

        t.l.sort()
        self.assertEqual(t.l, [1, 2, 3, 4])
        self.assertEqual(t2.l, [4, 2, 3, 1])
        t._flyweight_cache.clear()

    def test_list_reverse(self):
        t = SampleClass()

        t.l = [1, 2, 3, 4]
        self.assertEqual(t.l, [1, 2, 3, 4])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)

        t2 = copy(t)

        self.assertEqual(t2.l, t.l)

        t.l.reverse()
        self.assertEqual(t.l, [4, 3, 2, 1])
        self.assertEqual(t2.l, [1, 2, 3, 4])
        t._flyweight_cache.clear()

    def test_list_remove(self):
        t = SampleClass()

        t.l = [1, 2, 3, 4]
        self.assertEqual(t.l, [1, 2, 3, 4])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)

        t2 = copy(t)

        self.assertEqual(t2.l, t.l)

        t.l.remove(4)
        self.assertEqual(t.l, [1, 2, 3])
        self.assertEqual(t2.l, [1, 2, 3, 4])
        t._flyweight_cache.clear()

    def test_list_pop(self):
        t = SampleClass()

        t.l = [1, 2, 3, 4]
        self.assertEqual(t.l, [1, 2, 3, 4])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)

        t2 = copy(t)

        self.assertEqual(t2.l, t.l)

        self.assertEqual(t.l.pop(2), 3)
        self.assertEqual(t.l, [1, 2, 4])
        self.assertEqual(t2.l, [1, 2, 3, 4])
        t._flyweight_cache.clear()

    def test_list_insert(self):
        t = SampleClass()

        t.l = [1, 2, 3, 4]
        self.assertEqual(t.l, [1, 2, 3, 4])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)

        t2 = copy(t)

        self.assertEqual(t2.l, t.l)

        t.l.insert(1, 5)
        self.assertEqual(t.l, [1, 5, 2, 3, 4])
        self.assertEqual(t2.l, [1, 2, 3, 4])
        t._flyweight_cache.clear()

    def test_list_extend(self):
        t = SampleClass()

        t.l = [1, 2, 3, 4]
        self.assertEqual(t.l, [1, 2, 3, 4])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)

        t2 = copy(t)

        self.assertEqual(t2.l, t.l)

        t.l.extend([5, 6, 7])
        self.assertEqual(t.l, [1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(t2.l, [1, 2, 3, 4])
        t._flyweight_cache.clear()

    def test_list_append(self):
        t = SampleClass()

        t.l = [1, 2, 3, 4]
        self.assertEqual(t.l, [1, 2, 3, 4])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)

        t2 = copy(t)

        self.assertEqual(t2.l, t.l)

        t.l.append(5)
        self.assertEqual(t.l, [1, 2, 3, 4, 5])
        self.assertEqual(t2.l, [1, 2, 3, 4])
        t._flyweight_cache.clear()

    def test_list_basic(self):
        t = SampleClass()

        t.l = [1, 2, 3, 4]
        self.assertEqual(t.l, [1, 2, 3, 4])

        self.assertEqual(len(t._flyweight_cache[ProxyList]), 1)

        t2 = copy(t)
        self.assertEqual(t2.l, t.l)

        t.l += [5]
        self.assertEqual(t.l, [1, 2, 3, 4, 5])
        self.assertEqual(t2.l, [1, 2, 3, 4])
        self.assertEqual(len(t._flyweight_cache[ProxyList]), 2)
        t._flyweight_cache.clear()