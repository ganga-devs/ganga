from __future__ import absolute_import

try:
    import unittest2 as unittest
except ImportError:
    import unittest
import random
import string

from Ganga.GPIDev.Base.Proxy import addProxy, getProxyClass, getProxyAttr, isProxy, isType, stripProxy

from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
GangaList = getProxyClass(GangaList)
from .TFile import TFile
TFile = getProxyClass(TFile)

# set the seed for repeatable tests
random.seed(666)

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)


class TestGangaList(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestGangaList, self).__init__(*args, **kwargs)

        self.plain1 = []
        self.plain2 = []

        self.proxied1 = []
        self.proxied2 = []

        self.ganga_list = GangaList()

    @staticmethod
    def _makeRandomString():
        str_len = random.randint(3, 10)
        s = ''
        for _ in range(str_len):
            s += random.choice(string.ascii_letters)
        return s

    @staticmethod
    def _makeRandomTFile():
        name = TestGangaList._makeRandomString()
        subdir = TestGangaList._makeRandomString()
        return TFile(name=name, subdir=subdir)

    def setUp(self):
        super(TestGangaList, self).setUp()

        self.plain1 = [self._makeRandomTFile() for _ in range(15)]
        self.plain2 = [self._makeRandomTFile() for _ in range(10)]

        self.proxied1 = GangaList()
        self.proxied1.extend(self.plain1[:])
        self.proxied2 = GangaList()
        self.proxied2.extend(self.plain2[:])

        t = TFile()
        real_t = stripProxy(t)
        new_proxy_t = addProxy(real_t)
        #hopefully_t = stripProxy(new_proxy_t)
        #assert real_t is hopefully_t
        assert t is new_proxy_t

        self.assertEqual(len(getProxyAttr(self.proxied1, '_list')), len(self.plain1), "Something's wrong with construction")
        self.assertEqual(len(getProxyAttr(self.proxied2, '_list')), len(self.plain2), "Something's wrong with construction")

    def testAllListMethodsExported(self):
        """Tests that all methods on list are exposed by GangaList"""

        def getmethods(_obj):
            # get all the method names for an object
            return dir(_obj)

        list_methods = getmethods([])
        gangalist_methods = getmethods(self.ganga_list)

        missing_methods = []
        for m in list_methods:
            if not m in gangalist_methods:
                missing_methods.append(m)

        if missing_methods:
            logger.info(missing_methods)

        self.assertFalse(missing_methods, 'Not all of lists methods are implemented: %s' % missing_methods)

    def testEq(self):
        """Tests the equality op that the rest of the tests rely on."""
        self.assertEqual(self.proxied1, self.plain1, 'Proxied and non-proxied versions should be the same')

    def testNq(self):
        self.assertIsNotNone(self.proxied1, None)
        self.assertFalse(self.proxied1 != self.proxied1, 'Lists are the same')
        self.assertNotEqual(self.proxied1, self.proxied2, 'Lists are different')
        self.assertNotEqual(self.plain1, self.proxied2, 'Lists are different')

    def testNonZero(self):
        """@ExpectedFailure"""
        self.assertFalse(GangaList(), 'An empty GangaList should be false, just like a list')

    def testAdd(self):
        """Test __add__"""
        self.assertEqual(self.plain1 + self.plain2, self.proxied1 + self.proxied2)
        self.assertTrue(isProxy(self.proxied1 + self.proxied2))

    def testAddMixed(self):
        """Test __add__ with mixed lists and GangaLists"""
        self.assertEqual((self.plain1 + self.plain2), (self.proxied1 + self.plain2))
        self.assertEqual((self.plain2 + self.plain1), (self.plain2 + self.proxied1))
        self.assertTrue(isProxy(self.proxied1 + self.plain2))

        self.assertEqual((self.plain2 + self.plain1), (self.plain2 + self.proxied1))
        self.assertEqual((self.plain1 + self.plain2), (self.plain1 + self.proxied2))
        self.assertTrue(isinstance(self.plain1 + self.proxied2, list))

    def testAddMixed2(self):

        self.plain1 = range(10)
        self.plain2 = range(10)

        self.assertTrue(isProxy(self.proxied2[-1]), 'Element access must get proxies')
        self.assertFalse(isProxy(self.plain1[0]), 'Element access must not proxies')
        self.assertTrue(isProxy((self.plain1 + self.proxied2)[-1]), 'File objects should remain proxies')
        self.assertFalse(isProxy((self.plain1 + self.proxied2)[0]), 'Objects in plain lists should be left alone')

        self.assertEqual((self.plain1 + self.proxied2)[-1], self.proxied2[-1], 'File objects should be equal')
        self.assertIs((self.plain1 + self.proxied2)[-1], self.proxied2[-1], 'File objects should be identical')

    def testAddStr(self):
        """Makes sure that only lists can be added."""
        try:
            [] + ''
            assert False, 'Line above should throw a TypeError'
        except TypeError:
            pass

        try:
            self.proxied1 + ''
            assert False, 'Line above should throw a TypeError'
        except TypeError:
            pass

    def testContains(self):
        """Tests __contains__"""

        plist = [addProxy(x) for x in self.plain1]
        self.assertEqual(plist, self.proxied1)

        for p in plist:
            self.assertTrue(isProxy(p))
            self.assertIn(p, self.proxied1, 'Proxied list should contain each proxied object')

    def testDelItem(self):
        """Test __delitem__"""

        for p in [addProxy(x) for x in self.plain1[:]]:
            self.assertTrue(isProxy(p))
            del self.proxied1[self.proxied1.index(p)]

    def testGE(self):
        """Test __ge__"""

        self.assertEqual((self.plain1 >= self.plain2), (self.proxied1 >= self.proxied2), 'The lists should have the same ge')
        self.assertEqual((self.plain2 >= self.plain1), (self.proxied2 >= self.proxied1), 'The lists should have the same ge')

        self.assertNotEqual((self.proxied1 >= self.proxied2), (self.proxied2 >= self.proxied1), 'The gt should invert correctly')

    def testGetItem(self):
        """Test __getitem__"""

        for i in range(len(self.proxied1)):
            self.assertTrue(isProxy(self.proxied1[i]))

    def testGetSlice(self):
        """Test __getslice__"""

        slices = [(0, 0), (0, len(self.plain1))]

        for s in slices:
            self.assertEqual(self.plain1[s[0]:s[1]], self.proxied1[s[0]:s[1]], 'Slices {0} should be the same'.format(s))

        t = self.plain1[:]
        self.assertIsNot(t, self.plain1, 'Slice should be a copy.')
        self.assertIsNot(self.plain1[:], t)

        t = self.proxied1[:]
        self.assertIsNot(t, self.proxied1, 'Slice should be a copy.')
        self.assertIsNot(self.proxied1[:], t)

    def testGT(self):
        """Test __gt__"""

        self.assertEqual((self.plain1 > self.plain2), (self.proxied1 > self.proxied2), 'The lists should have the same gt')
        self.assertEqual((self.plain2 > self.plain1), (self.proxied2 > self.proxied1), 'The lists should have the same gt')

        self.assertNotEqual((self.proxied1 > self.proxied2), (self.proxied2 > self.proxied1), 'The gt should invert correctly')

    def testIAdd(self):
        """Test __iadd__"""
        self.assertTrue(isProxy(self.proxied1), 'Must be Proxy')
        self.assertTrue(isProxy(self.proxied2), 'Must be Proxy')

        self.plain1 += self.plain2
        self.proxied1 += self.proxied2

        self.assertEqual(self.plain1, self.proxied1, 'Addition should be the same')
        self.assertTrue(isProxy(self.proxied1), 'Proxy must be added')

    def testIAddMixed(self):
        """Test __iadd__ where we mix lists and GangaLists"""
        self.assertTrue(isProxy(self.proxied1), 'Must be Proxy')
        self.assertTrue(isProxy(self.proxied2), 'Must be Proxy')

        self.plain1 += self.plain2
        self.proxied1 += self.plain2

        self.assertEqual(self.plain1, self.proxied1, 'Addition should be the same')
        self.assertTrue(isProxy(self.proxied1), 'Proxy must be added')
        self.assertTrue(isinstance(self.plain1, list), 'Must be list instance')

        self.plain2 += self.proxied1
        self.proxied2 += self.proxied1

        self.assertEqual(self.plain2, self.proxied2, 'Addition should be the same')
        self.assertTrue(isProxy(self.proxied2), 'Proxy must be added')
        self.assertTrue(isinstance(self.plain2, list), 'Must be list instance')

    def testIAddSlice(self):
        """Test __iadd__ on slices"""

        s1 = self.plain1[3:7]
        s1 += self.plain2[2:5]

        s2 = self.proxied1[3:7]
        s2 += self.proxied2[2:5]

        self.assertEqual(s1, s2, 'Addition should be the same')

        self.assertFalse(isProxy(s1), 'Proxy Not Needed')
        self.assertTrue(isProxy(s2), 'Proxy Needed')

    def testIdentity(self):
        """Tests we obey list like identity relations."""

        t = self.plain1[4]
        self.assertIs(self.plain1[4], t)

        t = self.proxied1[4]
        self.assertIs(self.proxied1[4], t)

    def testIMul(self):
        """Test __imul__"""

        self.plain1 *= 5
        self.proxied1 *= 5

        self.assertEqual(self.plain1, self.proxied1, 'Multiplication should be the same')
        self.assertTrue(isProxy(self.proxied1), 'Proxy must be added')

    def testIMulSlice(self):
        """Test __imul__ on slices"""

        s1 = self.plain1[3:7]
        s1 *= 6

        s2 = self.proxied1[3:7]
        s2 *= 6

        self.assertEqual(s1, s2, 'Addition should be the same')

    def testLE(self):
        """Test __le__"""

        self.assertEqual(self.plain1 <= self.plain2, self.proxied1 <= self.proxied2, 'The lists should have the same le')
        self.assertEqual(self.plain2 <= self.plain1, self.proxied2 <= self.proxied1, 'The lists should have the same le')

        self.assertNotEqual(self.proxied1 <= self.proxied2, self.proxied2 <= self.proxied1, 'The le should invert correctly')

    def testLen(self):
        """Tests __len__"""
        self.assertEqual(len(self.plain1), len(self.proxied1), 'Lengths should be the same')

    def testLT(self):
        """Test __lt__"""

        self.assertEqual(self.plain1 < self.plain2, self.proxied1 < self.proxied2, 'The lists should have the same lt')
        self.assertEqual(self.plain2 < self.plain1, self.proxied2 < self.proxied1, 'The lists should have the same lt')

        self.assertNotEqual(self.proxied1 < self.proxied2, self.proxied2 < self.proxied1, 'The lt should invert correctly')

    def testMul(self):
        """Test __mul__"""
        self.assertEqual((self.plain1 * 7), (self.proxied1 * 7))
        self.assertTrue(isProxy(self.proxied1 * 9))

        for p in self.proxied1:
            self.assertTrue(isProxy(p))

    def testNE(self):
        """Test __ne__"""

        self.assertNotEqual(self.plain1, self.plain2)
        self.assertNotEqual(self.proxied1, self.proxied2, 'Lists should be different')

        self.assertNotEqual(self.plain1[0:5], self.plain1[2:7])
        self.assertNotEqual(self.proxied1[0:5], self.proxied1[2:7], 'Lists should be different')

    def testRMul(self):
        """Test __rmul__"""

        t1 = 5 * self.plain1
        t2 = 5 * self.proxied1

        self.assertEqual(t1, t2, 'Multiplication should be the same')

    def testReversed(self):
        """Test the __reversed__ feature (new in python 2.4)."""

        count = len(self.proxied1) - 1
        for i in self.proxied1.__reversed__():
            self.assertIs(i, self.proxied1[count])
            self.assertTrue(isProxy(i))
            self.assertTrue(isType(i, TFile))
            count -= 1

    def testSetItem(self):
        """Test __setitem__"""

        t = TFile(name='foo', subdir='cheese')
        test_index = 7
        self.plain1[test_index] = t
        self.assertIs(self.plain1[test_index], t)

        self.proxied1[test_index] = t
        self.assertIs(self.proxied1[test_index], t)
        self.assertTrue(isProxy(self.proxied1[test_index]))

    def testSetSliceProxyList(self):

        self.plain1[3:7] = self.plain2[3:7]
        self.assertEqual(self.plain1[3:7], self.plain2[3:7], 'The lists should be equal')
        self.assertIsNot(self.plain1[3:7], self.plain2[3:7], 'The lists should be copies')
        self.plain1[4] = self.plain2[9]
        self.assertNotEqual(self.plain1[4], self.plain2[4])

        tmp = self.plain1[2:9]
        self.assertIsNot(self.plain1[2:9], tmp)

        self.proxied1[3:7] = self.proxied2[3:7]
        self.assertEqual(self.proxied1[3:7], self.proxied2[3:7], 'The lists should be equal')
        self.assertIsNot(self.proxied1[3:7], self.proxied2[3:7], 'The lists should be copies')
        self.proxied1[4] = self.proxied2[9]
        self.assertNotEqual(self.proxied1[4], self.proxied2[4])

        tmp = self.proxied1[2:9]
        self.assertIsNot(self.proxied1[2:9], tmp)

    def testAppend(self):

        t = TFile(name='foo')

        self.plain1.append(t)
        self.assertEqual(self.plain1[-1], t)
        self.assertEqual(self.plain1.pop(), t)

        self.proxied1.append(t)
        self.assertEqual(self.proxied1[-1], t)
        self.assertIs(self.proxied1[-1], t, 'Identity Test')
        self.assertTrue(isProxy(self.proxied1[-1]), 'Make sure we get back a proxy')
        self.assertEqual(self.proxied1.pop(), t)

    def testExtend(self):

        t1 = [self._makeRandomTFile() for _ in xrange(10)]

        self.plain1.extend(t1)
        self.proxied1.extend(t1)

        self.assertEqual(self.plain1, self.proxied1, 'Lists should be the same')

        t2 = self.proxied1[4:7]
        self.assertTrue(isProxy(t2))
        from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList as glist
        self.assertTrue(isType(t2, glist))
        self.plain1.extend(t2)
        self.proxied1.extend(t2)
        self.assertEqual(self.plain1, self.proxied1, 'Lists should be the same')

    def testIndex(self):

        t = TFile(name='foo')
        self.proxied1.insert(8, t)
        self.assertEqual(self.proxied1[8], t)
        self.assertEqual(self.proxied1.index(t), 8)

    def testInsert(self):

        t = TFile(name='foo')
        self.proxied1.insert(8, t)
        self.assertEqual(self.proxied1[8], t)

    def testPop(self):

        list_len = len(self.proxied1)

        t = self.proxied1[-1]
        r = self.proxied1.pop()
        self.assertEqual(t, r)
        self.assertIs(t, r)
        self.assertEqual(len(self.proxied1), list_len - 1)
        self.assertNotIn(t, self.proxied1)
        self.assertNotIn(t._impl, self.proxied1._impl)

        t = self.proxied1[6]
        r = self.proxied1.pop(6)
        self.assertEqual(t, r)
        self.assertIs(t, r)
        self.assertEqual(len(self.proxied1), list_len - 2)
        self.assertNotIn(t, self.proxied1)
        self.assertNotIn(t._impl, self.proxied1._impl)

    def testRemove(self):

        t = TFile(name='bar')
        self.proxied1.insert(7, t)
        list_len = len(self.proxied1)

        self.proxied1.remove(t)

        self.assertEqual(len(self.proxied1), list_len - 1)
        self.assertNotIn(t, self.proxied1)
        self.assertNotIn(t._impl, self.proxied1._impl)

    def testIter(self):

        count = 0
        for f in self.proxied1:
            count += 1
            self.assertTrue(isProxy(f))
            self.assertTrue(isType(f, TFile))

        self.assertEqual(count, len(self.proxied1), 'Must visit every member')

    def testCmp(self):

        self.assertEqual(cmp(self.proxied1, self.proxied2), cmp(self.plain1, self.plain2))

    def testHash(self):

        try:
            hash(int(self.proxied1))
            assert False, 'Lists are not hashable'
        except TypeError:
            pass
