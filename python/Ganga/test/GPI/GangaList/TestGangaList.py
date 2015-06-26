from __future__ import absolute_import
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestGangaList.py,v 1.1 2008-07-17 16:41:11 moscicki Exp $
##########################################################################

from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.GPIDev.Base.Proxy import addProxy, getProxyAttr, isProxy, isType

from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
GangaList = GangaList._proxyClass

import random
import string
#from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList

# set the seed for repeatable tests
random.seed(666)

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)


class TestGangaList(GangaGPITestCase):

    def __init__(self):

        self.ganga_list = None

        self.plain1 = []
        self.plain2 = []

        self.proxied1 = []
        self.proxied2 = []

    def _makeRandomString(self):
        str_len = random.randint(3, 10)
        s = ''
        for _ in range(str_len):
            s += random.choice(string.ascii_letters)
        return s

    def _makeRandomTFile(self):
        name = self._makeRandomString()
        subdir = self._makeRandomString()
        return TFile(name=name, subdir=subdir)

    def setUp(self):
        self.ganga_list = GangaList()

        self.plain1 = [self._makeRandomTFile() for _ in range(15)]
        self.plain2 = [self._makeRandomTFile() for _ in range(10)]

        self.proxied1 = GangaList()
        self.proxied1.extend(self.plain1[:])
        self.proxied2 = GangaList()
        self.proxied2.extend(self.plain2[:])

        assert len(getProxyAttr(self.proxied1, '_list')) == len(
            self.plain1), 'Somthings wrong with construction'
        assert len(getProxyAttr(self.proxied2, '_list')) == len(
            self.plain2), 'Somthings wrong with construction'

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

        assert not missing_methods, \
            'Not all of lists methods are implemented: %s' % str(
                missing_methods)

    def testEq(self):
        """Tests the equality op that the rest of the tests rely on."""
        assert self.proxied1 == self.plain1, 'Proxied and non-proxied versions should be the same'

    def testNq(self):
        assert self.proxied1 != None
        assert not self.proxied1 != self.proxied1, 'Lists are the same'
        assert self.proxied1 != self.proxied2, 'Lists are different'
        assert self.plain1 != self.proxied2, 'Lists are different'

    def testNonZero(self):
        """@ExpectedFailure"""
        assert not GangaList(
        ), 'An empty GangaList should be false, just like a list'

    def testAdd(self):
        """Test __add__"""
        assert (self.plain1 + self.plain2) == (self.proxied1 + self.proxied2)
        assert isProxy(self.proxied1 + self.proxied2)

    def testAddMixed(self):
        """Test __add__ with mixed lists and GangaLists"""
        assert (self.plain1 + self.plain2) == (self.proxied1 + self.plain2)
        assert (self.plain2 + self.plain1) == (self.plain2 + self.proxied1)
        assert isProxy(self.proxied1 + self.plain2)

        assert (self.plain2 + self.plain1) == (self.plain2 + self.proxied1)
        assert (self.plain1 + self.plain2) == (self.plain1 + self.proxied2)
        assert isinstance(self.plain1 + self.proxied2, list)

    def testAddMixed2(self):

        self.plain1 = range(10)
        self.plain2 = range(10)

        assert isProxy(self.proxied2[-1]), 'Element access must get proxies'
        assert not isProxy(self.plain1[0]), 'Element access must not proxies'
        assert isProxy(
            (self.plain1 + self.proxied2)[-1]), 'File objects should remain proxies'
        assert not isProxy(
            (self.plain1 + self.proxied2)[0]), 'Objects in plain lists should be left alone'

        assert (
            self.plain1 + self.proxied2)[-1] == self.proxied2[-1], 'File objects should be equal'
        assert (
            self.plain1 + self.proxied2)[-1] is self.proxied2[-1], 'File objects should be identical'

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
        assert plist == self.proxied1

        for p in plist:
            assert isProxy(p)
            assert p in self.proxied1, 'Proxied list should contain each proxied object'

    def testDelItem(self):
        """Test __delitem__"""

        for p in [addProxy(x) for x in self.plain1[:]]:
            assert isProxy(p)
            del self.proxied1[self.proxied1.index(p)]

    def testGE(self):
        """Test __ge__"""

        assert (self.plain1 >= self.plain2) == (
            self.proxied1 >= self.proxied2), 'The lists should have the same ge'
        assert (self.plain2 >= self.plain1) == (
            self.proxied2 >= self.proxied1), 'The lists should have the same ge'

        assert (self.proxied1 >= self.proxied2) != (
            self.proxied2 >= self.proxied1), 'The gt should invert correctly'

    def testGetItem(self):
        """Test __getitem__"""

        for i in range(len(self.proxied1)):
            assert isProxy(self.proxied1[i])

    def testGetSlice(self):
        """Test __getslice__"""

        slices = [(0, 0), (0, len(self.plain1))]

        for s in slices:
            assert self.plain1[s[0]:s[1]] == self.proxied1[
                s[0]:s[1]], 'Slices %s should be the same' % str(s)

        t = self.plain1[:]
        assert t is not self.plain1, 'Slice should be a copy.'
        assert self.plain1[:] is not t

        t = self.proxied1[:]
        assert t is not self.proxied1, 'Slice should be a copy.'
        assert self.proxied1[:] is not t

    def testGT(self):
        """Test __gt__"""

        assert (self.plain1 > self.plain2) == (
            self.proxied1 > self.proxied2), 'The lists should have the same gt'
        assert (self.plain2 > self.plain1) == (
            self.proxied2 > self.proxied1), 'The lists should have the same gt'

        assert (self.proxied1 > self.proxied2) != (
            self.proxied2 > self.proxied1), 'The gt should invert correctly'

    def testIAdd(self):
        """Test __iadd__"""
        assert isProxy(self.proxied1), 'Must be Proxy'
        assert isProxy(self.proxied2), 'Must be Proxy'

        self.plain1 += self.plain2
        self.proxied1 += self.proxied2

        assert self.plain1 == self.proxied1, 'Addition should be the same'
        assert isProxy(self.proxied1), 'Proxy must be added'

    def testIAddMixed(self):
        """Test __iadd__ where we mix lists and GangaLists"""
        assert isProxy(self.proxied1), 'Must be Proxy'
        assert isProxy(self.proxied2), 'Must be Proxy'

        self.plain1 += self.plain2
        self.proxied1 += self.plain2

        assert self.plain1 == self.proxied1, 'Addition should be the same'
        assert isProxy(self.proxied1), 'Proxy must be added'
        assert isinstance(self.plain1, list), 'Must be list instance'

        self.plain2 += self.proxied1
        self.proxied2 += self.proxied1

        assert self.plain2 == self.proxied2, 'Addition should be the same'
        assert isProxy(self.proxied2), 'Proxy must be added'
        assert isinstance(self.plain2, list), 'Must be list instance'

    def testIAddSlice(self):
        """Test __iadd__ on slices"""

        s1 = self.plain1[3:7]
        s1 += self.plain2[2:5]

        s2 = self.proxied1[3:7]
        s2 += self.proxied2[2:5]

        assert s1 == s2, 'Addition should be the same'

        assert not isProxy(s1), 'Proxy Not Needed'
        assert isProxy(s2), 'Proxy Needed'

    def testIdentity(self):
        """Tests we obey list like identity relations."""

        t = self.plain1[4]
        assert self.plain1[4] is t

        t = self.proxied1[4]
        assert self.proxied1[4] is t

    def testIMul(self):
        """Test __imul__"""

        self.plain1 *= 5
        self.proxied1 *= 5

        assert self.plain1 == self.proxied1, 'Multiplication should be the same'
        assert isProxy(self.proxied1), 'Proxy must be added'

    def testIMulSlice(self):
        """Test __imul__ on slices"""

        s1 = self.plain1[3:7]
        s1 *= 6

        s2 = self.proxied1[3:7]
        s2 *= 6

        assert s1 == s2, 'Addition should be the same'

    def testLE(self):
        """Test __le__"""

        assert (self.plain1 <= self.plain2) == (
            self.proxied1 <= self.proxied2), 'The lists should have the same le'
        assert (self.plain2 <= self.plain1) == (
            self.proxied2 <= self.proxied1), 'The lists should have the same le'

        assert (self.proxied1 <= self.proxied2) != (
            self.proxied2 <= self.proxied1), 'The le should invert correctly'

    def testLen(self):
        """Tests __len__"""
        assert len(self.plain1) == len(
            self.proxied1), 'Lengths should be the same'

    def testLT(self):
        """Test __lt__"""

        assert (self.plain1 < self.plain2) == (
            self.proxied1 < self.proxied2), 'The lists should have the same lt'
        assert (self.plain2 < self.plain1) == (
            self.proxied2 < self.proxied1), 'The lists should have the same lt'

        assert (self.proxied1 < self.proxied2) != (
            self.proxied2 < self.proxied1), 'The lt should invert correctly'

    def testMul(self):
        """Test __mul__"""
        assert (self.plain1 * 7) == (self.proxied1 * 7)
        assert isProxy(self.proxied1 * 9)

        for p in self.proxied1:
            assert isProxy(p)

    def testNE(self):
        """Test __ne__"""

        assert self.plain1 != self.plain2
        assert self.proxied1 != self.proxied2, 'Lists should be different'

        assert self.plain1[0:5] != self.plain1[2:7]
        assert self.proxied1[0:5] != self.proxied1[
            2:7], 'Lists should be different'

    def testRMul(self):
        """Test __rmul__"""

        t1 = 5 * self.plain1
        t2 = 5 * self.proxied1

        assert t1 == t2, 'Multiplication should be the same'

    def testReversed(self):
        """Test the __reversed__ feature (new in python 2.4)."""

        from TFile import TFile as tF
        count = len(self.proxied1) - 1
        for i in self.proxied1.__reversed__():
            assert i is self.proxied1[count]
            assert isProxy(i)
            assert isType(i, tF)
            count -= 1

    def testSetItem(self):
        """Test __setitem__"""

        t = TFile(name='foo', subdir='cheese')
        test_index = 7
        self.plain1[test_index] = t
        assert self.plain1[test_index] is t

        self.proxied1[test_index] = t
        assert self.proxied1[test_index] is t
        assert isProxy(self.proxied1[test_index])

    def testSetSliceProxyList(self):

        self.plain1[3:7] = self.plain2[3:7]
        assert self.plain1[3:7] == self.plain2[
            3:7], 'The lists should be equal'
        assert self.plain1[3:7] is not self.plain2[
            3:7], 'The lists should be copies'
        self.plain1[4] = self.plain2[9]
        assert self.plain1[4] != self.plain2[4]

        tmp = self.plain1[2:9]
        assert self.plain1[2:9] is not tmp

        self.proxied1[3:7] = self.proxied2[3:7]
        assert self.proxied1[3:7] == self.proxied2[
            3:7], 'The lists should be equal'
        assert self.proxied1[3:7] is not self.proxied2[
            3:7], 'The lists should be copies'
        self.proxied1[4] = self.proxied2[9]
        assert self.proxied1[4] != self.proxied2[4]

        tmp = self.proxied1[2:9]
        assert self.proxied1[2:9] is not tmp

    def testAppend(self):

        t = addProxy(TFile(name='foo'))

        self.plain1.append(t)
        assert self.plain1[-1] == t
        assert self.plain1.pop() == t

        self.proxied1.append(t)
        assert self.proxied1[-1] == t
        assert self.proxied1[-1] is t, 'Identity Test'
        assert isProxy(self.proxied1[-1]), 'Make sure we get back a proxy'
        assert self.proxied1.pop() == t

    def testExtend(self):

        t1 = [self._makeRandomTFile() for _ in xrange(10)]

        self.plain1.extend(t1)
        self.proxied1.extend(t1)

        assert self.plain1 == self.proxied1, 'Lists should be the same'

        t2 = self.proxied1[4:7]
        assert isProxy(t2)
        from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList as glist
        assert isType(t2, glist)
        self.plain1.extend(t2)
        self.proxied1.extend(t2)
        assert self.plain1 == self.proxied1, 'Lists should be the same'

    def testIndex(self):

        t = addProxy(TFile(name='foo'))
        self.proxied1.insert(8, t)
        assert self.proxied1[8] == t
        assert self.proxied1.index(t) == 8

    def testInsert(self):

        t = addProxy(TFile(name='foo'))
        self.proxied1.insert(8, t)
        assert self.proxied1[8] == t

    def testPop(self):

        list_len = len(self.proxied1)

        t = self.proxied1[-1]
        r = self.proxied1.pop()
        assert t == r
        assert t is r
        assert len(self.proxied1) == list_len - 1
        assert t not in self.proxied1
        assert t._impl not in self.proxied1._impl

        t = self.proxied1[6]
        r = self.proxied1.pop(6)
        assert t == r
        assert t is r
        assert len(self.proxied1) == list_len - 2
        assert t not in self.proxied1
        assert t._impl not in self.proxied1._impl

    def testRemove(self):

        t = addProxy(TFile(name='bar'))
        self.proxied1.insert(7, t)
        list_len = len(self.proxied1)

        self.proxied1.remove(t)

        assert len(self.proxied1) == list_len - 1
        assert t not in self.proxied1
        assert t._impl not in self.proxied1._impl

    def testIter(self):

        from TFile import TFile as tF

        count = 0
        for f in self.proxied1:
            count += 1
            assert isProxy(f)
            assert isType(f, tF)

        assert count == len(self.proxied1), 'Must visit every member'

    def testCmp(self):

        assert cmp(self.proxied1, self.proxied2) == cmp(
            self.plain1, self.plain2)

    def testHash(self):

        try:
            hash(int(self.proxied1))
            assert False, 'Lists are not hashable'
        except TypeError:
            pass
