import unittest

from Ganga.GPIDev.Base.Proxy import getProxyClass
from . import TFile

TFile = getProxyClass(TFile)

class TestTFile(unittest.TestCase):

    def testEqualityName(self):
        t1 = TFile(name='foo')
        t2 = TFile(name='foo')
        assert t1 == t2, 'These two files should be equal.'

    def testEqualityNameInvert(self):
        t1 = TFile(name='foo')
        t2 = TFile(name='bar')
        assert t1 != t2, 'These two files should not be equal.'

    def testEqualitySubDir(self):
        t1 = TFile(name='foo')
        t2 = TFile(name='foo', subdir='..')
        assert t1 != t2, 'These two files should not be equal.'

    def testHash(self):
        t1 = TFile(name='foo')
        t2 = TFile(name='foo')
        assert t1 == t2
        assert hash(t1) == hash(t2)

    def testHashNotEqual(self):
        t1 = TFile(name='foo', subdir='..')
        t2 = TFile(name='foo')
        assert t1 != t2
        assert t1.__hash__() != t2.__hash__()

    def testCmpEqual(self):

        t1 = TFile(name='foo')
        t2 = TFile(name='foo')

        assert cmp(t1, t2) == 0, 'Files are equal'

    def testCmpOrder(self):

        # I've inverted the order here so that the references are not in
        # the same order as the strings.
        t2 = TFile(name='def')
        t1 = TFile(name='abc')

        assert cmp(t1, t2) < 0
        assert cmp(t2, t1) > 0

    def testCmpOrderSubdir(self):

        t2 = TFile(name='abc', subdir='a')
        t1 = TFile(name='abc')

        assert cmp(t1, t2) < 0
        assert cmp(t2, t1) > 0

    def testCmpList(self):

        a = [TFile(name='abc'), TFile(name='def')]
        b = [TFile(name='abc'), TFile(name='def'), TFile(name='ijk')]

        assert cmp(a, b) < 0

        a.append(TFile(name='ijk'))
        assert cmp(a, b) == 0, 'Lists should now be the same.'

    def testIAdd(self):

        t1 = TFile(name='abc')
        t2 = TFile(name='def')

        t1 += t2

        assert t1.added, '__iadd__ not called'

    def testIAddProxy(self):

        t1 = TFile(name='abc')
        t2 = TFile(name='def')

        t1 += t2

        assert t1.added, '__iadd__ not called'
        assert not t2.added, '__iadd__ not called'

    def testIMul(self):

        t1 = TFile(name='abc')

        t1 *= 3

        assert t1.multiplied, '__imul__ not called'

    def testIMulProxy(self):

        t1 = TFile(name='abc')
        t2 = TFile(name='def')

        t1 *= t2

        assert t1.multiplied, '__imul__ not called'
        assert not t2.multiplied, '__imul__ not called'
