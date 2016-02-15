
from unittest import TestCase

from Ganga.GPIDev.Base.Objects import Node, GangaObject, ObjectMetaclass


class TestGangaObject(TestCase):

    def setUp(self):
        self.obj = GangaObject()

    def test_getReadAccess(self):
        self.obj._getReadAccess()

    def test_getWriteAccess(self):
        self.obj._getWriteAccess()

    def test_releaseWriteAccess(self):
        self.obj._releaseWriteAccess()

    def test_setRegistry(self):
        self.obj._setRegistry(None)

    def test_getRegistryID(self):
        print self.obj._getRegistryID()

    def test_setDirty(self):
        self.obj._setDirty()

    def test_setFlushed(self):
        self.obj._setFlushed()

    def test_declared_property(self):
        self.obj._declared_property('NotAProperty')

    def testGetJobObject(self):
        try:
            self.obj.getJobObject()
            raise Exception("didn't expect to have a Job")
        except AssertionError:
            pass
        except:
            raise

    def test_attribute_filter__set__(self):
        self.obj._attribute_filter__set__('a', 1)

    def testCopy(self):
        from copy import deepcopy
        myTestObj = deepcopy(self.obj)

    def test_readonly(self):
        self.obj._readonly()

    def test__getstate__(self):
        self.obj.__getstate__()

    def testClone(self):
        temp = self.obj.clone()

    def testCopyFrom(self):
        temp = GangaObject()
        temp.copyFrom(self.obj)

    def testPrintTree(self):
        self.obj.printTree()

    def testPrintPrepTree(self):
        self.obj.printPrepTree()

    def testPrintSummaryTree(self):
        self.obj.printSummaryTree()

    def test__eq__(self):
        self.obj == GangaObject()

    def test__ne__(self):
        self.obj != GangaObject()

    def tearDown(self):
        pass


class TestNode(TestCase):

    def setUp(self):
        self.obj = Node(None)

    def test__copy__(self):
        temp = self.obj

    def test_getParent(self):
        self.obj._getParent()

    def test_setParent(self):
        self.obj._setParent(None)

    def test_getRoot(self):
        self.obj._getRoot()

#    def testAccept(self):
#        self.obj.accept(visitor)

    def testClone(self):
        temp = self.obj.clone()

    def testCopyFrom(self):
        temp = Node(None)
        temp.copyFrom(self.obj)

    def testPrintTree(self):
        self.obj.printTree()

    def tearDown(self):
        pass


class TestObjectMetaclass(TestCase):

    def testObjectMetaclass(self):
        class GangaObject(Node):
            __metaclass__ = ObjectMetaclass
            _schema = None
            _category = None
            _hidden = True

            @classmethod
            def _declared_property(cls, name):
                return '_' + name in cls.__dict__
