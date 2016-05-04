try:
    import unittest2 as unittest
except ImportError:
    import unittest

import threading
import time
import random


from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from Ganga.GPIDev.Base.Objects import Node, GangaObject, ObjectMetaclass


class TestGangaObject(unittest.TestCase):

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


class TestNode(unittest.TestCase):

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


class TestObjectMetaclass(unittest.TestCase):

    def testObjectMetaclass(self):
        class GangaObject(Node):
            __metaclass__ = ObjectMetaclass
            _schema = None
            _category = None
            _hidden = True

            @classmethod
            def _declared_property(cls, name):
                return '_' + name in cls.__dict__


class _ExcThread(threading.Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, verbose=None):
        super(_ExcThread, self).__init__(group, target, name, args, kwargs, verbose)
        if kwargs is None:
            kwargs = {}
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.exc = None
        self.daemon = True

    def run(self):
        try:
            self.target(*self.args, **self.kwargs)
        except Exception:
            import sys
            self.exc = sys.exc_info()
        finally:
            del self.target, self.args, self.kwargs

    def join(self, *args, **kwargs):
        threading.Thread.join(self, *args, **kwargs)
        if self.exc:
            raise self.exc[0], self.exc[1], self.exc[2]


class MultiThreadedTestCase(unittest.TestCase):
    """
    This is a specialisation of the ``unittest.TestCase`` to provide a function which can run multiple threads at once.
    These sorts of stress tests work best if each thread is relatively long-lived so make sure they either call a long-running function or loop many times within the function.
    In your ``test_foo`` function you should define one or more functions which you want to be run in threads and then pass them to ``self.run_threads``.
    Each function should take a single argument which is the thread number

    For example::

        class TestClass(MultiThreadedTestCase):
            def test_read_write(self):

                def a(thread_number):
                    something_hopefully_threadsafe()

                def b(thread_number):
                    for _ in range(100):
                        something_quicker_but_hopefully_threadsafe()

                self.run_threads([a, b])

    The timeout duration and the number of parallel threads to run can be customised
    and any exception raised by the thread will be correctly propagated to the main thread with correct traceback for test failure reporting.
    """

    def run_threads(self, functions=None, num_threads=50, timeout=60):
        """
        Args:
            functions: a list of functions which will be randomly chosen to run in threads. They will take one argument which is an integer thread id
            num_threads: the number of total threads to run for this test case
            timeout: When joining the threads at the end, how long to have as a timeout on each one.
        Raises:
            RuntimeError: if all the threads have not finished by the time the timeout ends.
        """
        if functions is None:
            functions = []

        # Start the threads where each one is running a function randomly chosen from ``threads``
        threads = []
        for i in range(num_threads):
            target = random.choice(functions)
            t = _ExcThread(target=target, args=[i])
            t.name = t.name + '-' + target.__name__
            threads.append(t)
            t.start()

        # Try to wait for each thread to finish
        start = cur_time = time.time()
        while cur_time <= (start + timeout):
            for thread in threads:
                # If it's still running, try to finish it
                thread.join(timeout=0)
            if all(not t.is_alive() for t in threads):
                break  # If we're al done, finish the loop
            time.sleep(0.1)
            cur_time = time.time()
        else:
            # If we hit the timeout without all the threads finishing, raise an exception.
            still_running = [t for t in threads if t.is_alive()]
            num_threads = len(still_running)
            names = [t.name for t in still_running]
            raise RuntimeError('Timeout while waiting for {0} threads to finish: {1}'.format(num_threads, names))


class SimpleGangaObject(GangaObject):
    _schema = Schema(Version(1, 0), {
        'a': SimpleItem(42, typelist=[int]),
    })
    _category = 'TestGangaObject'
    _hidden = True
    _enable_plugin = True


class ThreadedTestGangaObject(GangaObject):
    _schema = Schema(Version(1, 0), {
        'a': SimpleItem(42, typelist=[int]),
        'b': ComponentItem('TestGangaObject', defvalue='SimpleGangaObject'),
    })
    _category = 'TestGangaObject'
    _hidden = True
    _enable_plugin = True


class TestThreadSafeGangaObject(MultiThreadedTestCase):
    """
    This test is to check that ``GangaObject`` is thread-safe as far as possible.
    """

    def test_read_write(self):
        """
        This tests whether claiming a lock on the object stops other threads
        overwriting our value. It starts two types of thread:

        1. one which acquires a lock for its duraction, sets values and then
        makes sure they are unchanged
        2. one which sets the value being checked in thread-type 1 to random
        values.
        """
        o = ThreadedTestGangaObject()
        random.seed(time.gmtime())

        def write_read(thread_number):
            rand = random.Random()
            rand.seed(time.clock() + thread_number)
            for _ in range(100):
                with o.const_lock:
                    num = rand.randint(0, 1000)
                    o.a = num
                    time.sleep(rand.uniform(0, 1E-6))
                    assert o.a == num

        def write(thread_number):
            for _ in range(100):
                o.a = -1  # Not in the range being set by other threads

        self.run_threads([write_read, write])

    def test_read_write_parent(self):
        """
        Are parent locks held correctly?

        We make a parent and a child and then check whether claiming an
        explicit lock on the child will stop changes to the parent.

        This is similar to ``test_read_write`` except it is working on a child
        object and making sure the lock holds.
        """
        o = ThreadedTestGangaObject()
        o.b = SimpleGangaObject()
        child = o.b
        random.seed(time.gmtime())

        def write_read(thread_number):
            rand = random.Random()
            rand.seed(time.clock() + thread_number)
            for _ in range(100):
                with child.const_lock:
                    num = rand.randint(0, 1000)
                    o.a = num
                    child_num = rand.randint(0, 1000)
                    o.b.a = child_num
                    time.sleep(rand.uniform(0, 1E-6))
                    assert o.a == num
                    assert o.b.a == child_num
                    assert o.b is child

        def write(thread_number):
            for _ in range(100):
                o.a = -1  # Not in the range being set by other threads
                o.b.a = -1

        self.run_threads([write_read, write])

    def test_coarse_locks(self):
        """
        Make sure that the coarse locks work

        In each thread we grab sole access of the object via its ``const_lock``
        property and do things to it.
        This is required for the cases where you need to do multiple things to
        an object before allowing anyone else to.
        """
        o = ThreadedTestGangaObject()

        def change(thread_number):
            rand = random.Random()
            rand.seed(time.clock() + thread_number)
            for _ in range(10):  # Run this thread many times to keep it running for long enough to see problems.
                with o.const_lock:
                    num = rand.randint(0, 1000)
                    o.a = num
                    time.sleep(rand.uniform(0, 1E-6))
                    assert o.a == num

                with o.const_lock:
                    o.b = rand.choice([ThreadedTestGangaObject, SimpleGangaObject])()
                    child_num = rand.randint(0, 1000)
                    o.b.a = child_num
                    time.sleep(rand.uniform(0, 1E-6))
                    assert o.b.a == child_num

                    if isinstance(o.b, ThreadedTestGangaObject):
                        o.b.b.a = rand.choice([ThreadedTestGangaObject, SimpleGangaObject])()
                        num = rand.randint(0, 1000)
                        o.b.b.a = num
                        time.sleep(rand.uniform(0, 1E-6))
                        assert o.b.b.a == num

                with o.const_lock:
                    num = rand.randint(0, 1000)
                    o.b.a = num
                    time.sleep(rand.uniform(0, 1E-6))
                    assert o.b.a == num

        self.run_threads([change])
