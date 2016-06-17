from __future__ import absolute_import

import time

from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestLazyLoading(GangaUnitTest):
    """A set of tests to cover both ITask and CoreTask plus related objects"""

    argList = ['300', '300', '300']
    extra_opts = ('Tasks', 'disableTaskMon', True)

    def setUp(self):
        """Make sure that the Tasks object isn't destroyed between tests"""
        super(TestLazyLoading, self).setUp([TestLazyLoading.extra_opts])
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')
        setConfigOption('Tasks', 'TaskLoopFrequency', 1)
        self._numTasks = 25

    def createTask(self):
        """create a task with some defaults"""
        from Ganga.GPI import tasks, CoreTask, CoreTransform, Executable, GenericSplitter
        t = CoreTask()

        trf = CoreTransform()
        trf.application = Executable(exe='sleep')
        trf.unit_splitter = GenericSplitter()
        trf.unit_splitter.attribute = "application.args"
        trf.unit_splitter.values = TestLazyLoading.argList

        t.appendTransform(trf)
        t.float = 20

        return t

    def test_a_TaskCreation(self):
        """ First construct a Task object with Transform, etc."""
        from Ganga.Utility.Config import getConfig
        from Ganga.GPI import tasks, CoreTask, CoreTransform, Executable, GenericSplitter
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])
        from Ganga.GPIDev.Base.Proxy import isType

        t = self.createTask()

        t.run()

        assert t.status != 'new'

    def test_b_TaskNotLoaded(self):
        """ Test that task isn't loaded without Mon loop """
        from Ganga.GPI import tasks
        from Ganga.GPIDev.Base.Proxy import stripProxy
        t = tasks(0)

        assert not stripProxy(t)._fullyLoadedFromDisk()

    def test_c_taskLoadedWhenNEeded(self):
        """ Test that task is correctly loaded without Mon loop """

        from Ganga.GPI import tasks
        from Ganga.GPIDev.Base.Proxy import stripProxy
        t = tasks(0)
        tr = tasks(0).transforms[0]

        assert stripProxy(tr)._fullyLoadedFromDisk()

        # now re-enable monitor for next test
        self.extra_opts = ('Tasks', 'disableTaskMon', False)

    def test_d_taskLoadedByMonitor(self):
        """ Test that task is correctly loaded by Mon loop """

        from Ganga.GPI import tasks
        from Ganga.GPIDev.Base.Proxy import stripProxy

        t = tasks(0)

        i=0
        while not stripProxy(t)._fullyLoadedFromDisk():
            time.sleep(0.5)
            if i>60:
                break
            i+=1

        assert stripProxy(t)._fullyLoadedFromDisk()
