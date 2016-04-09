from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestCoreTasks(GangaUnitTest):
    """A set of tests to cover both ITask and CoreTask plus related objects"""

    def setUp(self):
        """Make sure that the Tasks object isn't destroyed between tests"""
        super(TestCoreTasks, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_TaskCreation(self):
        """ First construct a Task object with Transform, etc."""
        from Ganga.Utility.Config import getConfig
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from Ganga.GPIDev.Base.Proxy import isType
        from Ganga.GPI import tasks, CoreTask, CoreTransform, Executable, GenericSplitter
        t = CoreTask()

        trf = CoreTransform()
        trf.application = Executable()
        trf.unit_splitter = GenericSplitter()
        trf.unit_splitter.attribute = "application.args"
        trf.unit_splitter.values = ['arg 1', 'arg 2', 'arg 3']

        t.appendTransform(trf)
        t.float = 20

        # Test Task has been setup correctly
        assert len(tasks) == 1
        assert isType(t, CoreTask)
        assert len(t.transforms) == 1
        assert isType(t.transforms[0], CoreTransform)
        assert isType(t.transforms[0].application, Executable)
        assert isType(t.transforms[0].unit_splitter, GenericSplitter)
        assert t.transforms[0].unit_splitter.attribute == "application.args"
        assert t.transforms[0].unit_splitter.values == ['arg 1', 'arg 2', 'arg 3']
        assert t.float == 20

    def test_b_TaskPersistency(self):
        """Check the task is persisted properly"""
        from Ganga.GPIDev.Base.Proxy import isType
        from Ganga.GPI import tasks, CoreTask, CoreTransform, Executable, GenericSplitter

        assert len(tasks) == 1

        # Test Task has been persisted correctly
        t = tasks(0)

        assert len(tasks) == 1
        assert isType(t, CoreTask)
        assert len(t.transforms) == 1
        assert isType(t.transforms[0], CoreTransform)
        assert isType(t.transforms[0].application, Executable)
        assert isType(t.transforms[0].unit_splitter, GenericSplitter)
        assert t.transforms[0].unit_splitter.attribute == "application.args"
        assert t.transforms[0].unit_splitter.values == ['arg 1', 'arg 2', 'arg 3']
        assert t.float == 20

    def test_c_TaskRunning(self):
        """Check the task can be started"""
        from Ganga.GPI import tasks

        assert len(tasks) == 1

        t = tasks(0)
        t.run()

        assert t.status == "running"
        assert t.transforms[0].status == "running"

    def test_d_TaskStillRunning(self):
        """Check the task is still running"""
        from Ganga.GPI import tasks

        assert len(tasks) == 1

        # Test Task has been persisted correctly
        t = tasks(0)

        assert t.status == "running"
        assert t.transforms[0].status == "running"

