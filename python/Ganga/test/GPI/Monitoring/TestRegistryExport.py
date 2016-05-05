from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

class TestRegistryExport(GangaUnitTest):

    def test_0_testRegistryAsserts(self):
        from Ganga.GPI import jobs, box, tasks, prep
        from Ganga.GPIDev.Base.Proxy import stripProxy
        from Ganga.Core.GangaRepository import getRegistryProxy, getRegistrySlice, getRegistry
        assert getRegistryProxy('jobs') is jobs
        assert getRegistryProxy('box') is box
        assert getRegistryProxy('tasks') is tasks
        assert getRegistryProxy('prep') is prep

        assert getRegistrySlice('jobs') is stripProxy(jobs)
        assert getRegistrySlice('box') is stripProxy(box)
        assert getRegistrySlice('tasks') is stripProxy(tasks)
        assert getRegistrySlice('prep') is stripProxy(prep)

        assert getRegistry('jobs') is stripProxy(jobs).objects
        assert getRegistry('tasks') is stripProxy(tasks).objects
        assert getRegistry('box') is stripProxy(box).objects

