

from Ganga.testlib.GangaUnitTest import GangaUnitTest

from Ganga.GPIDev.Base.Proxy import stripProxy


class TestSetRegistry(GangaUnitTest):

    def test_job_create(self):
        """
        Check that registries on Jobs are set correctly and that Jobs are
        cloned when placed in the box
        """
        from Ganga.GPI import Job, box

        j = Job()
        assert stripProxy(j)._getRegistry() is not None, 'Proxy jobs should have a registry set'

        raw_j = stripProxy(Job)()
        assert raw_j._getRegistry() is None, 'Raw jobs should have no registry set'

        box.add(j, 'j')
        assert box['j'] is not j, 'Box entries should be cloned on adding'
        assert stripProxy(box['j']) is not stripProxy(j)
        assert stripProxy(box['j'])._getRegistry() is not stripProxy(j)._getRegistry(), 'Should be BoxRegistry and JobRegistry respectively'

    def test_box(self):
        """
        Check that objects which normally have no registry can be assigned one
        """
        from Ganga.GPI import LocalFile, box

        f = LocalFile()
        assert stripProxy(f)._getRegistry() is None

        box.add(f, 'f')
        assert box['f'] is not f
        assert stripProxy(box['f'])._getRegistry() is not None
