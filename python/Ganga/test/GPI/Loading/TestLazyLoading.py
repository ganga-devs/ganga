from __future__ import absolute_import

import pytest

from Ganga.GPIDev.Base.Proxy import stripProxy

from Ganga.testlib.decorators import add_config

from Ganga.Core.exceptions import GangaAttributeError

@add_config([('TestingFramework', 'AutoCleanup', 'False')])
@pytest.mark.usefixtures('gpi')
class TestLazyLoading(object):

    def test_a_JobConstruction(self):
        """ First construct the Job object"""

        from Ganga.GPI import Job, jobs

        from Ganga.Utility.Config import getConfig
        assert not getConfig('TestingFramework')['AutoCleanup']

        j = Job()
        assert len(jobs) == 1

    def test_b_JobNotLoaded(self):
        """S econd get the job and check that getting it via jobs doesn't cause it to be loaded"""

        from Ganga.GPI import jobs

        assert len(jobs) == 1

        j = jobs(0)

        raw_j = stripProxy(j)

        has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

        assert not has_loaded_job

    def test_c_JobLoaded(self):
        """ Third do something to trigger a loading of a Job and then test if it's loaded"""

        from Ganga.GPI import jobs

        assert len(jobs) == 1

        j = jobs(0)

        raw_j = stripProxy(j)

        # Any command to load a job can be used here
        raw_j.printSummaryTree()

        has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

        assert has_loaded_job

    def test_d_GetNonSchemaAttr(self):
        """ Don't load a job looking at non-Schema objects"""

        from Ganga.GPI import jobs

        raw_j = stripProxy(jobs(0))

        assert not raw_j._getRegistry().has_loaded(raw_j)

        dirty_status = raw_j._dirty

        assert not dirty_status

        assert not raw_j._getRegistry().has_loaded(raw_j)

        with pytest.raises(GangaAttributeError):
            _ = jobs(0)._dirty

        assert not raw_j._getRegistry().has_loaded(raw_j)

        raw_j.printSummaryTree()

        assert raw_j._getRegistry().has_loaded(raw_j)
