from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

class TestSelect(GangaUnitTest):

    def test_SelectJob(self):
        
        from Ganga.GPI import Job, jobs
        j=Job()

        job_slice =  jobs.select(status='new')

        assert len(job_slice) == 1

        j.submit()

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(j)

        assert j.status == 'completed'

        job_slice2 =  jobs.select(status='completed')

        assert len(job_slice2) == 1

    def test_SelectSubJob(self):

        from Ganga.GPI import Job, jobs, ArgSplitter
        j=Job(splitter=ArgSplitter(args=[[1],[2],[3]]))

        assert len(j.subjobs) == 0

        j.submit()

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(j)

        assert j.status == 'completed'

        assert len(j.subjobs) == 3

        job_slice =  j.subjobs.select(status='completed')


        assert len(job_slice) == len(j.subjobs)


