from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

job_num = 5
job_names = ['a', 'b', 'c', 'd', 'e']

class TestSelect(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestSelect, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from Ganga.Utility.Config import getConfig
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from Ganga.GPI import Job, jobs
        for i in range(job_num):
            j = Job()
            j.name = job_names[i]
        self.assertEqual(len(jobs), job_num) # Don't really gain anything from assertEqual...

    def test_b_SelectTests(self):
        """ Check select methods"""
        from Ganga.GPI import jobs, Executable

        self.assertEqual(len(jobs), job_num)

        mySlice = jobs.select(application=Executable)

        assert len(mySlice) == job_num

        mySlice2 = jobs.select(id=1)

        assert len(mySlice2) == 1
        assert mySlice2[1].id == 1

        for i in range(job_num):

            mySlice3 = jobs.select(name=job_names[i])

            assert len(mySlice3) == 1
            assert mySlice3[i].name == job_names[i]

    def test_c_SelectSJTests(self):
        """ Is is a bird is it a plane... no it's a test for selecting subjobs now"""
        from Ganga.GPI import jobs, Job, ArgSplitter
        
        j=Job(splitter=ArgSplitter(args=job_names))
        j.submit()
        
        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(j, 60)

        assert j.status == "completed"

        mySlice = jobs(j.id).subjobs.select(status="completed")

        assert len(mySlice) == len(job_names)

        mySlice2 = jobs(j.id).subjobs.select(id=2)

        assert len(mySlice2) == 1
        assert mySlice2[2].id == 2

