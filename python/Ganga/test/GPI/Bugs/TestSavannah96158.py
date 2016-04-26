from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah96158(GangaUnitTest):
    def test_Savannah96158(self):
        from Ganga.GPI import Job, jobs

        #The first two tests check the new functionality, the remainder just check that we didn't break existing functionality with this bug-fix
        a = Job()
        a.name = 'TestName'
        tmpList = jobs.select(name='*stN*')
        self.assertEqual(len(tmpList), 1, 'Test 1: jobs.select using wildcard returned unexpected number of results')

        a = Job()
        a.name = 'ekdicjsheeksoawoq1'
        a = Job()
        a.name = 'ekdicjsheeksoawoq2'
        a = Job()
        a.name = 'ekdicjsheeksoawoq3'
        a = Job()
        a.name = 'ekdicjsheeksoawoq4'
        tmpList = jobs.select(name='ekdicjsheeksoawoq?')
        self.assertEqual(len(tmpList), 4, 'Test 2: jobs.select using wildcard returned unexpected number of results')

        jobs.select(1)
        jobs.select(1, 4)
        jobs.select(status='new')
        jobs.select(backend='Local')
        jobs.select(application='Executable')
