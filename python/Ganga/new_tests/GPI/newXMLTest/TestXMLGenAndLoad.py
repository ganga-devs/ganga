from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

from os import path, stat

import time

def getJobsPath():
    from Ganga.Runtime.Repository_runtime import getLocalRoot
    jobs_path = path.join(getLocalRoot(), '6.0', 'jobs')
    return jobs_path

def getXMLDir(this_job):
    jobs_path = getJobsPath()
    _id = this_job.id
    jobs_master_path = path.join(jobs_path, "%sxxx" % str(int(_id/1000)))
    return path.join(jobs_master_path, str(_id))

def getXMLFile(this_job):
    return path.join(getXMLDir(this_job), 'data')

def getIndexFile(this_job):
    return path.join(getXMLDir(this_job), '../%s.index' % str(this_job.id))

testStr = "testFooString"

class TestXMLGenAndLoad(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestXMLGenAndLoad, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from Ganga.Utility.Config import getConfig
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from Ganga.GPI import Job, jobs
        j=Job()
        self.assertEqual(len(jobs), 1) # Don't really gain anything from assertEqual...

    def test_b_JobXMLExists(self):
        """ Second get the job and check that getting it via jobs doesn't cause it to be loaded"""
        from Ganga.GPI import jobs

        assert len(jobs) == 1

        print("len: %s" % str(len(jobs)))

        j=jobs(0)

        assert path.isdir(getJobsPath())

        assert path.isfile(path.join(getJobsPath(), 'cnt'))

        assert path.isdir(getXMLDir(j))

        assert path.isfile(getXMLFile(j))

        assert path.isfile(getXMLFile(j) + '~')

        assert path.isfile(getIndexFile(j))

    def test_c_XMLAutoUpdated(self):

        from Ganga.GPI import jobs

        j=jobs(0)

        last_update = stat(getXMLFile(j))

        j.name = testStr

        from Ganga.Utility.Config import getConfig
        flush_timeout = getConfig('Registry')['AutoFlusherWaitTime']
        time.sleep(2.*flush_timeout)

        newest_update = stat(getXMLFile(j))

        assert newest_update.st_mtime > last_update.st_mtime


    def test_d_XMLUpdated(self):

        from Ganga.GPI import jobs, disableMonitoring

        disableMonitoring()

        j=jobs(0)

        last_update = stat(getXMLFile(j)) 

        j.submit()

        newest_update = stat(getXMLFile(j))

        from GangaTest.Framework.utils import sleep_until_completed

        sleep_until_completed(j)

        final_update = stat(getXMLFile(j))

        assert newest_update.st_mtime > last_update.st_mtime
        assert final_update.st_mtime > newest_update.st_mtime

    def test_e_testXMLContent(self):

        from Ganga.Core.GangaRepository.VStreamer import to_file, from_file

        from Ganga.GPI import jobs, Job
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from tempfile import NamedTemporaryFile

        j = jobs(0)
        assert path.isfile(getXMLFile(j))
        handler = open(getXMLFile(j))
        tmpobj, errs = from_file(handler)

        assert hasattr(tmpobj, 'name')

        assert tmpobj.name == testStr

        new_temp_file = NamedTemporaryFile()

        ignore_subs = ''

        to_file(stripProxy(j), new_temp_file, ignore_subs)

        new_temp_file2 = NamedTemporaryFile()

        j2 = Job()
        j2.name = testStr
        j2.submit()
        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(j2)

        to_file(stripProxy(j2), new_temp_file2, ignore_subs)

        import filecmp

        assert filecmp.cmp(handler.name, new_temp_file.name)
        assert not filecmp.cmp(new_temp_file.name, new_temp_file2.name)
        handler.close()

    def test_f_testXMLIndex(self):

        from Ganga.Core.GangaRepository.PickleStreamer import to_file, from_file

        from Ganga.GPI import jobs

        j = jobs(0)

        assert path.isfile(getIndexFile(j))

        handler = open(getIndexFile(j))
        _obj, errs = from_file(handler)
        obj=_obj[0]
        print("_obj: %s" % str(_obj))

        assert isinstance(obj, list)

        from Ganga.GPIDev.Lib.Registry.JobRegistry import getIndexCache

        assert isinstance(getIndexCache(j), list)

        assert getIndexCache(j) == obj

        handler.close()

