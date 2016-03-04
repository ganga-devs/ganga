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

def getSJXMLIndex(this_sj):
    return path.join(getXMLDir(this_sj), 'subjobs.idx')

def getSJXMLFile(this_sj):
    return path.join(getXMLDir(this_sj), '%s' % str(this_sj.id), 'data')

def getIndexFile(this_job):
    return path.join(getXMLDir(this_job), '../%s.index' % str(this_job.id))

testStr = "testFooString"
testArgs = [[1],[2],[3],[4],[5]]

class TestSJXMLGenAndLoad(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestSJXMLGenAndLoad, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from Ganga.Utility.Config import getConfig
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from Ganga.GPI import Job, jobs, ArgSplitter
        j=Job(splitter=ArgSplitter(args=testArgs))
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

        assert len(j.subjobs) == len(testArgs)

        assert newest_update.st_mtime > last_update.st_mtime

    def test_e_SubJobXMLExists(self):
        """ Second get the job and check that getting it via jobs doesn't cause it to be loaded"""
        from Ganga.GPI import jobs

        assert len(jobs) == 1

        j=jobs(0)

        assert path.isdir(getXMLDir(j))

        assert path.isfile(getSJXMLIndex(j))

        for sj in j.subjobs:
            assert path.isfile(getSJXMLFile(j))
            assert path.isfile(getSJXMLFile(j)+'~')

    def test_f_testXMLContent(self):

        from Ganga.Core.GangaRepository.VStreamer import to_file, from_file

        from Ganga.GPI import jobs, Job
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from tempfile import NamedTemporaryFile

        j=jobs(0)
        assert path.isfile(getXMLFile(j))
        handler = open(getXMLFile(j))
        tmpobj, errs = from_file(handler)

        assert hasattr(tmpobj, 'name')

        assert tmpobj.name == testStr

        new_temp_file = NamedTemporaryFile()

        ignore_subs = ''

        to_file(stripProxy(j), new_temp_file, ignore_subs)

        new_temp_file2 = NamedTemporaryFile()

        j2=Job()
        j2.name=testStr

        to_file(stripProxy(j2), new_temp_file2, ignore_subs)

        import filecmp

        assert filecmp.cmp(handler.name, new_temp_file.name)
        assert filecmp.cmp(new_temp_file.name, new_temp_file2.name)
        handler.close()

    def test_g_testSJXMLContent(self):

        from Ganga.Core.GangaRepository.VStreamer import to_file, from_file

        from Ganga.GPI import jobs

        counter = 0
        for sj in jobs(0).subjobs:
            assert path.isfile(getSJXMLFile(sj))

            ignore_subs = ''

            handler = open(getSJXMLFile(sj))
            tmpobj, errs = from_file(handler)
            assert hasattr(tmpobj, 'id')
            assert tmpobj.id == counter

            new_temp_file = NamedTemporaryFile()
            to_file(stripProxy(j), new_temp_file, ignore_subs)

            import filecmp
            assert filecmp.cmp(handler.name, new_temp_file.name)
            handler.close()

            counter+=1

    def test_h_testXMLIndex(self):

        from Ganga.Core.GangaRepository.PickleStreamer import to_file, from_file

        from Ganga.GPI import jobs

        j=jobs(0)

        assert path.isfile(getIndexFile(j))

        handler = open(getIndexFile(j))
        _obj, errs = from_file(handler)
        obj=_obj[0]

        assert isinstance(obj, list)

        from Ganga.GPIDev.Lib.Registry.JobRegistry import getIndexCache

        assert isinstance(getIndexCache(j), list)

        assert getIndexCache(j) == obj

        handler.close()

    def test_i_testSJXMLIndex(self):

        from Ganga.Core.GangaRepository.PickleStreamer import to_file, from_file

        from Ganga.GPI import jobs

        handler = open(getSJXMLIndex(jobs(0)))
        handler = open(getIndexFile(j))
        _obj, errs = from_file(handler)
        obj=_obj[0]

        assert isinstance(obj, dict)

        from Ganga.GPIDev.Lib.Registry.JobRegistry import getIndexCache

        new_dict = {}
        for sj in jobs(0).subjobs:

            new_dict[sj.id] = getIndexCache(sj)

        assert obj == new_dict

