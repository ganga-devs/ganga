from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

from os import path, stat, unlink

import time

from .utilFunctions import getJobsPath, getXMLDir, getXMLFile, getSJXMLFile, getSJXMLIndex, getIndexFile

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
        assert len(jobs) == 1

    def test_b_JobXMLExists(self):
        # Check things exist
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
        # Check they get updated
        from Ganga.GPI import jobs

        j=jobs(0)

        XMLFileName = getXMLFile(j)

        last_update = stat(XMLFileName)

        j.name = testStr

        from Ganga.Utility.Config import getConfig
        flush_timeout = getConfig('Registry')['AutoFlusherWaitTime']
        time.sleep(2.*flush_timeout)

        newest_update = stat(XMLFileName)

        assert newest_update.st_mtime > last_update.st_mtime


    def test_d_XMLUpdated(self):
        # Check they get updated elsewhere
        from Ganga.GPI import jobs, disableMonitoring

        disableMonitoring()

        j=jobs(0)

        XMLFileName = getXMLFile(j)

        last_update = stat(XMLFileName)

        j.submit()

        newest_update = stat(XMLFileName)

        assert len(j.subjobs) == len(testArgs)

        assert newest_update.st_mtime > last_update.st_mtime

    def test_e_SubJobXMLExists(self):
        # Check other XML exit
        from Ganga.GPI import jobs

        assert len(jobs) == 1

        j=jobs(0)

        assert path.isdir(getXMLDir(j))

        assert path.isfile(getSJXMLIndex(j))

        for sj in j.subjobs:
            XMLFileName = getSJXMLFile(sj)
            assert path.isfile(XMLFileName)
            assert path.isfile(XMLFileName+'~')

    def test_f_testXMLContent(self):
        # Check their content
        from Ganga.Core.GangaRepository.VStreamer import to_file, from_file

        from Ganga.GPI import jobs, Job
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from tempfile import NamedTemporaryFile

        j=jobs(0)
        XMLFileName = getXMLFile(j)
        assert path.isfile(XMLFileName)
        handler = open(XMLFileName)
        tmpobj, errs = from_file(handler)

        assert hasattr(tmpobj, 'name')

        assert tmpobj.name == testStr

        new_temp_file = NamedTemporaryFile(delete=False)
        temp_name = new_temp_file.name
        ignore_subs = 'subjobs'

        to_file(stripProxy(j), new_temp_file, ignore_subs)
        new_temp_file.flush()
        new_temp_file.close()

        new_temp_file2 = NamedTemporaryFile(delete=False)
        temp_name2 = new_temp_file2.name

        j2=Job()
        j2.name=testStr

        to_file(stripProxy(j2), new_temp_file2, ignore_subs)
        new_temp_file2.flush()
        new_temp_file2.close()

        #import filecmp
        #assert filecmp.cmp(XMLFileName, temp_name)
        #assert not filecmp.cmp(temp_name, temp_name2)

        assert open(XMLFileName).read() == open(temp_name).read()
        assert open(temp_name).read() != open(temp_name2).read()

        handler.close()

        unlink(temp_name)
        unlink(temp_name2)

    def test_g_testSJXMLContent(self):
        # Check SJ content
        from Ganga.Core.GangaRepository.VStreamer import to_file, from_file

        from Ganga.GPI import jobs
        from tempfile import NamedTemporaryFile
        from Ganga.GPIDev.Base.Proxy import stripProxy

        counter = 0
        for sj in jobs(0).subjobs:
            XMLFileName = getSJXMLFile(sj)
            assert path.isfile(XMLFileName)

            ignore_subs = 'subjobs'

            handler = open(XMLFileName)
            tmpobj, errs = from_file(handler)
            assert hasattr(tmpobj, 'id')
            assert tmpobj.id == counter

            new_temp_file = NamedTemporaryFile(delete=False)
            temp_name = new_temp_file.name
            to_file(stripProxy(sj), new_temp_file, ignore_subs)
            new_temp_file.flush()
            new_temp_file.close()
            #import filecmp
            #assert filecmp.cmp(XMLFileName, temp_name)
            assert open(XMLFileName).read() == open(temp_name).read()
            handler.close()
            unlink(temp_name)

            counter+=1

        assert counter == len(jobs(0).subjobs)

    def test_h_testXMLIndex(self):
        # Check index of job
        from Ganga.Core.GangaRepository.PickleStreamer import to_file, from_file

        from Ganga.GPI import jobs

        j = jobs(0)

        assert path.isfile(getIndexFile(j))

        handler = open(getIndexFile(j))
        obj, errs = from_file(handler)

        assert isinstance(obj, tuple)

        from Ganga.GPIDev.Base.Proxy import stripProxy, getName
        raw_j = stripProxy(j)
        index_cache = raw_j._getRegistry().getIndexCache(raw_j)
        assert isinstance(index_cache, dict)

        index_cls = getName(raw_j)
        index_cat = raw_j._category
        this_index_cache = (index_cat, index_cls, index_cache)

        assert this_index_cache == obj

        handler.close()

    def test_i_testSJXMLIndex(self):
        # Check index of all sj
        from Ganga.Core.GangaRepository.PickleStreamer import to_file, from_file

        from Ganga.GPI import jobs

        assert len(jobs) == 2

        j=jobs(0)

        handler = open(getSJXMLIndex(j))
        obj, errs = from_file(handler)

        assert isinstance(obj, dict)

        from Ganga.GPIDev.Base.Proxy import stripProxy, getName
        raw_j = stripProxy(j)

        new_dict = {}
        for sj in j.subjobs:
            raw_sj = stripProxy(sj)
            temp_index = raw_sj._getRegistry().getIndexCache(raw_sj)

            new_dict[sj.id] = temp_index
            assert raw_sj._category == raw_j._category

        for k, v in new_dict.iteritems():
            for k1, v1 in v.iteritems():
                if k1 != 'modified':
                    assert obj[k][k1] == new_dict[k][k1]

        #assert obj == new_dict

        handler.close()

