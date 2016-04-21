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
        extra_opts = [('Registry', 'AutoFlusherWaitTime', 10)]
        super(TestSJXMLGenAndLoad, self).setUp(extra_opts=extra_opts)
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from Ganga.Utility.Config import getConfig
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from Ganga.GPI import Job, jobs, ArgSplitter
        j=Job(splitter=ArgSplitter(args=testArgs))
        assert len(jobs) == 1
        from Ganga.GPIDev.Base.Proxy import stripProxy
        stripProxy(j)._getRegistry().flush_all()
        stripProxy(j)._setDirty()

    def test_b_JobXMLExists(self):
        # Check things exist
        from Ganga.GPI import jobs

        assert len(jobs) == 1

        print("len: %s" % len(jobs))

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

        total_time=0.
        new_update = 0
        lst_update = last_update.st_mtime
        while total_time < 2.*flush_timeout and new_update <= lst_update:
            total_time+=1.
            time.sleep(1.)
            try:
                new_update = stat(XMLFileName).st_mtime
            except:
                new_update = 0.

        newest_update = stat(XMLFileName)

        assert newest_update.st_mtime > last_update.st_mtime


    def test_d_XMLUpdated(self):
        # Check they get updated elsewhere
        from Ganga.GPI import jobs, disableMonitoring, enableMonitoring

        disableMonitoring()

        j=jobs(0)

        XMLFileName = getXMLFile(j)

        last_update = stat(XMLFileName)

        j.submit()

        newest_update = stat(XMLFileName)

        assert len(j.subjobs) == len(testArgs)

        assert newest_update.st_mtime > last_update.st_mtime

        enableMonitoring()
        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(j)

    def test_e_SubJobXMLExists(self):
        # Check other XML exit
        from Ganga.GPI import jobs
        from Ganga.GPIDev.Base.Proxy import stripProxy

        assert len(jobs) == 1

        j=jobs(0)

        for sj in j.subjobs:
            this_bak = sj.backend
            stripProxy(sj)._setDirty()
        
        stripProxy(stripProxy(j).subjobs).flush()

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
        with open(XMLFileName) as handler:
            tmpobj, errs = from_file(handler)

            assert hasattr(tmpobj, 'name')

            assert tmpobj.name == testStr

            ignore_subs = ['status', 'subjobs', 'time', 'backend', 'id', 'splitter', 'info', 'application']

            with NamedTemporaryFile(delete=False) as new_temp_file:
                temp_name = new_temp_file.name

                to_file(stripProxy(j), new_temp_file, ignore_subs)
                new_temp_file.flush()

            with NamedTemporaryFile(delete=False) as new_temp_file2:
                temp_name2 = new_temp_file2.name

                j2=Job()
                j2.name=testStr

                to_file(stripProxy(j2), new_temp_file2, ignore_subs)
                new_temp_file2.flush()

            #assert open(XMLFileName).read() == open(temp_name).read()
            assert open(temp_name).read() == open(temp_name2).read()

            unlink(temp_name)
            unlink(temp_name2)

    def test_g_testSJXMLContent(self):
        # Check SJ content
        from Ganga.Core.GangaRepository.VStreamer import to_file, from_file

        from Ganga.GPI import jobs
        from tempfile import NamedTemporaryFile
        from Ganga.GPIDev.Base.Proxy import stripProxy

        ignore_subs = ['subjobs', 'time', 'backend', 'id', 'splitter', 'info', 'application', 'inputdata']

        with NamedTemporaryFile(delete=False) as new_temp_file_a:
            temp_name_a = new_temp_file_a.name

            j=jobs(0)
            to_file(stripProxy(j), new_temp_file_a, ignore_subs)
            new_temp_file_a.flush()

        counter = 0
        for sj in j.subjobs:
            XMLFileName = getSJXMLFile(sj)
            assert path.isfile(XMLFileName)

            with open(XMLFileName) as handler:
                tmpobj, errs = from_file(handler)
                assert hasattr(tmpobj, 'id')
                assert tmpobj.id == counter

                with NamedTemporaryFile(delete=False) as new_temp_file:
                    temp_name = new_temp_file.name
                    to_file(stripProxy(sj), new_temp_file, ignore_subs)
                    new_temp_file.flush()

                #import filecmp
                #assert filecmp.cmp(XMLFileName, temp_name)
                assert open(temp_name_a).read() == open(temp_name).read()
                unlink(temp_name)

            counter+=1

        assert counter == len(jobs(0).subjobs)
        unlink(temp_name_a)

    def test_h_testXMLIndex(self):
        # Check index of job
        from Ganga.Core.GangaRepository.PickleStreamer import to_file, from_file

        from Ganga.GPI import jobs

        j = jobs(0)

        assert path.isfile(getIndexFile(j))

        with open(getIndexFile(j)) as handler:
            obj, errs = from_file(handler)

            assert isinstance(obj, tuple)

            from Ganga.GPIDev.Base.Proxy import stripProxy, getName
            raw_j = stripProxy(j)
            index_cache = raw_j._getRegistry().getIndexCache(raw_j)
            assert isinstance(index_cache, dict)

            index_cls = getName(raw_j)
            index_cat = raw_j._category
            this_index_cache = (index_cat, index_cls, index_cache)

            print("just-built index: %s" % str(this_index_cache))
            print("from disk: %s" % str(obj))

            assert this_index_cache == obj

    def test_i_testSJXMLIndex(self):
        # Check index of all sj
        from Ganga.Core.GangaRepository.PickleStreamer import to_file, from_file

        from Ganga.GPI import jobs

        assert len(jobs) == 2

        j=jobs(0)

        with open(getSJXMLIndex(j)) as handler:
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

