from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

from os import path, stat, unlink

import time

from .utilFunctions import getJobsPath, getXMLDir, getXMLFile, getIndexFile

testStr = "testFooString"

class TestXMLGenAndLoad(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        extra_opts = [('Registry', 'AutoFlusherWaitTime', 10)]
        super(TestXMLGenAndLoad, self).setUp(extra_opts=extra_opts)
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from Ganga.Utility.Config import getConfig
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from Ganga.GPI import Job, jobs
        j=Job()
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

        #import filecmp
        #assert not filecmp.cmp(getXMLFile(j), getXMLFile(j)+'~')

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
        # check they get updated elsewhere
        from Ganga.GPI import jobs, disableMonitoring, enableMonitoring

        disableMonitoring()

        j=jobs(0)

        XMLFileName = getXMLFile(j)

        last_update = stat(XMLFileName) 

        j.submit()

        newest_update = stat(XMLFileName)

        from GangaTest.Framework.utils import sleep_until_completed

        enableMonitoring()

        can_assert = False
        if j.status in ['submitted', 'running']:
            can_assert = True
            sleep_until_completed(j, 60)

        final_update = stat(XMLFileName)

        assert newest_update.st_mtime > last_update.st_mtime

        # Apparently this requirement is a bad idea. This isn't implemented in 6.1.17 but should probably be in 6.1.18
        #if can_assert:
        #    assert final_update.st_mtime > newest_update.st_mtime
        #else:
        #    assert final_update.st_mtime == newest_update.st_mtime

    def test_e_testXMLContent(self):
        # Check content of XML is as expected
        from Ganga.Core.GangaRepository.VStreamer import to_file, from_file

        from Ganga.GPI import jobs, Job
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from tempfile import NamedTemporaryFile

        j = jobs(0)
        assert path.isfile(getXMLFile(j))
        with open(getXMLFile(j)) as handler:
            tmpobj, errs = from_file(handler)

            assert hasattr(tmpobj, 'name')

            assert tmpobj.name == testStr

            ignore_subs = ['time', 'subjobs', 'info', 'application', 'backend', 'id']

            with NamedTemporaryFile(delete=False) as new_temp_file:
                temp_name = new_temp_file.name


                to_file(stripProxy(j), new_temp_file, ignore_subs)
                new_temp_file.flush()

            with NamedTemporaryFile(delete=False) as new_temp_file2:
                temp_name2 = new_temp_file2.name

                j2 = Job()
                j2.name = testStr
                j2.submit()
                from GangaTest.Framework.utils import sleep_until_completed
                sleep_until_completed(j2)

                to_file(stripProxy(j2), new_temp_file2, ignore_subs)
                new_temp_file2.flush()

            #import filecmp
            #assert filecmp.cmp(handler.name, new_temp_file.name)
            #assert not filecmp.cmp(new_temp_file.name, new_temp_file2.name)

            #assert open(getXMLFile(j)).read() == open(temp_name).read()
            assert open(temp_name).read() == open(temp_name2).read()

            unlink(temp_name)
            unlink(temp_name2)

    def test_f_testXMLIndex(self):
        # Check XML Index content
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

            assert this_index_cache == obj


