from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

from os import path, stat, unlink

import time

from .utilFunctions import getJobsPath, getXMLDir, getXMLFile, getIndexFile

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
        time.sleep(2.*flush_timeout)

        newest_update = stat(XMLFileName)

        assert newest_update.st_mtime > last_update.st_mtime


    def test_d_XMLUpdated(self):
        # check they get updated elsewhere
        from Ganga.GPI import jobs, disableMonitoring

        disableMonitoring()

        j=jobs(0)

        XMLFileName = getXMLFile(j)

        last_update = stat(XMLFileName) 

        j.submit()

        newest_update = stat(XMLFileName)

        from GangaTest.Framework.utils import sleep_until_completed

        can_assert = False
        if j.status not in ['completed', 'failed']:
            can_assert = True
            sleep_until_completed(j)

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
        handler = open(getXMLFile(j))
        tmpobj, errs = from_file(handler)

        assert hasattr(tmpobj, 'name')

        assert tmpobj.name == testStr

        new_temp_file = NamedTemporaryFile(delete=False)
        temp_name = new_temp_file.name

        ignore_subs = ''

        to_file(stripProxy(j), new_temp_file, ignore_subs)
        new_temp_file.flush()

        new_temp_file2 = NamedTemporaryFile()

        j2 = Job()
        j2.name = testStr
        j2.submit()
        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(j2)

        to_file(stripProxy(j2), new_temp_file2, ignore_subs)
        new_temp_file2.flush()

        import filecmp

        assert filecmp.cmp(handler.name, new_temp_file.name)
        assert not filecmp.cmp(new_temp_file.name, new_temp_file2.name)
        handler.close()
        unlink(temp_name)

    def test_f_testXMLIndex(self):
        # Check XML Index content
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

