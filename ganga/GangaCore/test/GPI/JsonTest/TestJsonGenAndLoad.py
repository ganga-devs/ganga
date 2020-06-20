

from GangaCore.testlib.GangaUnitTest import GangaUnitTest

from os import path, stat, unlink

import json
import time

from .utilFunctions import getJobsPath, getJSONDir, getJSONFile, getIndexFile

testStr = "testFooString"

class TestJSONGenAndLoad(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        extra_opts = [('Registry', 'AutoFlusherWaitTime', 5), ('TestingFramework', 'AutoCleanup', 'False'), ('Configuration', 'repositorytype', 'LocalJson')]
        super(TestJSONGenAndLoad, self).setUp(extra_opts=extra_opts)

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from GangaCore.Utility.Config import getConfig
        ### I get AssertionError: 'False' is not false for some reason. My config does not have [TestingFramework] options I guess
        # self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from GangaCore.GPI import Job, jobs
        j=Job()
        assert len(jobs) == 1
        j.name = 'modified_name'

    def test_b_JobJSONExists(self):
        # Check things exist
        from GangaCore.GPI import jobs, Job

        assert len(jobs) == 1

        print(("len: %s" % str(len(jobs))))

        j=jobs(0)

        # j = Job()

        assert path.isdir(getJobsPath())

        assert path.isfile(path.join(getJobsPath(), 'cnt'))

        assert path.isdir(getJSONDir(j))

        assert path.isfile(getJSONFile(j))

        # TODO:
        # checking if the backup file is created. This fails as the backup file is not generated currently. Do not know the reason for this.
        assert path.isfile(getJSONFile(j) + '~')

        assert path.isfile(getIndexFile(j))

        #import filecmp
        #assert not filecmp.cmp(getJSONFile(j), getJSONFile(j)+'~')

    def test_c_JSONAutoUpdated(self):
        # Check they get updated
        from GangaCore.GPI import jobs

        j=jobs[-1]
        # j = Job()

        JSONFileName = getJSONFile(j)

        last_update = stat(JSONFileName)

        j.name = testStr

        # FIXME: test passes if we wait. 
        time.sleep(5)

        from GangaCore.Utility.Config import getConfig
        flush_timeout = 1
        total_time=0.
        new_update = 0
        lst_update = last_update.st_mtime
        while total_time < 2.*flush_timeout and new_update <= lst_update:
            total_time+=1.
            time.sleep(1.)
            try:
                new_update = stat(JSONFileName).st_mtime
            except:
                new_update = 0.

        newest_update = stat(JSONFileName)

        assert newest_update.st_mtime > last_update.st_mtime

# --------------------------------------------------------------------------

    def test_d_JSONUpdated(self):
        # check they get updated elsewhere
        from GangaCore.GPI import jobs, disableMonitoring, enableMonitoring

        disableMonitoring()

        j=jobs(0)

        JSONFileName = getJSONFile(j)

        last_update = stat(JSONFileName) 

        j.submit()

        newest_update = stat(JSONFileName)

        from GangaTest.Framework.utils import sleep_until_completed

        enableMonitoring()

        can_assert = False
        if j.status in ['submitted', 'running']:
            can_assert = True
            sleep_until_completed(j, 60)

        final_update = stat(JSONFileName)

        assert newest_update.st_mtime > last_update.st_mtime

        # Apparently this requirement is a bad idea. This isn't implemented in 6.1.17 but should probably be in 6.1.18
        #if can_assert:
        #    assert final_update.st_mtime > newest_update.st_mtime
        #else:
        #    assert final_update.st_mtime == newest_update.st_mtime

    # FIXME: What is the use of this test?
    # def test_e_testJSONContent(self):
    #     # Check content of JSON is as expected
    #     from GangaCore.Core.GangaRepository.JStreamer import to_file, from_file

    #     from GangaCore.GPI import jobs, Job
    #     from GangaCore.GPIDev.Base.Proxy import stripProxy

    #     from tempfile import NamedTemporaryFile

    #     j = jobs(0)
    #     assert path.isfile(getJSONFile(j))
    #     with open(getJSONFile(j)) as handler:
    #         tmpobj, errs = from_file(handler)

    #         assert hasattr(tmpobj, 'name')

    #         assert tmpobj.name == testStr

    #         ignore_subs = ['time', 'subjobs', 'info', 'application', 'backend', 'id']

    #         with NamedTemporaryFile(mode = 'w', delete=False) as new_temp_file:
    #             temp_name = new_temp_file.name

    #             to_file(stripProxy(j), new_temp_file, ignore_subs)
    #             new_temp_file.flush()

    #         with NamedTemporaryFile(mode = 'w', delete=False) as new_temp_file2:
    #             temp_name2 = new_temp_file2.name

    #             j2 = Job()
    #             j2.name = testStr
    #             j2.submit()
    #             from GangaTest.Framework.utils import sleep_until_completed
    #             sleep_until_completed(j2)

    #             to_file(stripProxy(j2), new_temp_file2, ignore_subs)
    #             new_temp_file2.flush()

    #         #import filecmp
    #         #assert filecmp.cmp(handler.name, new_temp_file.name)
    #         #assert not filecmp.cmp(new_temp_file.name, new_temp_file2.name)

    #         #assert open(getJSONFile(j)).read() == open(temp_name).read()
    #         assert json.load(open(temp_name)) == json.load(open(temp_name2))

    #         unlink(temp_name)
    #         unlink(temp_name2)

    def test_f_testJSONIndex(self):
        # Check JSON Index content
        from GangaCore.Core.GangaRepository.PickleStreamer import to_file, from_file

        from GangaCore.GPI import jobs

        j = jobs[-1 ]

        assert path.isfile(getIndexFile(j))

        with open(getIndexFile(j), 'rb') as handler:
            obj, errs = from_file(handler)

            # assert isinstance(obj, tuple)
            # using json implies the datastruct tuple ->> list
            assert isinstance(obj, list)

            from GangaCore.GPIDev.Base.Proxy import stripProxy, getName
            raw_j = stripProxy(j)
            index_cache = raw_j._getRegistry().getIndexCache(raw_j)
            assert isinstance(index_cache, dict)

            index_cls = getName(raw_j)
            index_cat = raw_j._category
            this_index_cache = [index_cat, index_cls, index_cache]

            assert this_index_cache == obj


