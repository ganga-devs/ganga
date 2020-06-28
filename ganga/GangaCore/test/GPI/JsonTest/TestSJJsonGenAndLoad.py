"""The dreaded test that does not pass :(
"""

from GangaCore.testlib.GangaUnitTest import GangaUnitTest

from os import path, stat, unlink

import time

from .utilFunctions import getJobsPath, getJSONDir, getJSONFile, getSJJSONFile, getSJJSONIndex, getIndexFile

testStr = "testFooString"
testArgs = [[1],[2],[3],[4],[5]]

class TestSJJsonGenAndLoad(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        extra_opts = [('Registry', 'AutoFlusherWaitTime', 5), ('TestingFramework', 'AutoCleanup', 'False'), ('Configuration', 'repositorytype', 'LocalJson')]
        super(TestSJJsonGenAndLoad, self).setUp(extra_opts=extra_opts)

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from GangaCore.Utility.Config import getConfig
        # self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from GangaCore.GPI import Job, jobs, ArgSplitter
        j=Job(splitter=ArgSplitter(args=testArgs))
        assert len(jobs) == 1
        from GangaCore.GPIDev.Base.Proxy import stripProxy
        stripProxy(j)._getRegistry().flush_all()
        stripProxy(j)._setDirty()

    def test_b_JobJsonExists(self):
        # Check things exist
        from GangaCore.GPI import jobs

        assert len(jobs) == 1

        print(("len: %s" % len(jobs)))

        j=jobs(0)

        assert path.isdir(getJobsPath())

        assert path.isfile(path.join(getJobsPath(), 'cnt'))

        assert path.isdir(getJSONDir(j))

        assert path.isfile(getJSONFile(j))

        assert path.isfile(getJSONFile(j) + '~')

        assert path.isfile(getIndexFile(j))

    def test_c_JsonAutoUpdated(self):
        # Check they get updated
        from GangaCore.GPI import jobs

        j=jobs(0)

        JsonFileName = getJSONFile(j)

        last_update = stat(JsonFileName)

        j.name = testStr

        from GangaCore.Utility.Config import getConfig
        flush_timeout = getConfig('Registry')['AutoFlusherWaitTime']

        total_time=0.
        new_update = 0
        lst_update = last_update.st_mtime
        while total_time < 2.*flush_timeout and new_update <= lst_update:
            total_time+=1.
            time.sleep(1.)
            try:
                new_update = stat(JsonFileName).st_mtime
            except:
                new_update = 0.

        newest_update = stat(JsonFileName)

        assert newest_update.st_mtime > last_update.st_mtime


    def test_d_JsonUpdated(self):
        # Check they get updated elsewhere
        from GangaCore.GPI import jobs
        # , disableMonitoring, enableMonitoring

        # disableMonitoring()

        j=jobs(0)

        JsonFileName = getJSONFile(j)

        last_update = stat(JsonFileName)

        j.submit()

        newest_update = stat(JsonFileName)

        assert len(j.subjobs) == len(testArgs)

        assert newest_update.st_mtime > last_update.st_mtime

        # enableMonitoring()
        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(j)

    # def test_e_SubJobJsonExists(self):
    #     # Check other Json exit
    #     from GangaCore.GPI import jobs
    #     from GangaCore.GPIDev.Base.Proxy import stripProxy

    #     assert len(jobs) == 1

    #     # j=jobs(0)
    #     j=jobs[-1]

    #     print("============", j.status)
    #     print("============", j.subjobs)
        # print("============\n", j, "\n============")

        # for sj in j.subjobs:
        #     this_bak = sj.backend
        #     stripProxy(sj)._setDirty()
        
        # # stripProxy(stripProxy(j).subjobs).flush()

        # assert path.isdir(getJSONDir(j))

        # # FIXME: This should only be checked after j.submit() as subjob.idx is only gen when subjobs are gen
        # # assert path.isfile(getSJJSONIndex(j))

        # for sj in j.subjobs:
        #     JsonFileName = getSJJSONFile(sj)
        #     assert path.isfile(JsonFileName)
        #     assert path.isfile(JsonFileName+'~')

    def test_f_testJsonContent(self):
        # Check their content
        from GangaCore.Core.GangaRepository.JStreamer import to_file, from_file

        from GangaCore.GPI import jobs, Job
        from GangaCore.GPIDev.Base.Proxy import stripProxy

        from tempfile import NamedTemporaryFile

        j=jobs(0)
        JsonFileName = getJSONFile(j)
        assert path.isfile(JsonFileName)
        with open(JsonFileName, 'r') as handler:
            tmpobj, errs = from_file(handler)

            assert hasattr(tmpobj, 'name')

            assert tmpobj.name == testStr

            ignore_subs = ['status', 'subjobs', 'time', 'backend', 'id', 'splitter', 'info', 'application']

            with NamedTemporaryFile(mode= 'w', delete=False) as new_temp_file:
                temp_name = new_temp_file.name

                to_file(stripProxy(j), new_temp_file, ignore_subs)
                new_temp_file.flush()

            with NamedTemporaryFile(mode = 'w', delete=False) as new_temp_file2:
                temp_name2 = new_temp_file2.name

                j2=Job()
                j2.name=testStr

                to_file(stripProxy(j2), new_temp_file2, ignore_subs)
                new_temp_file2.flush()

            #assert open(JsonFileName).read() == open(temp_name).read()
            assert open(temp_name).read() == open(temp_name2).read()

            unlink(temp_name)
            unlink(temp_name2)

    # def test_g_testSJJsonContent(self):
    #     # Check SJ content
    #     from GangaCore.Core.GangaRepository.JStreamer import to_file, from_file

    #     from GangaCore.GPI import jobs
    #     from tempfile import NamedTemporaryFile
    #     from GangaCore.GPIDev.Base.Proxy import stripProxy

    #     ignore_subs = ['subjobs', 'time', 'backend', 'id', 'splitter', 'info', 'application', 'inputdata']

    #     with NamedTemporaryFile(mode = 'w', delete=False) as new_temp_file_a:
    #         temp_name_a = new_temp_file_a.name

    #         j=jobs(0)
    #         to_file(stripProxy(j), new_temp_file_a, ignore_subs)
    #         new_temp_file_a.flush()

    #     counter = 0
    #     for sj in j.subjobs:
    #         JsonFileName = getSJJSONFile(sj)
    #         assert path.isfile(JsonFileName)

    #         with open(JsonFileName, 'r') as handler:
    #             tmpobj, errs = from_file(handler)
    #             assert hasattr(tmpobj, 'id')
    #             assert tmpobj.id == counter

    #             with NamedTemporaryFile(mode = 'w', delete=False) as new_temp_file:
    #                 temp_name = new_temp_file.name
    #                 to_file(stripProxy(sj), new_temp_file, ignore_subs)
    #                 new_temp_file.flush()

    #             #import filecmp
    #             #assert filecmp.cmp(JsonFileName, temp_name)
    #             assert open(temp_name_a).read() == open(temp_name).read()
    #             unlink(temp_name)

    #         counter+=1

    #     assert counter == len(jobs(0).subjobs)
    #     unlink(temp_name_a)

    # def test_h_testJsonIndex(self):
    #     # Check index of job
    #     from GangaCore.Core.GangaRepository.PickleStreamer import json_pickle_to_file, json_pickle_from_file

    #     from GangaCore.GPI import jobs

    #     j = jobs(0)

    #     assert path.isfile(getIndexFile(j))

    #     with open(getIndexFile(j), 'r') as handler:
    #         obj, errs = json_pickle_from_file(handler)

    #         assert isinstance(obj, list)

    #         from GangaCore.GPIDev.Base.Proxy import stripProxy, getName
    #         raw_j = stripProxy(j)
    #         index_cache = raw_j._getRegistry().getIndexCache(raw_j)
    #         assert isinstance(index_cache, dict)

    #         index_cls = getName(raw_j)
    #         index_cat = raw_j._category
    #         this_index_cache = (index_cat, index_cls, index_cache)

    #         print(("just-built index: %s" % str(this_index_cache)))
    #         print(("from disk: %s" % str(obj)))

    #         assert this_index_cache == obj

    # def test_i_testSJJsonIndex(self):
    #     # Check index of all sj
    #     from GangaCore.Core.GangaRepository.PickleStreamer import json_pickle_to_file, json_pickle_from_file

    #     from GangaCore.GPI import jobs

    #     assert len(jobs) == 2

    #     j=jobs(0)

    #     with open(getSJJsonIndex(j), 'r') as handler:
    #         obj, errs = json_pickle_from_file(handler)

    #         assert isinstance(obj, dict)

    #         from GangaCore.GPIDev.Base.Proxy import stripProxy, getName
    #         raw_j = stripProxy(j)

    #         new_dict = {}
    #         for sj in j.subjobs:
    #             raw_sj = stripProxy(sj)
    #             temp_index = raw_sj._getRegistry().getIndexCache(raw_sj)

    #             new_dict[sj.id] = temp_index
    #             assert raw_sj._category == raw_j._category

    #         for k, v in new_dict.items():
    #             for k1, v1 in v.items():
    #                 if k1 != 'modified':
    #                     assert obj[k][k1] == new_dict[k][k1]

