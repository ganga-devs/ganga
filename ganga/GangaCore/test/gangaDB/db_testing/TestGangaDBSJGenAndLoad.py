import os
import utils
import pymongo

from GangaCore.Utility.Config import getConfig
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaCore.Utility.Virtualization import checkNative, checkDocker


testStr = "testFooString"
testArgs = [[1], [2], [3], [4], [5]]
HOST, PORT = utils.get_host_port()


class TestGangaDBGenAndLoad(GangaUnitTest):

    def setUp(self):
        """
        Setup the environment for the testing
        """
        extra_opts = utils.get_options(HOST, PORT)
        self.connection = utils.get_db_connection(HOST, PORT)
        super(TestGangaDBGenAndLoad, self).setUp(
            repositorytype="Database", extra_opts=extra_opts)

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from GangaCore.Utility.Config import getConfig
        # self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from GangaCore.GPI import Job, jobs, ArgSplitter
        j = Job(splitter=ArgSplitter(args=testArgs))

        assert len(jobs) == 1

        from GangaCore.GPIDev.Base.Proxy import stripProxy
        stripProxy(j)._getRegistry().flush_all()
        stripProxy(j)._setDirty()

    def test_b_JobJsonExists(self):
        """Check if the Job exists in the repository
        """
        # Check things exist
        from GangaCore.GPI import jobs

        assert len(jobs) == 1

        print(("len: %s" % len(jobs)))

        j = jobs(0)

        assert len([*self.connection.jobs.find()]) == 1

    def test_c_JsonUpdated(self):
        """
        Submit the job and check if the `status` has been updated
        """
        from GangaCore.GPI import jobs

        j = jobs(0)

        job_json = self.connection.jobs.find_one(
            filter={"id": 0})
        if job_json is None:
            assert False

        last_update = job_json["modified_time"]

        j.name = testStr

        job_json = self.connection.jobs.find_one(
            filter={"id": 0})
        if job_json is None:
            assert False

        newest_update = job_json["modified_time"]

        assert newest_update > last_update

    def test_c_JsonUpdated(self):
        """
        Submit the job and check if the `status` has been updated
        """
        from GangaCore.GPI import jobs, disableMonitoring, enableMonitoring

        disableMonitoring()

        j = jobs(0)

        job_json = self.connection.jobs.find_one(
            filter={"id": 0})
        if job_json is None:
            assert False

        last_update = job_json["modified_time"]

        j.submit()

        job_json = self.connection.jobs.find_one(
            filter={"id": 0})
        if job_json is None:
            assert False

        newest_update = job_json["modified_time"]

        from GangaTest.Framework.utils import sleep_until_completed

        enableMonitoring()

        if j.status in ['submitted', 'running']:
            sleep_until_completed(j, 60)

        job_json = self.connection.jobs.find_one(
            filter={"id": 0})
        if job_json is None:
            assert False

        final_update = job_json["modified_time"]

        assert newest_update > last_update

    def test_d_SubJobJsonExists(self):
        # Check other Json exit
        from GangaCore.GPI import jobs
        from GangaCore.GPIDev.Base.Proxy import stripProxy

        assert len(jobs) == 1

        j = jobs(0)

        for sj in j.subjobs:
            this_bak = sj.backend
            stripProxy(sj)._setDirty()

        stripProxy(stripProxy(j).subjobs).flush()

        assert self.connection.jobs.find_one(filter={"id": 0})
        assert len([*self.connection.jobs.find(filter={"master": 0})])
        # assert path.isfile(getSJJsonIndex(j))

        for sj in j.subjobs:
            assert self.connection.jobs.find_one(
                filter={"id": sj.id, "master": 0}
            )

    def test_f_testJsonContent(self):
        # Check their content
        from GangaCore.GPI import jobs, Job
        from GangaCore.GPIDev.Base.Proxy import stripProxy

        from GangaCore.Core.GangaRepository.DStreamer import (
            JsonRepresentation, object_from_database
        )

        job_json = self.connection.jobs.find_one(
            filter={"id": 0, "master": -1})
        loader = JsonRepresentation()
        tmp_job, error = loader.parse_static(job_json)
        if error:
            raise Exception(error)

        ignore_subs = ['time', 'subjobs', 'info',
                       'application', 'backend', 'id']

        assert tmp_job == jobs(0)._impl

# FIXME: I do not undertand this test
    # def test_g_testSJJsonContent(self):
    #     # Check SJ content
    #     from GangaCore.Core.GangaRepository.VStreamer import to_file, from_file

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
    #         assert path.isfile(JSONFileName)

    #         with open(JSONFileName, 'rb') as handler:
    #             tmpobj, errs = from_file(handler)
    #             assert hasattr(tmpobj, 'id')
    #             assert tmpobj.id == counter

    #             with NamedTemporaryFile(mode = 'w', delete=False) as new_temp_file:
    #                 temp_name = new_temp_file.name
    #                 to_file(stripProxy(sj), new_temp_file, ignore_subs)
    #                 new_temp_file.flush()

    #             #import filecmp
    #             #assert filecmp.cmp(JSONFileName, temp_name)
    #             assert open(temp_name_a).read() == open(temp_name).read()
    #             unlink(temp_name)

    #         counter+=1

    #     assert counter == len(jobs(0).subjobs)
    #     unlink(temp_name_a)

    def test_h_testJSONIndex(self):
        # Check index of job
        from GangaCore.GPI import jobs, Job
        from GangaCore.GPIDev.Base.Proxy import stripProxy, getName
        from GangaCore.Core.GangaRepository.DStreamer import (
            JsonRepresentation, object_from_database
        )

        from GangaCore.GPI import jobs

        j = jobs(0)

        index = self.connection.index.find_one(filter={"id": j.id})
        assert isinstance(index, dict)

        raw_j = stripProxy(j)
        index_cache = raw_j._getRegistry().getIndexCache(raw_j)
        assert isinstance(index_cache, dict)

        index_cls = getName(raw_j)
        index_cat = raw_j._category
        index_cache.update({"classname": getName(raw_j)})
        index_cache.update({"category": raw_j._category})

        # assert that the database index has all the values requried
        for key, val in index_cache.items():
            assert index[key] == val

    def test_i_testSJJsonIndex(self):
        # Check index of all sj
        # from GangaCore.Core.GangaRepository.PickleStreamer import to_file, from_file

        # from GangaCore.GPI import jobs

        # assert len(jobs) == 2

        from GangaCore.GPI import jobs, Job
        from GangaCore.GPIDev.Base.Proxy import stripProxy, getName
        from GangaCore.Core.GangaRepository.DStreamer import (
            JsonRepresentation, object_from_database
        )

        from GangaCore.GPI import jobs

        j = jobs(0)

        for sj in j.subjobs:
            raw_sj = stripProxy(sj)
            index = self.connection.index.find_one(filter={
                "id": raw_sj.id, "master": raw_sj.master.id
            })
            assert isinstance(index, dict)

            index_cache = raw_sj._getRegistry().getIndexCache(raw_sj)
            assert isinstance(index_cache, dict)

            index_cache.update({"classname": getName(raw_sj)})
            index_cache.update({"category": raw_sj._category})

            # assert that the database index has all the values requried
            for key, val in index_cache.items():
                assert index[key] == val

    def test_j_DeleteAll(self):
        """
        This is usefull when testing locally.
        """
        utils.clean_database(HOST, PORT)
