import os

import pymongo
import utils
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.Virtualization import checkDocker

HOST, PORT = utils.get_host_port()


class TestGangaDBNested(GangaUnitTest):
    """
    Testing the generation of jobs, saving of jobs and finally loading of jobs
    """

    def setUp(self):
        """
        Setup the environment for the testing
        """
        extra_opts = utils.get_options(HOST, PORT)
        self.connection = utils.get_db_connection(HOST, PORT)
        super(TestGangaDBNested, self).setUp(
            repositorytype="Database", extra_opts=extra_opts)

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        # self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])
        from GangaCore.GPI import ArgSplitter, Job, jobs
        from GangaCore.Utility.Config import getConfig
        j = Job()
        assert len(jobs) == 1

        j.splitter = ArgSplitter()
        j.splitter.args = utils.getNestedList()

        assert j.splitter.args == utils.getNestedList()

    def test_b_JobNotLoaded(self):
        """ Second get the job and check that getting it via jobs doesn't cause it to be loaded"""
        from GangaCore.GPI import jobs

        assert len(jobs) == 1

        print(("len: %s" % len(jobs)))

        j = jobs(0)

        from GangaCore.GPIDev.Base.Proxy import stripProxy
        raw_j = stripProxy(j)

        has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

        assert not has_loaded_job

    def test_c_JobLoaded(self):
        """ Third do something to trigger a loading of a Job and then test if it's loaded"""
        from GangaCore.GPI import ArgSplitter, jobs

        assert len(jobs) == 1

        j = jobs(0)

        from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy
        raw_j = stripProxy(j)

        # ANY COMMAND TO LOAD A JOB CAN BE USED HERE
        raw_j.printSummaryTree()

        has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

        assert has_loaded_job

        assert isinstance(j.splitter, ArgSplitter)

        assert j.splitter.args._impl.to_json() == utils.getNestedList()._impl.to_json()

    def test_d_testJSONContent(self):
        # Check content of JSON is as expected
        from GangaCore.Core.GangaRepository.DStreamer import (
            JsonRepresentation, object_from_database)
        from GangaCore.GPI import Job, jobs
        from GangaCore.GPIDev.Base.Proxy import stripProxy

        # job_json = object_from_database(_filter={"name": "modified_name"})
        job_json = self.connection.jobs.find_one(filter={"id": 0})

        loader = JsonRepresentation()
        tmp_job, error = loader.parse_static(job_json)
        if error:
            raise Exception(error)

        assert tmp_job == jobs(0)._impl

    def test_e_DeleteAll(self):
        """
        This is usefull when testing locally.
        """
        utils.clean_database(HOST, PORT)
