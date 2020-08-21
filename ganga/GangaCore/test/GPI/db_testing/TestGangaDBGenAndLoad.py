"""
Testing functions related to ganga db
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
client = MongoClient(serverSelectionTimeoutMS=10, connectTimeoutMS=20000)
try:
    info = client.server_info() # Forces a call.
except ServerSelectionTimeoutError:
    print("server is down.")

"""
import os
import pymongo
from GangaCore.Utility.Config import getConfig
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaCore.Utility.Virtualization import checkNative, checkDocker

def clean_database():
    """
    Clean the information from the database
    """
    import pymongo
    db_name = "default"
    _ = pymongo.MongoClient()
    _.drop_database(db_name)


def get_db_connection():
    """
    Connection to the testing mongo database
    """

    # FIXME: Can't seem to get `ganga` to read the modified config changes
    # patching the effect by using custom config
    db_name = "default"
    _ = pymongo.MongoClient()
    connection = _[db_name]

    return connection

config = [
    ("DatabaseConfigurations", "port", "27017"),
    ("DatabaseConfigurations", "baseImage", "mongo"),
    ("DatabaseConfigurations", "dbname", "testDatabase"),
    ("DatabaseConfigurations", "containerName", "testContainer")
]


class TestGangaDBGenAndLoad(GangaUnitTest):
    """
    Testing the generation of jobs, saving of jobs and finally loading of jobs
    """

    def setUp(self):
        """
        """
        extra_opts = [
            ('TestingFramework', 'AutoCleanup', 'False'),
            ("DatabaseConfigurations", "controller", "native")
        ]
        self.connection = get_db_connection()
        super(TestGangaDBGenAndLoad, self).setUp(repositorytype="Database", extra_opts=extra_opts)

    def test_a_JobConstruction(self):
        """
        Constructing the first job
        """
        assert 'False' == (getConfig('TestingFramework')['AutoCleanup'])

        from GangaCore.GPI import Job, jobs
        j = Job()
        assert len(jobs) == 1
        j.name = 'modified_name'

    def test_b_JobJsonExists(self):
        """
        Check whether the job json exists in the table
        """
        # assert len(
        #     [*connection.jobs.find_one(filter={"name": "modified_name"})]) == 1
        assert len([*self.connection.jobs.find()]) == 1

    def test_c_JsonUpdated(self):
        """
        Submit the job and check if the `status` has been updated
        """
        from GangaCore.GPI import jobs, disableMonitoring, enableMonitoring

        disableMonitoring()

        j = jobs(0)

        job_json = self.connection.jobs.find_one(
            filter={"name": "modified_name"})
        if job_json is None:
            assert False

        last_update = job_json["modified_time"]

        j.submit()

        job_json = self.connection.jobs.find_one(
            filter={"name": "modified_name"})
        if job_json is None:
            assert False

        newest_update = job_json["modified_time"]

        from GangaTest.Framework.utils import sleep_until_completed

        enableMonitoring()

        if j.status in ['submitted', 'running']:
            sleep_until_completed(j, 60)

        job_json = self.connection.jobs.find_one(
            filter={"name": "modified_name"})
        if job_json is None:
            assert False

        final_update = job_json["modified_time"]

        assert newest_update > last_update

    def test_d_JsonContent(self):
        """
        Check if the json content in the database is correct and as expected

        Assert that the json saved in the database is valid enough to regenerate that job
        """
        from GangaCore.GPI import jobs, Job
        from GangaCore.GPIDev.Base.Proxy import stripProxy

        from GangaCore.Core.GangaRepository.DStreamer import (
            JsonLoader, object_from_database
        )

        # job_json = object_from_database(_filter={"name": "modified_name"})
        job_json = self.connection.jobs.find_one(
            filter={"name": "modified_name"})
        loader = JsonLoader()
        tmp_job, error = loader.parse_static(job_json)
        if error:
            raise Exception(error)

        ignore_subs = ['time', 'subjobs', 'info',
                       'application', 'backend', 'id']

        assert tmp_job == jobs(0)._impl

    def test_e_JsonContent(self):
        """
        Check if the json content in the database is correct and as expected

        Assert that the json saved in the database is valid enough to regenerate that job
        """
        from GangaCore.GPI import jobs, Job
        from GangaCore.GPIDev.Base.Proxy import stripProxy, getName
        from GangaCore.Core.GangaRepository.DStreamer import (
            JsonLoader, object_from_database
        )

        from GangaCore.GPI import jobs

        j = jobs(0)

        index = self.connection.index.find_one(filter={
            "name": "modified_name"
        })
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

    def test_f_RemoveJobs(self):
        """
        Remove all the jobs from the registry
        """
        from GangaCore.GPI import jobs

        jobs.remove()

        assert len(jobs) == 0

    def test_g_JobJsonExists(self):
        """
        Check whether the job json exists in the table
        """
        assert len([*self.connection.jobs.find()]) == 0
        clean_database()
