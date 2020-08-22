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


def getNestedList():
    from GangaCore.GPI import LocalFile, GangaList
    gl = GangaList()
    gl2 = GangaList()
    for i in range(5):
        gl.append(LocalFile())
    for i in range(5):
        gl2.append(gl)
    return gl2


config = [
    ("DatabaseConfigurations", "port", "27017"),
    ("DatabaseConfigurations", "baseImage", "mongo"),
    ("DatabaseConfigurations", "dbname", "testDatabase"),
    ("DatabaseConfigurations", "containerName", "testContainer")
]


class TestGangaDBNested(GangaUnitTest):
    """
    Testing the generation of jobs, saving of jobs and finally loading of jobs
    """

    def setUp(self):
        """
        """
        extra_opts = [
            ('TestingFramework', 'AutoCleanup', 'False'),
            ("DatabaseConfigurations", "controller", "docker")
        ]
        self.connection = get_db_connection()
        super(TestGangaDBNested, self).setUp(
            repositorytype="Database", extra_opts=extra_opts)

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from GangaCore.Utility.Config import getConfig
        # self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from GangaCore.GPI import Job, jobs, ArgSplitter
        j = Job()
        assert len(jobs) == 1

        j.splitter = ArgSplitter()
        j.splitter.args = getNestedList()

        assert j.splitter.args == getNestedList()

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
        from GangaCore.GPI import jobs, ArgSplitter

        assert len(jobs) == 1

        j = jobs(0)

        from GangaCore.GPIDev.Base.Proxy import stripProxy, addProxy
        raw_j = stripProxy(j)

        # ANY COMMAND TO LOAD A JOB CAN BE USED HERE
        raw_j.printSummaryTree()

        has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

        assert has_loaded_job

        assert isinstance(j.splitter, ArgSplitter)

        assert j.splitter.args._impl.to_json() == getNestedList()._impl.to_json()

    def test_d_testXMLContent(self):
        # Check content of XML is as expected
        from GangaCore.GPI import jobs, Job
        from GangaCore.GPIDev.Base.Proxy import stripProxy

        from GangaCore.Core.GangaRepository.DStreamer import (
            JsonLoader, object_from_database
        )

        # job_json = object_from_database(_filter={"name": "modified_name"})
        job_json = self.connection.jobs.find_one(filter={"id": 0})

        loader = JsonLoader()
        tmp_job, error = loader.parse_static(job_json)
        if error:
            raise Exception(error)

        assert tmp_job == jobs(0)._impl

    def test_j_DeleteAll(self):
        db_name = "default"
        _ = pymongo.MongoClient()
        _.drop_database(db_name)
        assert True
