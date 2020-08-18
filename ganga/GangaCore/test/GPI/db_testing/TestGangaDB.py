"""
Testing functions related to ganga db
"""
import pytest
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.Virtualization import checkNative, checkDocker

from GangaCore.testlib.GangaUnitTest import GangaUnitTest


def start_gangadb(database_config, method="native"):
    """
    Ignoring everything just start ganga
    """
    if method == "native":
        return 1
    else:
        # assuming we are using the docker backend
        from GangaCore.Core.GangaRepository.container_controllers import docker_handler
        docker_handler(
            action="start",
            database_config=database_config
        )
        return 1

def clean_database():
    """
    Clean the information from the database
    """
    import pymongo
    db_name = "testDatabase"
    _ = pymongo.MongoClient()
    _.drop_database(db_name)


def get_test_config():
    temp_config = getConfig("DatabaseConfigurations")
    keys_req = ["containerName", "baseImage", "dbname", "port"]
    config = dict([key, temp_config[key]] for key in keys_req)
    config["containerName"] = "testContainer"
    config["dbname"] = "testDatabase"
    return config


class TestGangaDBGenAndLoad(GangaUnitTest):
    """
    Testing the generation of jobs, saving of jobs and finally loading of jobs
    """

    def setUp(self):
        """
        """
        extra_opts = [
            ("TestingFramework", "AutoCleanup", "False"),
            ("Configuration", "repositorytype", "Database")
        ]
        self.database_config = get_test_config
        super(TestGangaDBGenAndLoad, self).setUp(extra_opts=extra_opts)

    def test_a_JobConstruction(self):
        """
        Constructing the first job
        """
        # self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])
        assert "False" == (getConfig('TestingFramework')['AutoCleanup'])

        from GangaCore.GPI import Job, jobs
        j = Job()
        assert len(jobs) == 1
        j.name = 'modified_name'

    def test_b_JobJsonExists(self):
        """
        Check whether the job json exists in the table
        """
        import pymongo
        db_name = "testDatabase"
        _ = pymongo.MongoClient()

        connection = _[db_name]

    def test_c_RemoveJobs(self):
        """
        Remove all the jobs from the registry
        """
        from GangaCore.GPI import Job, jobs

        jobs.remove()

        assert len(jobs) == 0