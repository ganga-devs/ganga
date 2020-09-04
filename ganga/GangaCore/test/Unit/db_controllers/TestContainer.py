"""
TODO:
1. Added tests to check fail conditions
"""
import os
import time
from GangaCore.test.GPI.db_testing import utils
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.Virtualization import (
    checkNative, checkDocker,
    checkUDocker, checkSingularity
)
from GangaCore.testlib.GangaUnitTest import GangaUnitTest


HOST, PORT = utils.get_host_port()


class TestDatabaseBackends(GangaUnitTest):
    """
    Init all the database containers and see if they work
    """

    def setUp(self):
        """
        """
        from GangaCore.Core.GangaRepository.container_controllers import generate_database_config
        extra_opts = utils.get_options(HOST, PORT)
        self.database_config = generate_database_config()
        self.installations = {
            "docker": checkDocker(),
            "udocker": checkUDocker(),
            "native": checkNative(),
            "singularity": checkSingularity()
        }
        super(TestDatabaseBackends, self).setUp(extra_opts=extra_opts)

    def test_a1_download_sif_file(self):
        """
        Download the sif file requried
        """
        if "GANGA_GITHUB_HOST" in os.environ.keys():
            import gdown  # download the required sif file
            url = 'https://drive.google.com/uc?id=1Z7k9LoFxGQKMjLzoe1D_jL9m5ReeixXk'
            output = 'mongo.sif'
            gdown.download(url, output, quiet=True)

        import shutil # copy the files from repository to testing directory
        shutil.copy(
            src="mongo.sif",
            dst=self.gangadir()
        )
        assert True

    def test_a2_singularity_backend_lifetime(self):
        """
        Test the starting and shutdown of udocker container image
        """
        import subprocess
        from GangaCore.Core.GangaRepository.container_controllers import singularity_handler, mongod_exists

        if self.installations["singularity"]:
            # start the singularity container
            singularity_handler(
                action="start",
                gangadir=self.gangadir(),
                database_config=self.database_config
            )

            # checking if the container started up
            flag = mongod_exists(
                controller="singularity", cname=os.path.join(self.gangadir(), "mongo.sif")
            )
            assert flag is not None

            # shutting down the container
            singularity_handler(
                action="quit",
                gangadir=self.gangadir(),
                database_config=self.database_config
            )
            flag = mongod_exists(
                controller="singularity", cname=os.path.join(self.gangadir(), "mongo.sif")
            )
            assert flag is None

    def test_b_udocker_backend_lifetime(self):
        """
        Test the starting and shutdown of udocker container image
        """
        import subprocess
        from GangaCore.Core.GangaRepository.container_controllers import udocker_handler, mongod_exists
        if self.installations["udocker"]:
            # start the singularity container
            udocker_handler(
                action="start",
                gangadir=self.gangadir(),
                database_config=self.database_config
            )

            # checking if the container started up
            flag = mongod_exists(
                controller="udocker", cname=self.database_config["containerName"]
            )
            assert flag is not None

            # shutting down the container
            udocker_handler(
                action="quit",
                gangadir=self.gangadir(),
                database_config=self.database_config
            )

            # checking if the container started up
            time.sleep(2)
            flag = mongod_exists(
                controller="udocker", cname=self.database_config["containerName"]
            )
            time.sleep(2)
            assert flag is None

    def test_c_native_backend_lifetime(self):
        """
        Check if native database is running
        """
        # we cannot control the starting of the database and shutting so skiping
        pass

    # def test_d_docker_backend_lifetime(self):
    #     """
    #     Check if docker contaienr is installed
    #     """
    #     import docker
    #     from GangaCore.Core.GangaRepository.container_controllers import docker_handler
    #     if self.installations["docker"]:
    #         # starting the database container
    #         docker_handler(
    #             action="start",
    #             gangadir=self.gangadir(),
    #             database_config=self.database_config
    #         )
    #         # testing status of the database container
    #         container_client = docker.from_env()
    #         flag = any([container.name == self.database_config["containerName"]
    #                     for container in container_client.containers.list()])
    #         assert flag

    #         # shutting the container
    #         docker_handler(
    #             action="quit",
    #             gangadir=self.gangadir(),
    #             database_config=self.database_config
    #         )

    #         flag = any([container.name == self.database_config["containerName"]
    #                     for container in container_client.containers.list()])
    #         assert not flag

    #     else:
    #         # skip the test if docker is not installed
    #         print("docker is not installed and thus skipping")
