"""
TODO:
1. Add test steps for:
    1. udocker
    2. singularity
    3. docker itself (maybe)
"""

import utils
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
        extra_opts = utils.get_options(HOST, PORT)
        self.database_config = getConfig("DatabaseConfigurations")
        self.installations = {
            "docker": checkDocker(),
            "udocker": checkUDocker(),
            "native": checkNative(),
            "singularity": checkSingularity()
        }
        super(TestDatabaseBackends, self).setUp(extra_opts=extra_opts)

    def test_a_singularity_backend_lifetime(self):
        """
        Test the starting and shutdown of udocker container image
        """
        import subprocess
        from GangaCore.Core.GangaRepository.container_controllers import singularity_handler
        if self.installations["singularity"]:
            # start the singularity container
            singularity_handler(
                action="start",
                gangadir=self.gangadir(),
                database_config=self.database_config
            )

            # checking if the container started up
            process = subprocess.Popen(
                "singularity instance list", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                shell=True
            )
            stdout, stderr = process.communicate()
            flag = False
            flag = any([
                self.database_config["containerName"] in line
                for line in stdout.decode().split()
            ])
            assert flag

            # shutting down the container
            singularity_handler(
                action="quit",
                gangadir=self.gangadir(),
                database_config=self.database_config
            )
            process = subprocess.Popen(
                "singularity instance list", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                shell=True
            )
            stdout, stderr = process.communicate()
            flag = True
            flag = any([
                self.database_config["containerName"] in line
                for line in stdout.decode().split()
            ])
            assert not flag

    def test_b_udocker_backend_lifetime(self):
        """
        Test the starting and shutdown of udocker container image
        """
        import subprocess
        from GangaCore.Core.GangaRepository.container_controllers import udocker_handler
        if self.installations["udocker"]:
            # start the singularity container
            udocker_handler(
                action="start",
                gangadir=self.gangadir(),
                database_config=self.database_config
            )

            # checking if the container started up
            process = subprocess.Popen(
                "udocker ps", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                shell=True
            )
            stdout, stderr = process.communicate()
            flag = self.database_config['containerName'] in stdout.decode()
            assert flag

            # shutting down the container
            udocker_handler(
                action="quit",
                gangadir=self.gangadir(),
                database_config=self.database_config
            )

            # checking if the container started up
            process = subprocess.Popen(
                "udocker ps", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                shell=True
            )
            stdout, stderr = process.communicate()
            flag = self.database_config['containerName'] in stdout.decode()
            assert not flag

    def test_c_native_backend_lifetime(self):
        """
        Check if native database is running
        """
        # we cannot control the starting of the database and shutting so skiping
        pass

    def test_d_docker_backend_lifetime(self):
        """
        Check if docker contaienr is installed
        """
        import docker
        from GangaCore.Core.GangaRepository.container_controllers import docker_handler
        if self.installations["docker"]:
            # starting the database container
            docker_handler(
                action="start",
                gangadir=self.gangadir(),
                database_config=self.database_config
            )
            # testing status of the database container
            container_client = docker.from_env()
            flag = any([container.name == self.database_config["containerName"]
                        for container in container_client.containers.list()])
            assert flag

            # shutting the container
            docker_handler(
                action="quit",
                gangadir=self.gangadir(),
                database_config=self.database_config
            )

            flag = any([container.name == self.database_config["containerName"]
                        for container in container_client.containers.list()])
            assert not flag

        else:
            # skip the test if docker is not installed
            print("docker is not installed and thus skipping")
            return
