# commented out due the the error mentioned in the class doc
# import os

import time

import docker
import utils
from GangaCore.Core.GangaRepository.container_controllers import (
    checkNative, docker_handler, get_database_config, mongod_exists,
    udocker_handler)
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaCore.Utility.Virtualization import (checkDocker, checkSingularity,
                                              checkApptainer, checkUDocker)

HOST, PORT = utils.get_host_port()


class TestContainerHandler(GangaUnitTest):
    """
    Init all the database containers and see if they work

    FIXME: The error when creating a singularity container in github actions
    errs: FATAL:
    container creation failed: mount /proc/self/fd/5->/usr/local/var/singularity/mnt/session/rootfs error:
    can't mount image /proc/self/fd/5: failed to mount squashfs filesystem: invalid argument
    https://github.com/ganga-devs/ganga/runs/1084178708?check_suite_focus=true

    Check if similar error occurs with apptainer.
    """

    def setUp(self):
        """
        """
        extra_opts = utils.get_options(HOST, PORT)
        self.installations = {
            "docker": checkDocker(),
            "udocker": checkUDocker(),
            "native": checkNative(),
            "singularity": checkSingularity(),
            "apptainer": checkApptainer(),
        }
        super(TestContainerHandler, self).setUp(extra_opts=extra_opts)

    # Commented due the the error mentioned in the class doc
    # def test_a1_download_sif_file(self):
    #     """
    #     Download the sif file requried
    #     """
    #     if not os.path.exists("mongo.sif"):
    #         import gdown  # download the required sif file
    #         url = 'https://drive.google.com/uc?id=1Z7k9LoFxGQKMjLzoe1D_jL9m5ReeixXk'
    #         output = 'mongo.sif'
    #         gdown.download(url, output, quiet=True)

    #     import shutil  # copy the files from repository to testing directory
    #     shutil.copy(
    #         src="mongo.sif",
    #         dst=self.gangadir()
    #     )
    #     assert True

    # def test_a2_singularity_backend_lifetime(self):
    #     """
    #     Test the starting and shutdown of udocker container image
    #     """
    #     from GangaCore.Core.GangaRepository.container_controllers import singularity_handler, mongod_exists

    #     database_config = get_database_config(self.gangadir())

    #     if self.installations["singularity"]:
    #         # start the singularity container
    #         singularity_handler(
    #             action="start",
    #             gangadir=self.gangadir(),
    #             database_config=database_config
    #         )

    #         # checking if the container started up
    #         flag = mongod_exists(
    #             controller="singularity", cname=os.path.join(self.gangadir(), "mongo.sif")
    #         )
    #         assert flag is not None

    #         # shutting down the container
    #         singularity_handler(
    #             action="quit",
    #             gangadir=self.gangadir(),
    #             database_config=database_config
    #         )
    #         flag = mongod_exists(
    #             controller="singularity", cname=os.path.join(self.gangadir(), "mongo.sif")
    #         )
    #         assert flag is None

    # def test_a2_apptainer_backend_lifetime(self):
    #     """
    #     Test the starting and shutdown of udocker container image
    #     """
    #     from GangaCore.Core.GangaRepository.container_controllers import apptainer_handler, mongod_exists

    #     database_config = get_database_config(self.gangadir())

    #     if self.installations["apptainer"]:
    #         # start the apptainer container
    #         apptainer_handler(
    #             action="start",
    #             gangadir=self.gangadir(),
    #             database_config=database_config
    #         )

    #         # checking if the container started up
    #         flag = mongod_exists(
    #             controller="apptainer", cname=os.path.join(self.gangadir(), "mongo.sif")
    #         )
    #         assert flag is not None

    #         # shutting down the container
    #         apptainer_handler(
    #             action="quit",
    #             gangadir=self.gangadir(),
    #             database_config=database_config
    #         )
    #         flag = mongod_exists(
    #             controller="apptainer", cname=os.path.join(self.gangadir(), "mongo.sif")
    #         )
    #         assert flag is None

    def test_b_udocker_backend_lifetime(self):
        """
        Test the starting and shutdown of udocker container image
        """
        database_config = get_database_config(self.gangadir())

        if self.installations["udocker"]:
            # start the singularity/apptainer container
            udocker_handler(
                action="start",
                gangadir=self.gangadir(),
                database_config=database_config
            )

            # checking if the container started up
            flag = mongod_exists(
                controller="udocker", cname=database_config["containerName"]
            )
            assert flag is not None

            # shutting down the container
            udocker_handler(
                action="quit",
                gangadir=self.gangadir(),
                database_config=database_config
            )

            # checking if the container started up
            time.sleep(2)
            flag = mongod_exists(
                controller="udocker", cname=database_config["containerName"]
            )
            assert flag is None

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
        database_config = get_database_config(self.gangadir())

        if self.installations["docker"]:
            # starting the database container
            docker_handler(
                action="start",
                gangadir=self.gangadir(),
                database_config=database_config
            )
            # testing status of the database container
            container_client = docker.from_env()
            flag = any([container.name == database_config["containerName"]
                        for container in container_client.containers.list()])
            assert flag

            # shutting the container
            docker_handler(
                action="quit",
                gangadir=self.gangadir(),
                database_config=database_config
            )

            flag = any([container.name == database_config["containerName"]
                        for container in container_client.containers.list()])
            assert not flag

        else:
            # skip the test if docker is not installed
            print("docker is not installed and thus skipping")
