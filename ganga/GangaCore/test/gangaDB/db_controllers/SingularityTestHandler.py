# """Test the singularity container 
# """

# import os
# import time
# import json
# import utils
# import pytest
# from GangaCore.Core.GangaRepository.container_controllers import get_database_config
# from GangaCore.Utility.Config import getConfig
# from GangaCore.Utility.Virtualization import (
#     checkNative, checkDocker,
#     checkUDocker, checkSingularity
# )
# from GangaCore.testlib.GangaUnitTest import GangaUnitTest
# from GangaCore.Core.GangaRepository.container_controllers import singularity_handler, mongod_exists, ContainerCommandError

# HOST, PORT = utils.get_host_port()


# class SingularityTestHandler(GangaUnitTest):
#     """
#     Singularity Backend Controllers
#     """

#     def setUp(self):
#         """
#         """
#         extra_opts = utils.get_options(HOST, PORT)
#         extra_opts.append(("TestingFramework", "Flag", True))
#         self.sing_installated = checkSingularity()
#         database_config = get_database_config(None)
#         super(SingularityTestHandler, self).setUp(extra_opts=extra_opts)

#     def test_a1_setup_sif_file(self):
#         """
#         Download the sif file requried
#         """
#         if os.path.isfile(os.path.join(self.gangadir(), "mongo.sif")):
#             import gdown  # download the required sif file
#             url = 'https://drive.google.com/uc?id=1Z7k9LoFxGQKMjLzoe1D_jL9m5ReeixXk'
#             output = 'mongo.sif'
#             gdown.download(url, output, quiet=True)

#             import shutil  # copy the files from repository to testing directory
#             shutil.copy(
#                 src="mongo.sif",
#                 dst=self.gangadir()
#             )

#         # create logs folder in the testing dir
#         os.makedirs(os.path.join(self.gangadir(), "logs"), exist_ok=True)
#         assert True


#     def test_a2_singularity_handlers(self):
#         """
#         Test the starting and shutdown of udocker container image
#         """
#         # try:
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

#     #         # shutting down the container
#     #         singularity_handler(
#     #             action="quit",
#     #             gangadir=self.gangadir(),
#     #             database_config=database_config
#     #         )
#     #         flag = mongod_exists(
#     #             controller="singularity", cname=os.path.join(self.gangadir(), "mongo.sif")
#     #         )
#     #         assert flag is None
#     #     except Exception as e:
#     #         print(open(os.path.join(
#     #             self.gangadir(), "logs", "mongod-ganga.log"
#     #         )).read())
#     #         raise e

#     # def test_b_singularity_check_logs_created(self):
#     #     """
#     #     Check if the logs were successfully created
#     #     """

#     #     log_path = os.path.join(
#     #         self.gangadir(), "logs", "mongod-ganga.log"
#     #     )

#     #     assert os.path.exists(log_path)

#     # def test_c_check_notimplemented_actions(self):
#     #     """
#     #     test not implemented error is raised
#     #     """
#     #     with pytest.raises(NotImplementedError) as _:
#     #         random_names = ["starts", "quite"]
#     #         for action in random_names:
#     #             singularity_handler(
#     #                 action=action,
#     #                 gangadir=self.gangadir(),
#     #                 database_config=database_config
#     #             )

#     # def test_d_permission_error_singularity_handler(self):
#     #     """
#     #     Remove the data dir from $GANGADIR
#     #     and check the expected behaviour from the logs
#     #     """
#     #     # removing the folder
#     #     data_path = os.path.join(self.gangadir(), "data")

#     #     os.system(f"chmod a-x {data_path}")

#     #     with pytest.raises(ContainerCommandError) as _:
#     #         # start the handler, expecting an error
#     #         singularity_handler(
#     #             action="start",
#     #             gangadir=self.gangadir(),
#     #             database_config=database_config
#     #         )

#     #     flag = mongod_exists(
#     #         controller="singularity", cname=os.path.join(self.gangadir(), "mongo.sif")
#     #     )
#     #     assert flag is None

#     #     # check the logs
#     #     log_path = os.path.join(
#     #         self.gangadir(), "logs", "mongod-ganga.log"
#     #     )
#     #     err_string = open(log_path, "r").read()
#     #     for _log in err_string.split("\n"):
#     #         if _log:
#     #             log = json.loads(_log)
#     #             if log['s'] == "E":
#     #                 assert "Unable to determine status of lock file in the data directory" in log[
#     #                     'attr']['error']
import os
import time
from GangaCore.test.GPI.db_testing import utils
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.Virtualization import (
    checkNative, checkDocker,
    checkUDocker, checkSingularity
)
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaCore.Core.GangaRepository.container_controllers import get_database_config


HOST, PORT = utils.get_host_port()


class TestDatabaseBackends(GangaUnitTest):
    """
    Init all the database containers and see if they work
    """

    def setUp(self):
        """
        """
        extra_opts = utils.get_options(HOST, PORT)
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
        if not os.path.exists("mongo.sif"):
            import gdown  # download the required sif file
            url = 'https://drive.google.com/uc?id=1Z7k9LoFxGQKMjLzoe1D_jL9m5ReeixXk'
            output = 'mongo.sif'
            gdown.download(url, output, quiet=True)

        import shutil  # copy the files from repository to testing directory
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

        database_config = get_database_config(self.gangadir())

        if self.installations["singularity"]:
            # start the singularity container
            singularity_handler(
                action="start",
                gangadir=self.gangadir(),
                database_config=database_config
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
                database_config=database_config
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

        database_config = get_database_config(self.gangadir())

        if self.installations["udocker"]:
            # start the singularity container
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
    #             database_config=database_config
    #         )
    #         # testing status of the database container
    #         container_client = docker.from_env()
    #         flag = any([container.name == database_config["containerName"]
    #                     for container in container_client.containers.list()])
    #         assert flag

    #         # shutting the container
    #         docker_handler(
    #             action="quit",
    #             gangadir=self.gangadir(),
    #             database_config=database_config
    #         )

    #         flag = any([container.name == database_config["containerName"]
    #                     for container in container_client.containers.list()])
    #         assert not flag

    #     else:
    #         # skip the test if docker is not installed
    #         print("docker is not installed and thus skipping")
