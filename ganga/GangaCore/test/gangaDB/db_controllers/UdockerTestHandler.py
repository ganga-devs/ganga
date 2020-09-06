# """
# TODO:
# 1. Added tests to check fail conditions
# """
# import os
# import time
# from GangaCore.test.GPI.db_testing import utils
# from GangaCore.Utility.Config import getConfig
# from GangaCore.Utility.Virtualization import (
#     checkNative, checkDocker,
#     checkUDocker, checkSingularity
# )
# from GangaCore.testlib.GangaUnitTest import GangaUnitTest


# HOST, PORT = utils.get_host_port()


# class TestDatabaseBackends(GangaUnitTest):
#     """
#     Init all the database containers and see if they work
#     """

#     def setUp(self):
#         """
#         """
#         from GangaCore.Core.GangaRepository.container_controllers import get_database_config
#         extra_opts = utils.get_options(HOST, PORT)
#         self.database_config = get_database_config(None)
#         self.installations = {
#             "docker": checkDocker(),
#             "udocker": checkUDocker(),
#             "native": checkNative(),
#             "singularity": checkSingularity()
#         }
#         super(TestDatabaseBackends, self).setUp(extra_opts=extra_opts)

#     def test_d_docker_backend_lifetime(self):
#         """
#         Check if docker contaienr is installed
#         """
#         import docker
#         from GangaCore.Core.GangaRepository.container_controllers import docker_handler
#         if self.installations["docker"]:
#             # starting the database container
#             docker_handler(
#                 action="start",
#                 gangadir=self.gangadir(),
#                 database_config=self.database_config
#             )
#             # testing status of the database container
#             container_client = docker.from_env()
#             flag = any([container.name == self.database_config["containerName"]
#                         for container in container_client.containers.list()])
#             assert flag

#             # shutting the container
#             docker_handler(
#                 action="quit",
#                 gangadir=self.gangadir(),
#                 database_config=self.database_config
#             )

#             flag = any([container.name == self.database_config["containerName"]
#                         for container in container_client.containers.list()])
#             assert not flag

#         else:
#             # skip the test if docker is not installed
#             print("docker is not installed and thus skipping")
