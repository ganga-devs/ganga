# """Test the docker container 
# """

# import os
# import time
# import json
# import docker
# import pytest
# from GangaCore.Core.GangaRepository.container_controllers import get_database_config
# from GangaCore.test.GPI.db_testing import utils
# from GangaCore.Utility.Config import getConfig
# from GangaCore.Utility.Virtualization import checkDocker
# from GangaCore.testlib.GangaUnitTest import GangaUnitTest
# from GangaCore.Core.GangaRepository.container_controllers import docker_handler, mongod_exists, ContainerCommandError

# HOST, PORT = utils.get_host_port()


# class DockerTestHandler(GangaUnitTest):
#     """
#     docker Backend Controllers
#     """

#     def setUp(self):
#         """
#         """
#         extra_opts = utils.get_options(HOST, PORT)
#         extra_opts.append(("TestingFramework", "Flag", True))
#         self.docker_installed = checkDocker()
#         self.container_client = docker.from_env()
#         self.database_config = get_database_config(None)
#         super(DockerTestHandler, self).setUp(extra_opts=extra_opts)

#     def test_docker_handler(self):
#         """
#         Test the starting and shutdown of docker container image
#         """
#         import subprocess

#         if self.docker_installed:
#             # start the docker container
#             print(self.database_config)
#             assert False
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
