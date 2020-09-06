# """Test the docker container 
# """

# import json
# import os
# import time

# import docker
# import pytest

# from GangaCore.Core.GangaRepository.container_controllers import (
#     get_database_config, mongod_exists, docker_handler
# )
# from GangaCore.test.GPI.db_testing import utils
# from GangaCore.testlib.GangaUnitTest import GangaUnitTest
# from GangaCore.Utility.Config import getConfig

# HOST, PORT = utils.get_host_port()


# class DockerTestHandler(GangaUnitTest):
#     """
#     Docker Backend Controllers
#     """

#     def setUp(self):
#         """
#         """
#         extra_opts = utils.get_options(HOST, PORT)
#         extra_opts.append(("TestingFramework", "Flag", True))
#         self.database_config = get_database_config(None)
#         super(DockerTestHandler, self).setUp(extra_opts=extra_opts)

#     def test_docker_life(self):
#         """
#         Check if the logs were successfully created
#         """

#         # starting the database container
#         docker_handler(
#             action="start",
#             gangadir=self.gangadir(),
#             database_config=self.database_config
#         )
#         # # testing status of the database container
#         # container_client = docker.from_env()
#         # flag = any([container.name == self.database_config["containerName"]
#         #             for container in container_client.containers.list()])
#         # assert flag

#         # # shutting the container
#         # docker_handler(
#         #     action="quit",
#         #     gangadir=self.gangadir(),
#         #     database_config=self.database_config
#         # )

#         # flag = any([container.name == self.database_config["containerName"]
#         #             for container in container_client.containers.list()])
#         # assert not flag

#     def test_temp(self):
#         print(self.gangadir())