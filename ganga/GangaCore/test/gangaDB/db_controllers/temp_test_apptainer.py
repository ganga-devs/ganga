# """Test the apptainer container
# Custom test for apptainer container: not used here
# """

# import os
# import time
# import json
# import utils
# import pytest
# from GangaCore.Core.GangaRepository.container_controllers import get_database_config
# from GangaCore.Utility.Config import getConfig
# from GangaCore.Utility.Virtualization import (
#     checkDocker,
#     checkUDocker, checkApptainer
# )
# from GangaCore.testlib.GangaUnitTest import GangaUnitTest
# from GangaCore.Core.GangaRepository.container_controllers import apptainer_handler, \
# mongod_exists, ContainerCommandError, checkNative

# HOST, PORT = utils.get_host_port()


# class ApptainerTestHandler(GangaUnitTest):
#     """
#     Apptainer Backend Controllers
#     """

#     def setUp(self):
#         """
#         """
#         extra_opts = utils.get_options(HOST, PORT)
#         extra_opts.append(("TestingFramework", "Flag", True))
#         self.sing_installated = checkApptainer()
#         database_config = get_database_config(None)
#         super(ApptainerTestHandler, self).setUp(extra_opts=extra_opts)

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


#     def test_a2_apptainer_handlers(self):
#         """
#         Test the starting and shutdown of udocker container image
#         """
#         # try:
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

#             # shutting down the container
#             apptainer_handler(
#                 action="quit",
#                 gangadir=self.gangadir(),
#                 database_config=database_config
#             )
#             flag = mongod_exists(
#                 controller="apptainer", cname=os.path.join(self.gangadir(), "mongo.sif")
#             )
#             assert flag is None
#         except Exception as e:
#             print(open(os.path.join(
#                 self.gangadir(), "logs", "mongod-ganga.log"
#             )).read())
#             raise e

#     def test_b_apptainer_check_logs_created(self):
#         """
#         Check if the logs were successfully created
#         """

#         log_path = os.path.join(
#             self.gangadir(), "logs", "mongod-ganga.log"
#         )

#         assert os.path.exists(log_path)

#     def test_c_check_notimplemented_actions(self):
#         """
#         test not implemented error is raised
#         """
#         with pytest.raises(NotImplementedError) as _:
#             random_names = ["starts", "quite"]
#             for action in random_names:
#                 apptainer_handler(
#                     action=action,
#                     gangadir=self.gangadir(),
#                     database_config=database_config
#                 )

#     def test_d_permission_error_apptainer_handler(self):
#         """
#         Remove the data dir from $GANGADIR
#         and check the expected behaviour from the logs
#         """
#         # removing the folder
#         data_path = os.path.join(self.gangadir(), "data")

#         os.system(f"chmod a-x {data_path}")

#         with pytest.raises(ContainerCommandError) as _:
#             # start the handler, expecting an error
#             apptainer_handler(
#                 action="start",
#                 gangadir=self.gangadir(),
#                 database_config=database_config
#             )

#         flag = mongod_exists(
#             controller="apptainer", cname=os.path.join(self.gangadir(), "mongo.sif")
#         )
#         assert flag is None

#         # check the logs
#         log_path = os.path.join(
#             self.gangadir(), "logs", "mongod-ganga.log"
#         )
#         err_string = open(log_path, "r").read()
#         for _log in err_string.split("\n"):
#             if _log:
#                 log = json.loads(_log)
#                 if log['s'] == "E":
#                     assert "Unable to determine status of lock file in the data directory" in log[
#                         'attr']['error']
