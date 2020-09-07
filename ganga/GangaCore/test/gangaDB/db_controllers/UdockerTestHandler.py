"""Test the udocker container 
"""

import os
import time
import json
import pytest
from GangaCore.Core.GangaRepository.container_controllers import get_database_config
from GangaCore.test.GPI.db_testing import utils
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.Virtualization import checkUDocker
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaCore.Core.GangaRepository.container_controllers import udocker_handler, mongod_exists, ContainerCommandError

HOST, PORT = utils.get_host_port()


class UdockerTestHandler(GangaUnitTest):
    """
    udocker Backend Controllers
    """

    def setUp(self):
        """
        """
        extra_opts = utils.get_options(HOST, PORT)
        extra_opts.append(("TestingFramework", "Flag", True))
        self.udocker_installed = checkUDocker()
        self.database_config = get_database_config(None)
        super(UdockerTestHandler, self).setUp(extra_opts=extra_opts)

    def test_udocker_handler(self):
        """
        Test the starting and shutdown of udocker container image
        """
        import subprocess

        if self.udocker_installed:
            # start the udocker container
            udocker_handler(
                action="start",
                gangadir=self.gangadir(),
                database_config=self.database_config
            )

            # checking if the container started up
            flag = mongod_exists(
                controller="udocker", cname=self.database_config['containerName'])
            assert flag is not None

            # shutting down the container
            udocker_handler(
                action="quit",
                gangadir=self.gangadir(),
                database_config=self.database_config
            )
            time.sleep(5)
            flag = mongod_exists(
                controller="udocker", cname=self.database_config['containerName'])
            assert flag is None

        else:
            raise Exception(
                "udocker is not installed in the testing environment")
