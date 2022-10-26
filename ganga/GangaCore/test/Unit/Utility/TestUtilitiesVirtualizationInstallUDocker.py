import unittest
import tempfile
import os
from unittest.mock import patch

from GangaCore.Utility.Virtualization import installUDocker
from GangaCore.Core.exceptions import GangaException


class TestInstallUDocker(unittest.TestCase):

    def test_installUDocker_success(self):
        with tempfile.TemporaryDirectory() as dir:
            installUDocker(dir)

    @patch('subprocess.call')
    def test_installUDocker_download_fail(self, mock_subprocess_call):
        mock_subprocess_call.side_effect = [1]
        with tempfile.TemporaryDirectory() as dir:
            with self.assertRaises(GangaException):
                installUDocker(dir)
