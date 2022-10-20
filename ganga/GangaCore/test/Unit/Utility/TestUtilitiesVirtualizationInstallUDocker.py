import unittest
import tempfile
import shutil
import os
from unittest.mock import patch, MagicMock, mock_open
from urllib.error import URLError

from GangaCore.Utility.Virtualization import installUdocker
from GangaCore.Core.exceptions import GangaException, GangaIOError


class TestInstallUDocker(unittest.TestCase):

    def test_installUDocker_success(self):
        with tempfile.TemporaryDirectory() as dir:
            # Allow for installation itself (but not download) to fail if running test as root.
            try:
                installUdocker(dir)
            except GangaException as e:
                if os.geteuid():
                    raise e
                    pass

    @patch('GangaCore.Utility.Virtualization.open')
    @patch('subprocess.call')
    @patch('GangaCore.Utility.Virtualization.urlopen')
    def test_installUDocker_download_fail(self,
                                          mock_urlopen, mock_subprocess_call, mock_main_open):
        mock_main_open = mock_open()
        mock_subprocess_call.side_effect = [1]
        cm = MagicMock()
        cm.getcode.return_value = 404
        cm.read.return_value = 'abc'
        cm.__enter__.side_effect = URLError('Error')
        cm.__enter__.return_value = cm
        mock_urlopen.return_value = cm
        self.assertRaises(GangaIOError, installUdocker)
        assert(mock_urlopen.call_count == 1)
        assert(mock_subprocess_call.call_count == 1)
