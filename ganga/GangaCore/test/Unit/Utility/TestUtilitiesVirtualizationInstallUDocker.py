import unittest
import tempfile
import shutil
from unittest.mock import patch, MagicMock, mock_open
from GangaCore.Utility.Virtualization import installUdocker
from urllib.error import URLError

class TestInstallUDocker(unittest.TestCase):

    @patch('subprocess.call')
    def test_installUDocker_success(self, mock_subprocess_call):
        mock_subprocess_call.side_effect = [0, 0]
        with tempfile.TemporaryDirectory() as dir:
            installUdocker(dir)
        assert(mock_subprocess_call.call_count == 2)

    @patch('GangaCore.Utility.Virtualization.open')
    @patch('subprocess.call')
    @patch('subprocess.check_call')
    @patch('GangaCore.Utility.Virtualization.urlopen')
    def test_installUDocker_download_fail(self,
                                          mock_urlopen, mock_check_call, mock_subprocess_call, mock_main_open):
        mock_main_open=mock_open()
        mock_check_call.side_effect = [1]
        cm = MagicMock()
        cm.getcode.return_value = 404
        cm.read.return_value = 'abc'
        cm.__enter__.side_effect = URLError('Error')
        cm.__enter__.return_value = cm
        mock_urlopen.return_value = cm
        self.assertRaises(OSError, installUdocker)
        assert(mock_urlopen.call_count == 1)
        assert(mock_check_call.call_count == 1)
        assert(mock_subprocess_call.call_count == 0)
