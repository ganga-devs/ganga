from mock import patch
import unittest
from GangaCore.Utility.Virtualization import installUdocker
from io import BytesIO as StringIO

class TestInstallUDocker(unittest.TestCase):

    @patch('builtins.open')
    @patch('subprocess.check_call')
    @patch('subprocess.call')
    def test_installUDocker_success(self, mock_subprocess_call, mock_subprocess_check_call, mock_open):
        mock_subprocess_call.side_effect = [0, 0, 0]
        mock_subprocess_check_call.side_effect = [0]
        installUdocker()
        assert(mock_subprocess_call.call_count == 2)
        mock_subprocess_check_call.assert_called_once()
        mock_open.assert_called_once()

    @patch('builtins.open')
    @patch('subprocess.call')
    @patch('subprocess.check_call')
    def test_installUDocker_download_fail(self, mock_subprocess_check_call, mock_subprocess_call, mock_open):
        mock_subprocess_check_call.side_effect = [1]
        self.assertRaises(OSError, installUdocker)
        mock_subprocess_check_call.assert_called_once()
        mock_subprocess_call.assert_not_called()
        mock_open.assert_called_once()

    @patch('builtins.open')
    @patch('subprocess.call')
    @patch('subprocess.check_call')
    def test_installUDocker_install_fail(self, mock_subprocess_check_call, mock_subprocess_call, mock_open):
        mock_subprocess_check_call.side_effect = [0]
        mock_subprocess_call.side_effect = [0, 1]
        self.assertRaises(OSError, installUdocker)
        mock_subprocess_check_call.assert_called_once()
        assert(mock_subprocess_call.call_count == 2)
        mock_open.assert_called_once()
