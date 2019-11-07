from mock import patch
import unittest
from GangaCore.Utility.Virtualization import installUdocker
from io import BytesIO as StringIO

class TestInstallUDocker(unittest.TestCase):

    @patch('subprocess.check_call')
    @patch('subprocess.call')
    def test_installUDocker_success(self, mock_subprocess_call, mock_subprocess_check_call):
        mock_subprocess_call.side_effect = [0, 0, 0]
        mock_subprocess_check_call.side_effect = [0]
        assert(installUdocker() == True)
        assert(mock_subprocess_call.call_count == 2)
        mock_subprocess_check_call.assert_called_once()

    @patch('builtins.print')
    @patch('subprocess.call')
    @patch('subprocess.check_call')
    def test_installUDocker_download_fail(self, mock_subprocess_check_call, mock_subprocess_call, mock_print):
        mock_subprocess_check_call.side_effect = [1]
        assert(installUdocker() == False)
        mock_subprocess_check_call.assert_called_once()
        mock_subprocess_call.assert_not_called()
        mock_print.assert_called_with("Error downloading UDocker")

    @patch('subprocess.call')
    @patch('builtins.print')
    @patch('subprocess.check_call')
    def test_installUDocker_install_fail(self, mock_subprocess_check_call, mock_print, mock_subprocess_call):
        mock_subprocess_check_call.side_effect = [0]
        mock_subprocess_call.side_effect = [0, 1]
        assert(installUdocker() == False)
        mock_subprocess_check_call.assert_called_once()
        mock_print.assert_called_with("Error installing uDocker")
        assert(mock_subprocess_call.call_count == 2)
