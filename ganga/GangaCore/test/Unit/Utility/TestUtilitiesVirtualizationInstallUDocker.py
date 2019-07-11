from mock import call, patch, Mock
import unittest
from GangaCore.Utility.Virtualization import installUdocker
from io import BytesIO as StringIO

class TestCheckDocker(unittest.TestCase):

    @patch('subprocess.check_call')
    @patch('subprocess.call')
    def test_checkUDocker_success(self, mock_subprocess_call, mock_subprocess_check_call):
        mock_subprocess_call.side_effect = [0, 0, 0]
        mock_subprocess_check_call.side_effect = [0]
        assert(installUdocker() == True)
        assert(mock_subprocess_call.call_count == 2)
        mock_subprocess_check_call.assert_called_once()

    @patch('sys.stdout', new_callable=StringIO)
    @patch('subprocess.call')
    @patch('subprocess.check_call')
    def test_checkUDocker_download_fail(self, mock_subprocess_check_call, mock_subprocess_call, mock_print):
        mock_subprocess_check_call.side_effect = [1]
        assert(installUdocker() == False)
        mock_subprocess_check_call.assert_called_once()
        self.assertEqual(mock_print.getvalue(), "Error downloading UDocker\n")
        mock_subprocess_call.assert_not_called()

    @patch('subprocess.call')
    @patch('sys.stdout', new_callable=StringIO)
    @patch('subprocess.check_call')
    def test_checkUDocker_install_fail(self, mock_subprocess_check_call, mock_print, mock_subprocess_call):
        mock_subprocess_check_call.side_effect = [0]
        mock_subprocess_call.side_effect = [0, 1]
        assert(installUdocker() == False)
        mock_subprocess_check_call.assert_called_once()
        self.assertEqual(mock_print.getvalue(), "Error installing uDocker\n")
        assert(mock_subprocess_call.call_count == 2)