from mock import patch
import unittest
from GangaCore.Utility.Virtualization import checkUDocker


class TestCheckUDocker(unittest.TestCase):

    @patch('subprocess.call')
    def test_checkUDocker_success(self, mock_subprocess_call):
        mock_subprocess_call.side_effect = [0]
        assert(checkUDocker() == True)
        mock_subprocess_call.assert_called_once()

    @patch('subprocess.call')
    def test_checkUDocker_fail(self,mock_subprocess_call):
        mock_subprocess_call.side_effect = [1]
        assert(checkUDocker() == False)
        mock_subprocess_call.assert_called_once()