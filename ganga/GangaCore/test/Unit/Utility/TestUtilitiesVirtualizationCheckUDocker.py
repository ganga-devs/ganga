try:
    from unittest.mock import patch, call
except ImportError:
    from mock import patch, call
import unittest

from GangaCore.Utility.Virtualization import checkUDocker


class TestCheckUDocker(unittest.TestCase):

    @patch('subprocess.call')
    @patch('os.path.isfile')
    def test_checkUDocker_success(self, mock_is_file, mock_subprocess_call):
        mock_is_file.sideeffect = [True]
        mock_subprocess_call.side_effect = [0]
        assert(checkUDocker() == True)
        mock_subprocess_call.assert_called_once()

    @patch('subprocess.call')
    @patch('os.path.isfile')
    def test_checkUDocker_fail(self, mock_is_file, mock_subprocess_call):
        mock_is_file.sideeffect = [True]
        mock_subprocess_call.side_effect = [1]
        assert(checkUDocker() == False)
        assert(mock_subprocess_call.call_count==2)