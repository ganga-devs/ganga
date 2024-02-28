try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import unittest
from GangaCore.Utility.Virtualization import checkApptainer


class TestCheckApptainer(unittest.TestCase):
    @patch('subprocess.call')
    def test_checkApptainer_success(self, mock_subprocess_call):
        mock_subprocess_call.side_effect = [0]
        assert checkApptainer() is True
        mock_subprocess_call.assert_called_once()

    @patch('subprocess.call')
    def test_checkApptainer_fail(self, mock_subprcess_call):
        mock_subprcess_call.side_effect = [1]
        assert checkApptainer() is False
        mock_subprcess_call.assert_called_once()
