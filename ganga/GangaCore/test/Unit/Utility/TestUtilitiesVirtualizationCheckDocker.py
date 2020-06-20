try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import unittest
from GangaCore.Utility.Virtualization import checkDocker

class TestCheckDocker(unittest.TestCase):
    @patch('subprocess.call')
    def test_checkDocker_success(self, mock_subprocess_call):
        mock_subprocess_call.side_effect = [0]
        assert(checkDocker() == True)
        mock_subprocess_call.assert_called_once()


    @patch('subprocess.call')
    def test_checkDocker_fail(self,mock_subprcess_call):
        mock_subprcess_call.side_effect = [1]
        assert(checkDocker() == False)
        mock_subprcess_call.assert_called_once()
    
 
