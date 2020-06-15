try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import unittest
from GangaCore.Utility.Virtualization import checkSingularity

class TestCheckSingularity(unittest.TestCase):
    @patch('subprocess.call')
    def test_checkSingularity_success(self, mock_subprocess_call):
        mock_subprocess_call.side_effect = [0]
        assert(checkSingularity() == True)
        mock_subprocess_call.assert_called_once()


    @patch('subprocess.call')
    def test_checkSingularity_fail(self,mock_subprcess_call):
        mock_subprcess_call.side_effect = [1]
        assert(checkSingularity() == False)
        mock_subprcess_call.assert_called_once()
    
 
