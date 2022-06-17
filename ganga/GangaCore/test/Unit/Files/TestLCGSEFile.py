import importlib
import os
from unittest.mock import MagicMock, patch

from ganga.GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestLCGSEFile(GangaUnitTest):
    @patch('GangaCore.GPIDev.Credentials.CredentialStore.CredentialStore.__getitem__')
    def test_put_lcsgefile(self, creds_mock):
        """Test LCGSEFile's put method which is used to upload a  local file to the grid."""
        from GangaCore.GPI import LCGSEFile

        # Manually import the LCGSEFile module due to shared module and class name confusion
        l_module = importlib.import_module('GangaCore.GPIDev.Lib.File.LCGSEFile')
        grid_shell_mock = MagicMock()
        l_module.getShell = grid_shell_mock
        grid_shell_mock.return_value.cmd1.return_value = (0, '', '')

        filename = 'test.txt'
        f = LCGSEFile(filename)
        localdir = '/'
        f.localDir = localdir
        f.put()

        grid_shell_mock.assert_called()
        upload_cmd = grid_shell_mock.return_value.cmd1.call_args[0][0]
        self.assertTrue(f.getUploadCmd() in upload_cmd)
        self.assertTrue(os.path.join(localdir, filename) in upload_cmd)
