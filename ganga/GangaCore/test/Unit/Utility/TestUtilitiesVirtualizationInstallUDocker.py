import unittest
import tempfile
import shutil
from mock import patch, MagicMock
from GangaCore.Utility.Virtualization import installUdocker
from urllib.error import URLError

class TestInstallUDocker(unittest.TestCase):

    @patch('subprocess.call')
    def test_installUDocker_success(self, mock_subprocess_call):
        mock_subprocess_call.side_effect = [0, 0]
        dir = tempfile.mkdtemp()
        installUdocker(dir)
        shutil.rmtree(dir)
        assert(mock_subprocess_call.call_count == 2)

    @patch('GangaCore.Utility.Virtualization.urlopen')
    def test_installUDocker_download_fail(self, mock_urlopen):
        cm = MagicMock()
        cm.getcode.return_value = 404
        cm.read.return_value = 'abc'
        cm.__enter__.side_effect = URLError('Error')
        cm.__enter__.return_value = cm
        mock_urlopen.return_value = cm
        self.assertRaises(URLError, installUdocker)
        assert mock_urlopen.call_count == 1
