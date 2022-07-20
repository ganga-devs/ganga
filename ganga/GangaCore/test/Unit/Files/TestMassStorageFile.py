import os
import unittest
from unittest.mock import ANY, patch

from ganga.GangaCore.testlib.GangaUnitTest import GangaUnitTest


@patch('GangaCore.GPI.MassStorageFile._impl.execSyscmdSubprocess')
class TestMassStorageFile(GangaUnitTest):
    def setUp(self):
        import GangaCore

        super(TestMassStorageFile, self).setUp()
        # Load mass storage commands into the test class.
        self.mass_storage_config = GangaCore.massStorageUploadOptions

    @patch('GangaCore.GPI.MassStorageFile._impl._mkdir')
    def test_put_mass_storage_file(self, mkdir_mock, exec_mock):
        """Test MassStorageFile's put method which is used to upload a local file into mass storage"""
        from GangaCore.GPI import MassStorageFile

        file_name = "test.txt"
        localdir = "/"
        f = MassStorageFile(file_name, localDir=localdir)
        exec_mock.return_value = (0, '', '')
        f.put()

        mkdir_mock.assert_called_once()
        mkdir_mock.assert_called_with(self.mass_storage_config['path'], exitIfNotExist=ANY)

        src_path = os.path.join(localdir, file_name)
        dest_path = os.path.join(self.mass_storage_config['path'], file_name)
        exec_mock.assert_called_with(f"{self.mass_storage_config['cp_cmd']} {src_path} {dest_path}")

    def test_remove_mass_storage_file(self, exec_mock):
        """Test MassStorageFile's remove method which is used to delete a file/name pattern from mass storage"""
        from GangaCore.GPI import MassStorageFile

        file_name = "test.txt"
        f = MassStorageFile(file_name)
        exec_mock.return_value = (0, '', '')
        locations = ["/tmp/test1.txt", "/tmp/test2.txt"]
        f._impl.locations = ["/tmp/test1.txt", "/tmp/test2.txt"]
        f.remove(force=True)

        self.assertEqual(exec_mock.call_count, 2)
        exec_mock.assert_any_call(f"{self.mass_storage_config['rm_cmd']} {locations[0]}")
        exec_mock.assert_any_call(f"{self.mass_storage_config['rm_cmd']} {locations[1]}")


if __name__ == "__main__":
    unittest.main()
