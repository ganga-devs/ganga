import os
import unittest
from shutil import rmtree
from unittest.mock import MagicMock, patch

from ganga.GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestLocalFile(GangaUnitTest):
    def setUp(self):
        super(TestLocalFile, self).setUp()

        # Create sample files for testing the LocalFile class
        self.local_file_list = ['test1.txt', 'test2.txt', 'test3.txt']
        for filename in self.local_file_list:
            with open(filename, 'w'):
                pass

    def tearDown(self):
        super(TestLocalFile, self).tearDown()

        for file in self.local_file_list:
            if os.path.exists(file):
                os.remove(file)

    def test_get_local_filename_list(self):
        """Test LocalFile's location method which is used to match wildcard names"""
        from GangaCore.GPI import LocalFile

        f = LocalFile("test*.txt")

        self.assertEqual(sorted(f.location()), sorted(self.local_file_list))

        [os.remove(filename) for filename in self.local_file_list]

    # Removing a LocalFile requires keyboard confirmation. Mock the input to always return 'y'
    @patch('builtins.input', return_value='y')
    def test_remove_localfile(self, input_mock):
        """Test LocalFile's remove method which is used to delete all files matching the name pattern"""
        from GangaCore.GPI import LocalFile

        f = LocalFile("test*.txt")
        f.remove()

        self.assertEqual(input_mock.call_count, len(self.local_file_list))
        [self.assertFalse(os.path.exists(file)) for file in self.local_file_list]

    @patch('GangaCore.GPIDev.Base.GangaObject.getJobObject')
    def test_put_localfile(self, get_job_mock):
        """Test LocalFile's put method which is used to copy files
        from the LocalFile localdir to the parent Job's outputdir
        """
        from GangaCore.GPI import LocalFile

        f = LocalFile("test1.txt")
        os.mkdir("testing")
        localdir = os.path.join(os.getcwd(), "testing")
        try:
            f.localDir = localdir
            job_obj_mock = MagicMock(name="ParentJob")
            job_obj_mock.outputdir = os.getcwd()
            get_job_mock.return_value = job_obj_mock
            f.put()

            self.assertEqual(os.listdir(localdir), ['test1.txt'])
        finally:
            if os.path.exists(localdir):
                rmtree(localdir)


if __name__ == "__main__":
    unittest.main()
