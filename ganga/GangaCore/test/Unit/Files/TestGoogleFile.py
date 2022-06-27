import os
import unittest
from unittest.mock import ANY, patch

from ganga.GangaCore.testlib.GangaUnitTest import GangaUnitTest


@patch('GangaCore.GPI.GoogleFile._impl._setup_service')
class TestGoogleFile(GangaUnitTest):

    @patch('GangaCore.GPI.GoogleFile._impl.download_file_from_drive')
    def test_get_googlefile(self, download_mock, google_service_mock):
        """Test GoogleFile's get method which is used to download a given file/name pattern from Google Drive"""
        from GangaCore.GPI import GoogleFile

        f = GoogleFile('test.txt')
        google_service_mock.return_value.files.return_value.list.return_value.execute.return_value = {
            'files': [{'id': 1}, {'id': 2}]}
        f.get()

        self.assertEqual(download_mock.call_count, 2)
        download_mock.assert_any_call(google_service_mock.return_value, 1, ANY, 'test.txt')
        download_mock.assert_any_call(google_service_mock.return_value, 2, ANY, 'test.txt')

    def test_put_googlefile(self, google_service_mock):
        """Test GoogleFile's remove method which is used to upload a local file to Google Drive."""
        from GangaCore.GPI import GoogleFile

        try:
            filename = 'test.txt'
            with open(filename, 'w'):
                pass
            f = GoogleFile(filename)

            create_method_mock = google_service_mock.return_value.files.return_value.create
            dummy_file_id = 'dummyid'
            create_method_mock.return_value.execute.return_value = {'id': dummy_file_id}
            f.put()

            # The create method mock is called at least twice in the actual code to use its inner execute method.
            # Get the call that includes the file creation parameters
            create_file_args = next(filter(lambda call: len(call) > 0, create_method_mock.call_args), None)

            self.assertTrue(create_file_args is not None)
            self.assertTrue(create_file_args['body']['name'] == filename)
            self.assertEqual(f._impl.id, dummy_file_id)
            self.assertEqual(f.downloadURL, f"https://drive.google.com/file/d/{dummy_file_id}")
        finally:
            if os.path.exists(filename):
                os.remove(filename)

    def test_remove_googlefile_permanent(self, google_service_mock):
        """Test GoogleFile's remove permament method which is used to permanently delete a file from Google Drive."""
        from GangaCore.GPI import GoogleFile

        f = GoogleFile('test.txt')
        dummy_file_id = 'dummyid'
        f._impl.id = dummy_file_id
        f.remove(permanent=True)

        delete_method_mock = google_service_mock.return_value.files.return_value.delete
        delete_method_mock.assert_any_call(fileId=dummy_file_id)

    def test_remove_googlefile_to_trash(self, google_service_mock):
        """Test GoogleFile's remove non permanent which is used to a Google Drive file to the trash."""
        from GangaCore.GPI import GoogleFile

        f = GoogleFile('test.txt')
        dummy_file_id = 'dummyid'
        f._impl.id = dummy_file_id
        f.remove(permanent=False)

        update_method_mock = google_service_mock.return_value.files.return_value.update
        update_method_mock.assert_any_call(fileId=dummy_file_id, body={"trashed": True})

    def test_restore_googlefile_from_trash(self, google_service_mock):
        """Test GoogleFile's restore method which is used to restore a Google File from trash."""
        from GangaCore.GPI import GoogleFile

        f = GoogleFile('test.txt')
        dummy_file_id = 'dummyid'
        f._impl.id = dummy_file_id
        f.restore()

        update_method_mock = google_service_mock.return_value.files.return_value.update
        update_method_mock.assert_any_call(fileId=dummy_file_id, body={"trashed": False})


if __name__ == "__main__":
    unittest.main()
