from GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestInteractiveJobOutput(GangaUnitTest):
    def testInteractiveJobOutput(self):
        from GangaCore.GPI import Job, Executable, Interactive, LocalFile
        from GangaTest.Framework.utils import sleep_until_completed
        import os

        app = Executable()
        app.exe = 'echo'
        app.args = ["Hello World", ""]
        j = Job(backend=Interactive(), application=app,
                outputfiles=[LocalFile('stdout'), LocalFile('stderr')])
        j.submit()

        self.assertTrue(sleep_until_completed(j, 60),
                        'Timeout on registering Interactive job as completed')

        stdout_path = os.path.join(j.outputdir, 'stdout')
        stderr_path = os.path.join(j.outputdir, 'stderr')
        self.assertTrue(os.path.exists(stdout_path), 'stdout file not created')
        self.assertTrue(os.path.exists(stderr_path), 'stderr file not created')

        expected_stdout_content = b'Hello World'
        expected_stderr_content = b''

        with open(stdout_path, 'rb') as stdout_file:
            actual_stdout_content = stdout_file.read()
            self.assertIn(expected_stdout_content, actual_stdout_content,
                          'Incorrect content in stdout file')

        with open(stderr_path, 'rb') as stderr_file:
            actual_stderr_content = stderr_file.read()
            self.assertIn(expected_stderr_content, actual_stderr_content,
                          'Incorrect content in stderr file')
