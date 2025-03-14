from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaCore.Lib.Virtualization import Docker
from GangaCore.Utility.job_monitoring import sleep_until_completed


class TestDocker(GangaUnitTest):
    def test_DockerNoImageArg(self):
        """
        The test_DockerNoImageArg function tests that Ganga can
        submit a virtualization job using the default Docker image.

        This test will fail if Ganga cannot find the job in the registry or
        it cannot intialize the container with the default Docker image.
        """
        from GangaCore.GPI import Job
        j = Job(name="dockertest")
        self.assertEqual(j.name, 'dockertest', "Something wrong with how\
Job was imported or initialized.")
        j.virtualization = Docker()
        self.assertEqual(j.virtualization.image, '', "Docker image was not\
passed correctly as an argument.")
        j.submit()
        sleep_until_completed(j)
        j.remove()

    def test_DockerBareImageArg(self):
        """
        The test_DockerBareImageArg function tests that the Docker image
        is passed correctly as an argument without the 'image=' prefix.

        This test will fail if Ganga cannot find the job in the registry or
        it cannot intialize the container with the desired Docker image.
        """
        from GangaCore.GPI import Job
        j = Job(name="dockertest")
        self.assertEqual(j.name, 'dockertest', "Something wrong with how\
Job was imported or initialized.")
        j.virtualization = Docker('fedora:latest')
        self.assertEqual(j.virtualization.image, 'fedora:latest', "Docker\
image was not passed correctly as an argument.")
        j.submit()
        sleep_until_completed(j)
        j.remove()

    def test_DockerImageArg(self):
        """
        The DockerImageArg function tests that the Docker image
        is passed correctly as an argument with the 'image=' prefix.

        This test will fail if Ganga cannot find the job in the registry or
        it cannot intialize the container with the desired Docker image.
        """
        from GangaCore.GPI import Job
        j = Job(name="dockertest")
        self.assertEqual(j.name, 'dockertest', "Something wrong with how\
Job was imported or initialized.")
        j.virtualization = Docker(image='fedora:latest')
        self.assertEqual(j.virtualization.image, 'fedora:latest', "Docker\
image was not passed correctly as an argument.")
        j.submit()
        sleep_until_completed(j)
        j.remove()

    def test_DockerImageArgWithMode(self):
        """
        The DockerImageArgWithMode function tests that the Docker image
        is passed correctly as an argument with the 'image=' prefix. It
        also test if mode is explicitly set to 'P1'.

        This test will fail if Ganga cannot find the job in the registry
        or it cannot intialize the container with the desired Docker image
        and mode.
        """
        from GangaCore.GPI import Job
        j = Job(name="dockertest")
        self.assertEqual(j.name, 'dockertest', "Something wrong with how\
Job was imported or initialized.")
        j.virtualization = Docker(image='fedora:latest', mode='P1')
        self.assertEqual(j.virtualization.image, 'fedora:latest', "Docker\
image was not passed correctly as an argument.")
        self.assertEqual(j.virtualization.mode, 'P1', "Mode was not passed\
correctly as an argument.")
        j.submit()
        sleep_until_completed(j)
        j.remove()
