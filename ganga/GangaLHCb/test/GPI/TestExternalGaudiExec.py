from __future__ import absolute_import

from os import path
import shutil
from tempfile import mkdtemp

from GangaCore.GPIDev.Lib.File.FileBuffer import FileBuffer
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaCore.testlib.mark import external
from GangaCore.testlib.monitoring import run_until_completed
from GangaCore.GPIDev.Base.Proxy import stripProxy

def latestLbDevVersion(app):
    import subprocess
    pipe = subprocess.Popen('lb-dev %s -l' % app, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = pipe.communicate()
    return stdout.split()[0]

def latestDaVinci():
    return latestLbDevVersion('DaVinci')

def LFNstr():
    return "/not/a/LFN"

def inputOptsFile():
    return """
from Gaudi.Configuration import *

EventSelector().Input   = [
"DATAFILE='LFN:%s' TYP='POOL_ROOTTREE' OPT='READ'",
]
""" % LFNstr


@external
class TestExternalGaudiExec(GangaUnitTest):

    tmpdir_release = mkdtemp()

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        extra_opts = [('TestingFramework', 'AutoCleanup', 'False')]
        super(TestExternalGaudiExec, self).setUp(extra_opts=extra_opts)

    def testConstruction(self):
        """
        This tests that we can construct a GaudiExec object in a simple way
        """
        from GangaCore.GPI import Job, prepareGaudiExec

        j = Job(application=prepareGaudiExec('DaVinci', latestDaVinci(), TestExternalGaudiExec.tmpdir_release))

        assert j.application.directory == path.join(TestExternalGaudiExec.tmpdir_release, 'DaVinciDev_%s' % latestDaVinci())

        assert path.isfile(path.join(TestExternalGaudiExec.tmpdir_release, 'DaVinciDev_%s' % latestDaVinci(), 'run'))

        assert path.isfile(path.join(TestExternalGaudiExec.tmpdir_release, 'DaVinciDev_%s' % latestDaVinci(), 'Makefile'))

    def testParseInputFile(self):
        """
        Test that we can parse a fake opts file and get the inputdata from it
        """
        from GangaCore.GPI import jobs

        j = TestExternalGaudiExec._constructJob()

        myOptsFile = path.join(TestExternalGaudiExec.tmpdir_release, 'myOpts.py')

        FileBuffer('myOpts.py', inputOptsFile()).create(myOptsFile)

        assert path.isfile(myOptsFile)

        j.application.readInputData(myOptsFile)

        assert len(j.inputdata) == 1

    def testPrepareJob(self):

        from GangaCore.GPI import Job, LocalFile, prepareGaudiExec

        import os
        if os.path.exists(TestExternalGaudiExec.tmpdir_release):
            os.system("rm -rf %s/*" % TestExternalGaudiExec.tmpdir_release)
            
        j = Job(application=prepareGaudiExec('DaVinci', latestDaVinci(), TestExternalGaudiExec.tmpdir_release))

        myHelloOpts = path.join(TestExternalGaudiExec.tmpdir_release, 'hello.py')

        FileBuffer('hello.py', 'print("Hello")').create(myHelloOpts)

        assert path.isfile(myHelloOpts)

        j.application.options=[LocalFile(myHelloOpts)]

        j.prepare()

        assert j.application.is_prepared.name

        assert path.isdir(j.application.is_prepared.path())

    def testSubmitJob(self):

        from GangaCore.GPI import jobs
        
        j = TestExternalGaudiExec._constructJob()

        j.submit()

    def testSubmitJobComplete(self):
        """
        Test that the job completes successfully
        """

        j = TestExternalGaudiExec._constructJob()

        j.submit()

        run_until_completed(j)

        assert j.status == 'completed'

        outputfile = path.join(j.outputdir, 'stdout')

        assert path.isfile(outputfile)

        for this_string in ('testfile.py', 'data.py', 'ThisIsATest', j.application.platform):
            assert this_string in open(outputfile).read()

    @staticmethod
    def _constructJob():
        """
        This is a helper method to construct a new GaudiExec job object for submission testing
        This just helps reduce repeat code between tests
        """

        import os
        if os.path.exists(TestExternalGaudiExec.tmpdir_release):
            os.system("rm -fr %s/" % TestExternalGaudiExec.tmpdir_release)

        from GangaCore.GPI import Job, LocalFile, prepareGaudiExec

        j = Job(application=prepareGaudiExec('DaVinci', latestDaVinci(), TestExternalGaudiExec.tmpdir_release))

        myOpts = path.join(TestExternalGaudiExec.tmpdir_release, 'testfile.py')

        FileBuffer('testfile.py', 'print("ThisIsATest")').create(myOpts)

        j.application.options=[LocalFile(myOpts)]

        return j

    def testSubmitJobDirac(self):
        """

        """

        from GangaCore.GPI import Dirac, DiracProxy

        j = TestExternalGaudiExec._constructJob()

        j.backend=Dirac(credential_requirements=DiracProxy(group='lhcb_user', encodeDefaultProxyFileName=False))

        j.submit()

        assert j.status == "submitted"

    def testSubmitJobWithInputFile(self):
        """
        This test adds a dummy inputfile into the job and tests that it is returned when the job is completed
        """

        from GangaCore.GPI import LocalFile

        tempName = 'testGaudiExecFile.txt'
        tempName2 = 'testGaudiExecFile2.txt'
        tempContent = '12345'
        tempContent2 = '67890'

        j = TestExternalGaudiExec._constructJob()

        tempFile = path.join(TestExternalGaudiExec.tmpdir_release, tempName)
        tempFile2 = path.join(TestExternalGaudiExec.tmpdir_release, tempName2)
        FileBuffer(tempName, tempContent).create(tempFile)
        FileBuffer(tempName2, tempContent2).create(tempFile2)

        j.inputfiles = [tempFile, LocalFile(tempFile2)]
        j.outputfiles = [LocalFile(tempName), LocalFile(tempName2)]

        j.submit()

        run_until_completed(j)

        assert j.status == 'completed'

        outputDir = stripProxy(j).getOutputWorkspace(create=False).getPath()

        assert path.isfile(tempFile)
        assert path.isfile(tempFile2)

        assert tempContent in open(tempFile).read()
        assert tempContent2 in open(tempFile2).read()

    def testSubmitJobDiracWithInput(self):

        j = TestExternalGaudiExec._constructJob()

        from GangaCore.GPI import LocalFile, Dirac, DiracProxy

        j.backend=Dirac(credential_requirements=DiracProxy(group='lhcb_user', encodeDefaultProxyFileName=False))

        tempName = 'testGaudiExecFile.txt'
        tempContent = '12345'
        tempFile = path.join(TestExternalGaudiExec.tmpdir_release, tempName)
        FileBuffer(tempName, tempContent).create(tempFile)

        j.inputfiles = [tempFile]
        j.outputfiles = [LocalFile(tempName)]

        j.submit()

        assert j.status == "submitted"

    @classmethod
    def tearDownClass(cls):
        """
        Remove the 'release area'
        """
        shutil.rmtree(cls.tmpdir_release, ignore_errors=True)

