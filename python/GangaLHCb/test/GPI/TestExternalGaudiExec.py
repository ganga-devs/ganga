from __future__ import absolute_import

from os import path
import shutil
from tempfile import mkdtemp

from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.testlib.GangaUnitTest import GangaUnitTest
from Ganga.testlib.mark import external
from Ganga.testlib.monitoring import run_until_completed

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
        from Ganga.GPI import Job, prepareGaudiExec

        j = Job(application=prepareGaudiExec('DaVinci', latestDaVinci(), TestExternalGaudiExec.tmpdir_release))

        assert j.application.directory == path.join(TestExternalGaudiExec.tmpdir_release, 'DaVinciDev_%s' % latestDaVinci())

        assert path.isfile(path.join(TestExternalGaudiExec.tmpdir_release, 'DaVinciDev_%s' % latestDaVinci(), 'run'))

        assert path.isfile(path.join(TestExternalGaudiExec.tmpdir_release, 'DaVinciDev_%s' % latestDaVinci(), 'Makefile'))

    def testParseInputFile(self):
        """
        Test that we can parse a fake opts file and get the inputdata from it
        """
        from Ganga.GPI import jobs

        j = jobs[-1]

        myOptsFile = path.join(TestExternalGaudiExec.tmpdir_release, 'myOpts.py')

        FileBuffer('myOpts.py', inputOptsFile()).create(myOptsFile)

        assert path.isfile(myOptsFile)

        j.application.readInputData(myOptsFile)

        assert len(j.inputdata) == 1

    def testPrepareJob(self):

        from Ganga.GPI import Job, LocalFile, prepareGaudiExec

        import os
        if os.path.exists(TestExternalGaudiExec.tmpdir_release):
            os.system("rm -rf %s/*" % TestExternalGaudiExec.tmpdir_release)
            
        j = Job(application=prepareGaudiExec('DaVinci', latestDaVinci(), TestExternalGaudiExec.tmpdir_release))

        myHelloOpts = path.join(TestExternalGaudiExec.tmpdir_release, 'hello.py')

        FileBuffer('hello.py', 'print("Hello")').create(myHelloOpts)

        assert path.isfile(myHelloOpts)

        j.application.options=[LocalFile(myHelloOpts)]

        j.prepare()

    def testSubmitJob(self):

        from Ganga.GPI import jobs
        
        j = jobs[-1]
        
        j.submit()

    def testSubmitJobComplete(self):
        """
        Test that the job completes successfully
        """

        from Ganga.GPI import jobs
        from Ganga.GPI import Job, LocalFile, prepareGaudiExec

        import os
        if os.path.exists(TestExternalGaudiExec.tmpdir_release):
            os.system("rm -rf %s/*" % TestExternalGaudiExec.tmpdir_release)

        j = Job(application=prepareGaudiExec('DaVinci', latestDaVinci(), TestExternalGaudiExec.tmpdir_release))

        myOpts = path.join(TestExternalGaudiExec.tmpdir_release, 'testfile.py')

        FileBuffer('testfile.py', 'print("ThisIsATest")').create(myOpts)

        j.application.options=[LocalFile(myOpts)]
        
        j.submit()

        run_until_completed(j)

        assert j.status == 'completed'

        outputfile = path.join(j.outputdir, 'stdout')

        assert path.isfile(outputfile)

        assert 'testfile.py' in open(outputfile).read()

        assert 'data.py' in open(outputfile).read()

        assert 'ThisIsATest' in open(outputfile).read()

        assert j.application.platform in open(outputfile).read()

    @classmethod
    def tearDownClass(cls):
        """
        Remove the 'release area'
        """
        shutil.rmtree(cls.tmpdir_release, ignore_errors=True)

