from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

from os import path, stat, unlink

import time

from .utilFunctions import getJobsPath, getXMLDir, getXMLFile, getIndexFile

global_AutoStartReg = True

notXMLStr = ['ThisIsNOTXML' for _ in range(20)]
badStr = ''.join(notXMLStr)

class TestXMLCorruption(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestXMLCorruption, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')
        setConfigOption('Configuration', 'AutoStartReg', global_AutoStartReg)

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from Ganga.Utility.Config import getConfig
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from Ganga.GPI import Job, jobs
        j=Job()
        assert len(jobs) == 1

        global global_AutoStartReg
        global_AutoStartReg = False

    def test_b_TestRemoveXML(self):
        # Remove XML force to use backup
        XMLFileName = getXMLFile(0)

        unlink(XMLFileName)

        assert not path.isfile(XMLFileName)
        assert path.isfile(XMLFileName+'~')

        global global_AutoStartReg
        global_AutoStartReg = True

    def test_c_TestBackupLoad(self):
        # Test loading from backup
        from Ganga.GPI import jobs

        assert len(jobs) == 1

        ## trigger load
        backend2 = jobs(0).backend

        assert backend2 is not None

        global global_AutoStartReg
        global_AutoStartReg = False

    def test_d_TestCorruptXML(self):
        # Corrupt the XML file
        XMLFileName = getXMLFile(0)

        unlink(XMLFileName)
        assert not path.isfile(XMLFileName)

        with open(XMLFileName, 'w') as handler:
            handler.write(badStr)
            handler.flush()

        from tempfile import NamedTemporaryFile
        with NamedTemporaryFile(delete=False) as myTempfile:
            myTempfile.write(badStr)
            myTempfile.flush()
            myTempName = myTempfile.name

        import filecmp
        assert filecmp.cmp(XMLFileName, myTempName)
        unlink(myTempName)

        global global_AutoStartReg
        global_AutoStartReg = True

    def test_e_TestCorruptLoad(self):
        # Test loading of backup when corrupt
        from Ganga.GPI import jobs, Job

        assert len(jobs) == 1

        backend2 = jobs(0).backend

        assert isinstance(jobs(0), Job)
        assert backend2 is not None

        XMLFileName = getXMLFile(0)
        
        from Ganga.GPIDev.Base.Proxy import stripProxy

        print("%s" % stripProxy(jobs(0)).__dict__)
        assert stripProxy(jobs(0))._dirty is True

        stripProxy(jobs(0))._setDirty()
        stripProxy(jobs(0))._getRegistry().flush_all()

        from tempfile import NamedTemporaryFile
        with NamedTemporaryFile(delete=False) as myTempfile:
            myTempfile.write(badStr)
            myTempfile.flush()
            myTempName=myTempfile.name

        assert open(XMLFileName).read() != open(myTempName).read()
        unlink(myTempName)

