from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

from os import path, stat, unlink

import time

from .utilFunctions import getJobsPath, getXMLDir, getXMLFile, getIndexFile

global_AutoStartReg = True

notXMLStr = ['ThisIsNOTXML' for _ in range(20)]
badStr = ''.join(notXMLStr)

class TestXMLGenAndLoad(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestXMLGenAndLoad, self).setUp()
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

        handler = open(XMLFileName, 'w')
        handler.write(badStr)
        handler.flush()
        handler.close()

        from tempfile import NamedTemporaryFile
        myTempfile = NamedTemporaryFile()
        myTempfile.write(badStr)
        myTempfile.flush()

        import filecmp
        assert filecmp.cmp(XMLFileName, myTempfile.name)

        global global_AutoStartReg
        global_AutoStartReg = True

    def test_d_TestCorruptLoad(self):
        # Test loading of backup when corrupt
        from Ganga.GPI import jobs

        assert len(jobs) == 1

        backend2 = jobs(0).backend

        assert backend2 is not None

        XMLFileName = getXMLFile(0)
        
        from tempfile import NamedTemporaryFile
        myTempfile = NamedTemporaryFile()
        myTempfile.write(badStr)
        myTempfile.flush()

        import filecmp
        assert not filecmp.cmp(XMLFileName, myTempfile.name)

