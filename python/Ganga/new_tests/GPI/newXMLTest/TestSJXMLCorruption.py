from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

from os import path, stat, unlink

import time

from .utilFunctions import getJobsPath, getXMLDir, getSJXMLFile, getIndexFile

global_AutoStartReg = True

notXMLStr = ['ThisIsNOTXML' for _ in range(20)]
badStr = ''.join(notXMLStr)

class TestSJXMLCorruption(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestSJXMLCorruption, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')
        setConfigOption('Configuration', 'AutoStartReg', global_AutoStartReg)

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from Ganga.Utility.Config import getConfig
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from Ganga.GPIDev.Base.Proxy import stripProxy
        from Ganga.GPI import Job, jobs, ArgSplitter
        j=Job()
        orig_sj_proxy = j.subjobs
        j.splitter = ArgSplitter()
        j.splitter.args = [[0], [1]]
        i=0
        for sj in stripProxy(j.splitter).split(stripProxy(j)):
            sj.id = i
            stripProxy(j).subjobs.append(sj)
            i=i+1
        assert len(jobs) == 1
        assert len(j.subjobs) == 2

        sj_proxy = j.subjobs
        assert sj_proxy is j.subjobs
        assert orig_sj_proxy is sj_proxy

        for sj in j.subjobs:
            assert isinstance(sj, Job)

        global global_AutoStartReg
        global_AutoStartReg = False

        stripProxy(sj)._getRegistry().flush_all()

        for sj in j.subjobs:
            stripProxy(sj)._setDirty()

        stripProxy(sj)._getRegistry().flush_all()

        for sj in j.subjobs:
            stripProxy(sj)._setDirty()

    def test_b_TestRemoveSJXML(self):
        # Remove XML force to use backup
        from Ganga.GPI import jobs
        XMLFileName = getSJXMLFile(jobs(0).subjobs(0))

        unlink(XMLFileName)

        assert not path.isfile(XMLFileName)
        assert path.isfile(XMLFileName+'~')

        global global_AutoStartReg
        global_AutoStartReg = True

    def test_c_TestBackupLoad(self):
        # Test loading from backup
        from Ganga.GPI import jobs, Job

        assert len(jobs) == 1
        assert len(jobs(0).subjobs) == 2

        for sj in jobs(0).subjobs:
            assert isinstance(sj, Job)

        ## trigger load
        backend2 = jobs(0).subjobs(0).backend

        assert backend2 is not None

        from Ganga.GPIDev.Base.Proxy import stripProxy
        assert stripProxy(jobs(0).subjobs(0))._dirty is True
        print("jobs(0): %s" % str(stripProxy(jobs(0))))
        assert stripProxy(jobs(0))._dirty is True

        stripProxy(sj)._getRegistry().flush_all()

        global global_AutoStartReg
        global_AutoStartReg = False

    def test_d_TestCorruptXML(self):
        # Corrupt the XML file
        from Ganga.GPI import jobs, Job
        assert isinstance(jobs(0).subjobs(0), Job)
        XMLFileName = getSJXMLFile(jobs(0).subjobs(0))

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

        from Ganga.GPIDev.Base.Proxy import stripProxy
        assert stripProxy(jobs(0).subjobs(0))._dirty is False
        assert stripProxy(jobs(0))._dirty is False

        assert open(XMLFileName, 'r').read() == open(myTempName, 'r').read()
        unlink(myTempName)

        global global_AutoStartReg
        global_AutoStartReg = True

    def test_e_TestCorruptLoad(self):
        # Test loading of backup when corrupt
        from Ganga.GPI import jobs

        assert len(jobs) == 1
        assert len(jobs(0).subjobs) == 2

        backend2 = jobs(0).subjobs(0).backend

        assert backend2 is not None

        XMLFileName = getSJXMLFile(jobs(0).subjobs(0))
        
        from tempfile import NamedTemporaryFile
        with NamedTemporaryFile(delete=False) as myTempfile:
            myTempfile.write(badStr)
            myTempfile.flush()
            myTempName=myTempfile.name

        from Ganga.GPIDev.Base.Proxy import stripProxy
        assert stripProxy(jobs(0).subjobs(0))._dirty is True
        assert stripProxy(jobs(0))._dirty is True

        stripProxy(jobs(0).subjobs(0))._getRegistry().flush_all()

        assert open(XMLFileName).read() != open(myTempName).read()
        unlink(myTempName)

