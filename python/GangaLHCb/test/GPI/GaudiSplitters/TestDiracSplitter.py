################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestDiracSplitter.py,v 1.4 2008-11-13 10:08:37 jwilliam Exp $
################################################################################
from __future__ import division
from Ganga.GPIDev.Adapters.ISplitter import SplittingError
from GangaLHCb.test import addDiracTestSubmitter
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed, sleep_until_state
from GangaLHCb.Lib.Dirac.DiracSplitter import _diracSplitter

addDiracTestSubmitter()

class TestDiracSplitter(GangaGPITestCase):

    def testSplitWithMergeList(self):

        inputdata = MockDataset (
            cache_date = '' ,
            files = [ MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000923_5.dst' ,
            replicas = ['A', 'B','C'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000178_5.dst' ,
            replicas = ['A', 'B', 'C'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000819_5.dst' ,
            replicas = ['A', 'B', 'C', 'D', 'E'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000292_5.dst' ,
            replicas = ['A'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000471_5.dst' ,
            replicas = ['A'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000472_5.dst' ,
            replicas = ['A'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000895_5.dst' ,
            replicas = ['X'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000723_5.dst' ,
            replicas = ['Y'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000890_5.dst' ,
            replicas = ['A','B'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/PIC_ONLY.dst' ,
            replicas = ['D','E'] 
            ) , ] 
            )

        len_files = len(inputdata.files)
        ds = _diracSplitter(3,len_files,False)
        result = ds.split(inputdata, inputdata)
        assert len(result) == 5, 'Unexpected number of subjobs'


    def testSplitWithMissingReplicas(self):

        inputdata = MockDataset (
            cache_date = '' ,
            files = [ MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000923_5.dst' ,
            replicas = [] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000178_5.dst' ,
            replicas = [] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000819_5.dst' ,
            replicas = ['A', 'B', 'C', 'D', 'E'] 
            ) ] 
            )

        len_files = len(inputdata.files)

        ds = _diracSplitter(2,len_files,True)
        result = ds.split(inputdata, inputdata)
        assert len(result) == 2, 'Unexpected number of subjobs'

    def testSplitWithErrorReplicas(self):

        inputdata = MockDataset (
            cache_date = '' ,
            files = [ MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000923_5.dst' ,
            replicas = [] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000178_5.dst' ,
            replicas = [_diracSplitter.DIRAC_ERROR_MESSAGE] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000819_5.dst' ,
            replicas = ['A', 'B', 'C', 'D', 'E'] 
            ) ] 
            )

        len_files = len(inputdata.files)
        ds = _diracSplitter(2,len_files,True)
        result = ds.split(inputdata, inputdata)
        assert len(result) == 2, 'Unexpected number of subjobs'



    def testSavannah27430(self):

        inputdata = MockDataset (
            cache_date = '' ,
            files = [ MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000923_5.dst' ,
            replicas = ['CNAF-disk', 'PIC-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000178_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000819_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk', 'RAL-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000292_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk', 'RAL-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000471_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk', 'RAL-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000472_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk', 'RAL-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000895_5.dst' ,
            replicas = ['CNAF-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000723_5.dst' ,
            replicas = ['CNAF-disk', 'RAL-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000890_5.dst' ,
            replicas = ['CNAF-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/PIC_ONLY.dst' ,
            replicas = ['PIC-disk'] 
            ) , ] 
            )

        len_files = len(inputdata.files)
        ds = _diracSplitter(len_files,len_files,False)
        result = ds.split(inputdata, inputdata)
        assert len(result) == 2, 'One file can only run at PIC-disk.'
        
    def testSavannah27430MoreJobs(self):

        inputdata = MockDataset (
            cache_date = '' ,
            files = [ MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000923_5.dst' ,
            replicas = ['CNAF-disk', 'PIC-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000178_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000819_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk', 'RAL-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000292_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk', 'RAL-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000471_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk', 'RAL-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000472_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk', 'RAL-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000895_5.dst' ,
            replicas = ['CNAF-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000723_5.dst' ,
            replicas = ['CNAF-disk', 'RAL-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000890_5.dst' ,
            replicas = ['CNAF-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/PIC_ONLY.dst' ,
            replicas = ['PIC-disk'] 
            ) , ] 
            )

        len_files = len(inputdata.files)
        ds = _diracSplitter(2,len_files,False)
        result = ds.split(inputdata, inputdata)
        assert len(result) == 5, 'Should be split optimally'
        
    def testSavannah27430LessSites(self):
        
        inputdata = MockDataset (
            cache_date = '' ,
            files = [ MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000923_5.dst' ,
            replicas = ['OTHER'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000178_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000819_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk', 'RAL-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000292_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk', 'RAL-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000471_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk', 'RAL-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000472_5.dst' ,
            replicas = ['NIKHEF-disk', 'CNAF-disk', 'PIC-disk', 'RAL-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000895_5.dst' ,
            replicas = ['CNAF-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000723_5.dst' ,
            replicas = ['CNAF-disk', 'RAL-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000890_5.dst' ,
            replicas = ['CNAF-disk', 'IN2P3-disk'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/PIC_ONLY.dst' ,
            replicas = ['PIC-disk'] 
            ) , ] 
            )

        len_files = len(inputdata.files)
        ds = _diracSplitter(2,len_files,False)
        result = ds.split(inputdata, inputdata)
        assert len(result) == 6, 'Should be split optimally'


    def testRHallamDataSet(self):

        inputdata = MockDataset (
         cache_date = 'Mon Jul 16 19:35:33 2007' ,
         files = [ MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000001_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000002_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000003_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000004_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000005_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000006_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000007_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000008_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000009_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000010_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000011_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000012_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000013_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000014_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000015_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000016_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000017_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000018_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000019_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000020_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000021_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000022_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000023_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000024_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000025_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000026_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000027_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000028_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000029_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000030_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000031_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000032_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000033_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000034_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000035_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000036_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000037_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000038_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000039_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000040_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000041_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000042_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000043_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000044_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000045_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000046_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000047_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000048_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000049_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000050_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000051_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000052_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000053_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000054_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000055_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000056_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000057_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000058_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000059_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000060_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000061_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000062_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000063_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000064_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000065_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000066_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000067_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000068_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000069_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000070_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000071_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000072_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000073_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000074_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000075_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000076_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000077_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000078_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000079_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000080_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000081_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000082_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000083_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000084_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000085_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000086_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000087_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000088_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000089_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000090_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000091_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000092_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000093_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000094_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000095_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000096_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000097_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000098_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000099_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000100_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000101_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000102_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000103_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000104_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000105_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000106_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000107_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000108_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000109_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000110_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000111_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000112_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000113_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000114_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000115_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000116_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000117_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000118_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000119_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000120_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000121_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000122_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000123_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000124_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000125_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000126_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000128_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000129_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000130_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000131_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000132_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000133_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000134_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000135_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000136_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000137_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000138_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000139_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000140_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000141_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000142_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000143_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000144_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000145_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000146_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000147_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000148_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000149_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000150_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000151_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000152_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000153_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000154_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000155_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000156_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000157_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000158_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000159_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000160_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000161_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000162_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000163_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000164_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000165_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000166_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000167_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000168_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000169_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000170_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000171_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000172_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000173_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000174_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000175_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000176_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000177_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000178_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000179_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000180_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000181_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000182_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000183_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000184_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000185_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000186_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000187_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000188_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000189_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000190_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000191_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000192_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000193_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000194_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000195_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000196_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000197_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000198_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000199_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000200_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000201_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000202_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000203_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000204_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000205_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000206_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000207_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000208_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000209_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000210_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000211_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000212_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000213_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000214_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000215_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000216_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000217_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000218_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000219_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000220_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000221_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000222_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000224_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000225_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000226_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000227_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000228_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000229_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000230_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000231_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000232_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000233_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000234_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000235_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000236_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000237_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000238_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000240_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000241_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000242_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000243_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000244_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000245_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000246_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000247_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000248_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000249_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000250_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000251_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000252_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000253_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000254_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000255_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000256_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000257_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000258_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000259_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000260_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000261_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000262_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000263_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000264_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000265_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000266_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000267_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000268_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000269_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000270_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000271_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000272_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000273_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000274_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000275_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000276_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000277_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000278_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000279_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000280_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000281_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000282_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000283_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000284_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000285_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000286_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000287_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000288_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000289_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000290_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000291_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000292_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000293_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000294_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000295_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000296_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000297_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000298_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000299_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000300_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000302_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000304_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000305_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000306_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000307_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000308_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000309_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000310_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000311_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000312_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000313_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000314_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000315_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000316_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000317_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000318_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000319_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000320_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000321_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000322_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000323_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000324_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000325_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000326_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000327_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000328_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000329_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000330_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000331_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000332_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000333_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000334_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000335_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000336_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000337_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000338_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000339_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000340_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000341_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000342_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000343_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000344_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000345_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000346_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000347_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000348_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000349_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000350_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000351_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000352_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000353_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000354_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000355_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000356_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000357_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000358_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000359_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000360_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000361_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000362_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000363_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000364_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000365_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000366_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000367_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000368_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000369_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000370_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000371_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000372_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000373_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000374_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000375_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000376_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000377_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000378_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000379_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000380_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000381_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000382_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000383_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000384_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000385_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000386_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000387_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000388_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000389_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000390_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000391_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000392_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000393_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000394_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000395_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000396_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000397_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000398_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000399_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000400_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000401_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000402_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000403_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000404_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000405_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000406_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000407_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000408_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000409_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000410_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000411_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000412_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000413_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000414_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000415_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000416_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000417_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000418_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000419_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000420_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000421_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000422_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000423_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000424_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000425_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000426_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000427_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000428_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000429_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000430_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000431_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000432_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000433_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000434_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000435_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000436_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000437_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000438_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000439_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000440_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000441_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000442_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000443_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000444_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000445_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000446_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000447_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000448_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000449_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000450_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000451_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000452_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000453_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000454_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000455_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000456_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000457_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000458_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000459_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000460_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000461_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000462_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000463_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000464_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000465_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000466_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000467_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000468_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000469_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000470_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000471_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000472_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000473_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000474_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000475_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000476_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000477_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000478_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000479_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000480_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000481_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000482_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000483_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000484_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000486_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000487_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000488_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000489_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000490_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000491_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000492_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000493_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000494_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000495_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000496_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000497_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000498_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000499_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000500_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000501_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000502_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000503_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000504_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000505_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000506_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000507_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000508_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000509_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000510_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000511_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000512_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000513_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000514_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000515_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000516_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000517_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000518_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000519_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000520_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000521_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000522_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000523_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000524_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000525_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000526_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000527_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000528_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000529_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000530_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000531_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000532_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000533_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000534_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000535_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000536_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000537_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000538_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000539_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000540_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000541_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000542_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000543_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000544_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000545_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000546_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000547_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000548_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000549_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000550_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000551_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000552_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000553_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000554_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000555_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000556_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000557_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000558_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000559_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000560_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000561_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000562_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000563_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000564_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000565_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000566_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000567_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000569_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000570_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000571_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000574_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000575_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000576_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000577_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000578_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000579_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000580_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000581_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000582_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000583_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000584_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000585_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000586_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000587_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000588_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000589_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000590_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000591_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000592_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000593_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000594_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000595_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000596_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000597_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000598_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000599_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000600_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000601_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000602_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000603_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000604_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000605_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000606_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000608_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000609_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000610_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000611_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000612_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000613_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000614_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000615_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000616_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000617_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000618_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000619_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000620_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000621_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000622_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000623_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000624_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000625_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000626_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000627_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000628_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000629_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000630_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000631_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000632_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000633_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000634_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000635_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000636_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000637_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000638_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000639_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000640_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000641_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000642_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000643_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000644_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000645_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000646_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000647_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000648_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000649_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000650_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000651_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000652_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000653_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000654_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000655_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000656_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000657_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000658_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000659_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000660_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000661_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000662_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000663_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000664_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000665_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000666_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000667_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000668_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000669_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000670_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000671_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000672_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000673_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000674_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000675_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000676_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000677_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000678_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000679_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000680_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000681_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000682_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000683_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000684_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000685_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000686_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000687_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000688_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000689_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000690_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000691_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000692_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000693_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000694_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000695_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000696_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000697_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000698_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000699_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000700_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000701_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000702_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000703_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000704_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000705_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000706_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000707_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000708_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000709_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000710_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000711_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000712_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000713_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000714_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000715_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000716_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000717_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000718_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000719_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000720_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000721_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000722_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000723_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000724_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000725_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000726_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000727_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000728_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000729_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000730_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000731_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000732_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000733_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000734_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000735_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000736_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000737_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000738_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000739_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000740_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000741_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000742_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000743_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000744_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000745_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000746_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000747_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000748_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000749_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000750_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000751_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000752_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000753_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000754_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000755_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000756_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000757_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000758_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000759_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000760_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000761_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000762_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000763_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000764_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000765_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000766_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000767_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000768_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000769_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000770_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000771_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000772_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000773_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000774_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000775_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000776_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000777_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000778_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000779_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000780_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000781_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000782_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000783_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000784_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000785_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000786_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000787_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000788_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000789_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000790_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000791_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000792_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000793_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000794_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000795_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000796_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000797_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000798_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000799_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000800_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000801_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000802_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000803_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000804_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000805_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000806_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000807_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000808_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000809_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000810_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000811_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000812_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000813_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000814_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000815_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000816_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000817_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000818_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000819_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000820_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000821_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000822_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000823_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000824_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000825_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000826_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000827_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000828_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000829_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000830_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000831_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000832_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000833_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000834_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000835_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000836_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000837_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000838_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000839_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000840_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000841_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000842_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000843_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000844_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000845_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000846_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000847_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000848_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000851_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000852_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000854_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000855_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000856_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000857_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000858_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000859_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000860_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000861_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000862_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000863_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000864_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000865_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000866_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000867_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000868_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000869_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000870_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000871_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000872_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000873_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000874_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000875_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000876_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000877_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000878_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000879_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000880_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000881_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000882_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000883_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000884_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000885_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000886_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000887_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000888_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000889_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000890_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000891_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000892_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000893_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000894_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000895_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000896_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000897_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000898_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000899_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000900_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000901_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000902_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000903_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000904_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000905_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000906_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000907_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000908_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000909_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000910_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000911_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000912_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000913_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000914_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000915_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000916_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000917_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000918_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000919_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000920_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000921_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000922_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000923_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000924_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000925_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000926_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000927_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000928_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000929_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000930_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000931_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000932_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000933_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000934_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000935_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000936_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000937_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000938_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000939_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000940_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000941_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000942_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000943_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000944_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000945_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000946_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000947_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000948_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000949_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000950_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000951_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000952_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000953_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000954_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000955_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000956_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000957_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000958_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000959_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000960_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000961_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000962_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000963_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000964_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000965_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000966_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000967_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000968_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000969_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000970_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000971_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000972_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000973_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000974_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000975_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000976_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000977_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000978_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000979_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000980_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000981_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000982_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000983_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000984_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000985_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000986_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000987_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000988_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000989_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000990_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000991_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000992_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000993_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000994_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000995_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000996_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000997_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000998_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00000999_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , MockDataFile (
            name = 'LFN:/lhcb/production/DC04/v2r3/00001257/DST/00001257_00001000_5.dst' ,
            replicas = ['CERN-tape'] 
            ) , ] 
         ) 

        
        ds = _diracSplitter(10,50,False)
        try:
            ds.split(inputdata, inputdata)
        except SplittingError:
            assert False, 'No Error should be thrown'

    def testSplittingWithLFC(self): 
    
            data = load('lfn_data.py',True)[0]
            dataLen = len(data)
            data.updateReplicaCache(forceUpdate = True)
            assert data.cache_date != '', 'Cache should have been updated'
            
            for f in data.files:
                assert f.replicas, 'Test needs files correctly resolved in LFC'
            
            j = Job(backend = Dirac(), application = DaVinci())
            j.backend.CPUTime = 600
            j.inputdata = data
            j.splitter = DiracSplitter(filesPerJob = 20)
            
            j.submit()
            assert j.subjobs, 'Splitter must have created subjobs'
            assert len(j.inputdata) == dataLen, 'Must keep data in sane way'
            for jj in j.subjobs:
                assert jj.inputdata, 'Data must have been propagated'

            sleep_until_state(j,state='submitted')
                
            j2 = j.copy()
            assert len(j2.inputdata) == dataLen, 'Copy should be full'
            for f in j2.inputdata.files:
                assert f.replicas, 'Replicas must have copied'
            for jj in j2.subjobs:
                assert not jj.inputdata, 'Data must have not been propagated'
            j2.submit()
            sleep_until_state(j2,state='submitted')
            assert j.inputdata.cache_date == j2.inputdata.cache_date, 'cache dates should be the same'
            
            j.kill()
            j2.kill()
            
            
            
            
            

