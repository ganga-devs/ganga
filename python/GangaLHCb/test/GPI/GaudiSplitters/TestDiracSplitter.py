################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestDiracSplitter.py,v 1.7 2009-06-16 12:03:39 jwilliam Exp $
################################################################################
from __future__ import division
from Ganga.GPIDev.Adapters.ISplitter import SplittingError
from GangaLHCb.test import addDiracTestSubmitter
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed, sleep_until_state

addDiracTestSubmitter()

class TestDiracSplitter(GangaGPITestCase):

    def testSplit(self):

        inputdata = LHCbDataset(files=[
            'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000923_5.dst',
            'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000178_5.dst',
            'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000819_5.dst',
            'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000292_5.dst',
            'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000471_5.dst',
            'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000472_5.dst'
            ])

        len_files = len(inputdata.files)
        ds = DiracSplitter()
        ds.filesPerJob = 2        
        result = ds._impl._splitFiles(inputdata)
        assert len(result) >= 3, 'Unexpected number of subjobs'

    def testIgnoreMissing(self):
        
        inputdata = LHCbDataset(files=[
            'LFN:/not/a/file.dst',
            'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000923_5.dst',
            'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000178_5.dst',
            'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000819_5.dst',
            'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000292_5.dst',
            'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000471_5.dst',
            'LFN:/lhcb/production/DC06/phys-lumi2/00001586/DST/0000/00001586_00000472_5.dst'
            ])

        ds = DiracSplitter()
        ds.ignoremissing = True
        # shouldn't throw exception
        result = ds._impl._splitFiles(inputdata)
        print 'result = ', result
        ds.ignoremissing = False
        # should throw exception
        threw = False
        try:
            result = ds._impl._splitFiles(inputdata)
            print 'result = ', result
        except:
            threw = True
        assert threw, 'should have thrown exception'

    
        
