from GangaTest.Framework.tests import GangaGPITestCase
from Ganga import GPI
#import unittest

#Setup bookeeping
stripping15up = '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagUp/Real Data/Reco11/Stripping15/90000000/DIMUON.DST'
stripping15down = '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real Data/Reco11/Stripping15/90000000/DIMUON.DST'
stripping16up = '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagUp/Real Data/Reco11a/Stripping16/90000000/DIMUON.DST'
stripping16down = '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real Data/Reco11a/Stripping16/90000000/DIMUON.DST'
bkQueryList = [GPI.BKTestQuery(stripping15down), GPI.BKTestQuery(stripping16up), GPI.BKTestQuery(stripping16down)]


class TestLHCbAnalysisTransform(GangaGPITestCase):
     def test_overview(self):
         tr = GPI.LHCbAnalysisTransform(application=DaVinci(),backend=Local())
         tr.overview()

     def test_update(self):
         t = GPI.LHCbAnalysisTask()
         tr = GPI.LHCbAnalysisTransform(application=DaVinci(),backend=Local())
         t.appendTransform(tr)
         try:
             tr.update()
             assert false, 'Should have thrown exception if updated with no query'
         except:
             tr.query = GPI.BKTestQuery(stripping15down)
             tr.update()
             
             ## Check some new data added
             assert len(tr._impl.toProcess_dataset.files), 'No data added after call to update'
             
             ## remove toProcess dataset so can update again with a removed dataset
             tr._impl.toProcess_dataset.files=[]
             del tr._impl.query.dataset.files[0]
             tr.update(True)
             
             ## Check the dead dataset is picked up
             assert len(tr._impl.removed_data.files), "Didn\'t Pick up loss of a dataset"
             
             try:
                 ## Shouldn't allow a second update before processed the data in toProcess_dataset
                 tr.update()
                 assert false, 'Should have thrown an error if updated with files already to process'
             except:
                 pass
              
