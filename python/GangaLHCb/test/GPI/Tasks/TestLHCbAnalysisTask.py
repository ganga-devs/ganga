from GangaTest.Framework.tests import GangaGPITestCase
from Ganga import GPI
#import unittest

#Setup bookeeping
stripping15up = '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagUp/Real Data/Reco11/Stripping15/90000000/DIMUON.DST'
stripping15down = '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real Data/Reco11/Stripping15/90000000/DIMUON.DST'
stripping16up = '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagUp/Real Data/Reco11a/Stripping16/90000000/DIMUON.DST'
stripping16down = '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real Data/Reco11a/Stripping16/90000000/DIMUON.DST'
bkQueryList = [GPI.BKTestQuery(stripping15down), GPI.BKTestQuery(stripping16up), GPI.BKTestQuery(stripping16down)]


class TestLHCbAnalysisTask(GangaGPITestCase):
     def test_addQuery(self):
          tr = GPI.LHCbAnalysisTransform(application=DaVinci(),backend=Local())
          t = GPI.LHCbAnalysisTask()
          
          ## Check non-lists and adding query to transform and non-associated
          t.addQuery(tr,GPI.BKTestQuery(stripping15up))
          assert len(t.transforms),'Transform not associated correctly'
          assert t.transforms[0].query.path == stripping15up,'Query path not correctly assigned' 
          
          ## Check duplicating
          t.addQuery(tr,bkQueryList)
          assert len(t.transforms)==4,'Problem duplicating and appending transforms'
          tmpList = [stripping15up,stripping15down,stripping16up,stripping16down]
          for tran in t.transforms:
               assert tran.query.path in tmpList, 'Query attribute not setup properly for all transforms'
         
     def test_appendTransform(self):
          tr1 = GPI.LHCbAnalysisTransform(application=DaVinci(),backend=Local())
          t = GPI.LHCbAnalysisTask()
          
          ## Try appending
          t.appendTransform(tr1)
          assert len(t.transforms),'Didn\'t append a transform properly'

          ## Try appending a transform with a query and check for update
          tr2 = GPI.LHCbAnalysisTransform(application=DaVinci(),backend=Local())
          tr2.query = GPI.BKTestQuery(stripping15up)
          t.appendTransform(tr2)
          assert len(t.transforms[-1]._impl.toProcess_dataset.files),'Transform not updated properly after appending'
         
     def test_help(self):
          t = GPI.LHCbAnalysisTask()
          t.help()

     def test_overview(self):
          t = GPI.LHCbAnalysisTask()
          t.overview()  

     def test_update(self):
          t = GPI.LHCbAnalysisTask()
          tr1 = GPI.LHCbAnalysisTransform(application=DaVinci(),backend=Local())
          tr2 = GPI.LHCbAnalysisTransform(application=DaVinci(),backend=Local())
          t.appendTransform(tr1)
          t.appendTransform(tr2)
          tr1.query = GPI.BKTestQuery(stripping15up)
          tr2.query = GPI.BKTestQuery(stripping15down)
          
          ## Check that update produces some files to process over multiple transforms
          t.update()
          assert len(t.transforms[0]._impl.toProcess_dataset.files),'Update did not produce any datafiles to process in transform 0'
          assert len(t.transforms[1]._impl.toProcess_dataset.files),'Update did not produce any datafiles to process in transform 1'
