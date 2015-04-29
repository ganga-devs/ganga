
from GangaTest.Framework.tests import GangaGPITestCase

try:
    import Ganga.Utility.Config.Config
    doConfig = not Ganga.Utility.Config.Config._after_bootstrap
except x:
    print x
    doConfig = True

#Setup bookeeping
stripping15up = '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagUp/Real Data/Reco11/Stripping15/90000000/DIMUON.DST'
stripping15down = '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real Data/Reco11/Stripping15/90000000/DIMUON.DST'
stripping16up = '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagUp/Real Data/Reco11a/Stripping16/90000000/DIMUON.DST'
stripping16down = '/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real Data/Reco11a/Stripping16/90000000/DIMUON.DST'

if doConfig:
    from Ganga import GPI
    bkQueryList = [GPI.BKTestQuery(stripping15down), GPI.BKTestQuery(stripping16up), GPI.BKTestQuery(stripping16down)]


class TestLHCbTask(GangaGPITestCase):
     def test_addQuery(self):
          from Ganga import GPI
          tr = GPI.LHCbTransform(application=DaVinci(),backend=Local())
          t = GPI.LHCbTask()
          
          ## Check non-lists and adding query to transform and non-associated
          t.addQuery(tr,GPI.BKTestQuery(stripping15up))
          assert len(t.transforms),'Transform not associated correctly'
          assert t.transforms[0].queries[0].path == stripping15up,'Query path not correctly assigned' 
          
          ## Check duplicating
          t.addQuery(tr,bkQueryList)
          assert len(t.transforms)==4,'Problem duplicating and appending transforms'
          tmpList = [stripping15up,stripping15down,stripping16up,stripping16down]
          for tran in t.transforms:
               assert tran.queries[0].path in tmpList, 'Query attribute not setup properly for all transforms'
         
     def test_appendTransform(self):
          from Ganga import GPI
          tr1 = GPI.LHCbTransform(application=DaVinci(),backend=Local())
          t = GPI.LHCbTask()
          
          ## Try appending
          t.appendTransform(tr1)
          assert len(t.transforms),'Didn\'t append a transform properly'

          ## Try appending a transform with a query and check for update
          tr2 = GPI.LHCbTransform(application=DaVinci(),backend=Local())
          tr2.addQuery(GPI.BKTestQuery(stripping15up))
          t.appendTransform(tr2)
          assert len(t.transforms[-1]._impl.toProcess_dataset.files),'Transform not updated properly after appending'
         
     def test_overview(self):
          from Ganga import GPI
          t = GPI.LHCbTask()
          t.overview()  

     def test_update(self):
          from Ganga import GPI
          t = GPI.LHCbTask()
          tr1 = GPI.LHCbTransform(application=DaVinci(),backend=Local())
          tr2 = GPI.LHCbTransform(application=DaVinci(),backend=Local())
          t.appendTransform(tr1)
          t.appendTransform(tr2)
          tr1.addQuery(GPI.BKTestQuery(stripping15up))
          tr2.addQuery(GPI.BKTestQuery(stripping15down))
          
          ## Check that update produces some files to process over multiple transforms
          t.update()
          assert len(t.transforms[0]._impl.toProcess_dataset.files),'Update did not produce any datafiles to process in transform 0'
          assert len(t.transforms[1]._impl.toProcess_dataset.files),'Update did not produce any datafiles to process in transform 1'

