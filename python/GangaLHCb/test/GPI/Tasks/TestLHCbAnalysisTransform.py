from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_state
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
          tr = GPI.LHCbAnalysisTransform(application=DaVinci(),backend=Dirac())
          t.appendTransform(tr)
          try:
               tr.update()
               assert false, 'Should have thrown exception if updated with no query'
          except:
               tr.query = GPI.BKTestQuery(stripping15down)
               tr.update()
             
               ## Check some new data added
               assert len(tr._impl.toProcess_dataset.files), 'No data added after call to update'

               try:
                    ## Shouldn't allow a second update before processed the data in toProcess_dataset
                    tr.update()
                    assert false, 'Should have thrown an error if updated with files already to process'
               except:
                    ## run so can update again with a removed dataset recall that jobs with the
                    ## old dataset only created when run called.
                    t.run()
                    assert len(tr.getPartitionJobs(0)), "No Jobs created upon run()"
                    job = GPI.jobs(int(tr.getPartitionJobs(0)[0].fqid.split('.')[0]))
                    sleep_until_state(job,300,'submitted')
                    del tr._impl.query.dataset.files[0]
                    tr.update(True)
                    
                    ## Check the dead dataset is picked up
                    assert len(tr._impl.removed_data.files), "Didn\'t Pick up loss of a dataset"
                    job.remove()
              
