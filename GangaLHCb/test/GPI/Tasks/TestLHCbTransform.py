from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_state
from Ganga import GPI
#import unittest

#Setup bookeeping
stripping20up = '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagUp/Real Data/Reco14/Stripping20/90000000/DIMUON.DST'
stripping20down = '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagDown/Real Data/Reco14/Stripping20/90000000/DIMUON.DST'
bkQueryList = [GPI.BKTestQuery(stripping20up)]


class TestLHCbTransform(GangaGPITestCase):
     def test_overview(self):
          tr = GPI.LHCbTransform(application=DaVinci(),backend=Local())
          tr.overview()

     def test_update(self):
          t = GPI.LHCbTask()
          tr = GPI.LHCbTransform(application=DaVinci(),backend=Dirac())
          t.appendTransform(tr)
          try:
               tr.updateQuery()
               assert false, 'Should have thrown exception if updated with no query'
          except:
               tr.addQuery(GPI.BKTestQuery(stripping20down))
             
               ## Check some new data added
               assert len(tr.inputdata), 'No data added after call to update'

               try:
                    ## Shouldn't allow a second update before processed the data in toProcess_dataset
                    tr.updateQuery()
                    assert false, 'Should have thrown an error if updated with files already to process'
               except:
                    ## run so can update again with a removed dataset recall that jobs with the
                    ## old dataset only created when run called.
                    t.run()
                    assert len(tr.getJobs()), "No Jobs created upon run()"
                    job = GPI.jobs(int(tr.getJobs()[0].fqid.split('.')[0]))
                    sleep_until_state(job,300,'submitted')
                    del tr._impl.query.dataset.files[0]
                    tr.update(True)
                    
                    ## Check the dead dataset is picked up
                    assert len(tr._impl.removed_data.files), "Didn\'t Pick up loss of a dataset"
                    job.remove()
              
