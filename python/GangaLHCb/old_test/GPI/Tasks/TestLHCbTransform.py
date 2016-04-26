from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_state

# Setup bookeeping
stripping20up = '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagUp/Real Data/Reco14/Stripping20/90000000/DIMUON.DST'
stripping20down = '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagDown/Real Data/Reco14/Stripping20/90000000/DIMUON.DST'

import Ganga.Utility.Config.Config
doConfig = not Ganga.Utility.Config.Config._after_bootstrap

from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.GPI import BKTestQuery
bkQueryList = [BKTestQuery(stripping20up)]


class TestLHCbTransform(GangaGPITestCase):

    def test_overview(self):
        from Ganga.GPI import LHCbTransform
        tr = LHCbTransform(application=DaVinci(), backend=Local())
        tr.overview()

    def test_update(self):
        from Ganga.GPI import LHCbTask, LHCbTransform, jobs
        t = LHCbTask()
        tr = LHCbTransform(application=DaVinci(), backend=Dirac())
        t.appendTransform(tr)
        try:
            bkQueryList = [BKTestQuery(stripping20up)]
            tr.updateQuery()
            assert false, 'Should have thrown exception if updated with no query'
        except:
            tr.addQuery(BKTestQuery(stripping20down))

            # Check some new data added
            assert len(tr.inputdata), 'No data added after call to update'

            try:
                # Shouldn't allow a second update before processed the data in
                # toProcess_dataset
                tr.updateQuery()
                assert false, 'Should have thrown an error if updated with files already to process'
            except:
                # run so can update again with a removed dataset recall that jobs with the
                # old dataset only created when run called.
                t.run()
                assert len(tr.getJobs()), "No Jobs created upon run()"
                job = jobs(int(tr.getJobs()[0].fqid.split('.')[0]))
                sleep_until_state(job, 300, 'submitted')
                del tr._impl.query.dataset.files[0]
                tr.update(True)

                # Check the dead dataset is picked up
                assert len(
                    tr._impl.removed_data.files), "Didn\'t Pick up loss of a dataset"
                job.remove()
