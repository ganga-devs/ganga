import os
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed

class TestDaVinci(GangaGPITestCase):

    def setUp(self):
        ganga_path = os.path.abspath(os.path.dirname(__file__))
        self.path = ganga_path + '/../python/GangaLHCb/test/GPI/DaVinci/'
        self.input_data = ['LFN:/lhcb/MC/MC09/DST/00004831/0000/00004831_00000008_1.dst']
        #self.input_data = ["LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000001_5.dst",
        #       "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000002_5.dst",
        #       "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000003_5.dst",
        #       "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000004_5.dst"]
        

    def test_current_opts(self):
        dv = DaVinci()
        # until this is updated, just use the default options
        #dv.optsfile = [self.path + 'opts.current.py']
        #dv.extraopts = 'DaVinci().EvtMax = 100\nDaVinci().DataType =\'DC06\''
        j = Job(application=dv)
        #outfile_name = 'Presel_test.dst'
        outfile_name = 'DVHistos_1.root'
        j.outputsandbox = [outfile_name]        
        j.inputdata = self.input_data
        j.submit()
        assert sleep_until_completed(j,1800)
        j.peek()
        assert(os.path.exists(os.path.join(j.outputdir,outfile_name)))

    def test_old_opts(self):
        dv = DaVinci()
        dv.version = 'v19r7' # force to use old gaudirun.py call
        #dv.optsfile = [self.path + 'opts.old.py']
        j = Job(application=dv)
        #j.outputsandbox = ['DVHistos.root']        
        j.inputdata = self.input_data
        got_err = False
        try:
            j.submit()
        except JobError:
            got_err = True
        assert got_err, 'an error should have been raise'
        #assert sleep_until_completed(j,1800)
        #j.peek()
        #assert(os.path.exists(os.path.join(j.outputdir,'DVHistos.root')))

    def test_outputdata_submit(self):
        j = Job(application=DaVinci(),backend=Dirac())
        j.application.platform = config.DIRAC.AllowedPlatforms[0]
        j.outputdata = ['Something.root']
        j.submit()
        j.kill()
