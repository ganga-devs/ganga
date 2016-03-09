import os
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed

import Ganga.Utility.Config.Config

from GangaLHCb.test import getDiracAppPlatform

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()


class TestDaVinci(GangaGPITestCase):

    def setUp(self):
        ganga_path = os.path.abspath(os.path.dirname(__file__))
        self.path = ganga_path + '/../python/GangaLHCb/old_test/GPI/DaVinci/'
        self.input_data = [
            'LFN:/lhcb/LHCb/Collision10/DIMUON.DST/00010942/0000/00010942_00000218_1.dimuon.dst']
        #self.input_data = ['LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000326_1.dimuon.dst']

    def test_current_opts(self):
        dv = DaVinci(version='v35r1')
        # until this is updated, just use the default options
        logger.info("Checking Options: %s" % (self.path + 'opts.current.py'))
        dv.optsfile = [self.path + 'opts.current.py']
        #dv.extraopts = 'from Configurables import DaVinci\nDaVinci.HistFile="DVHistos_1.root"\nDaVinci().EvtMax = 100\nDaVinci().DataType =\'2010\''
        j = Job(application=dv)
        j.backend = Dirac()
        #outfile_name = 'Presel_test.dst'
        outfile_name = 'DVHistos_1.root'
        j.outputfiles = [LocalFile(outfile_name)]
        j.inputdata = [DiracFile(
            lfn="/lhcb/LHCb/Collision10/DIMUON.DST/00010942/0000/00010942_00000218_1.dimuon.dst")]
        j.submit()
        sleep_until_completed(j, 1800)
        # print(j.status)
        assert j.status == "completed"
        # j.peek()
        assert(os.path.exists(os.path.join(j.outputdir, outfile_name)))

    def test_old_opts(self):
        dv = DaVinci()
        dv.version = 'v19r7'  # force to use old gaudirun.py call
        #dv.optsfile = [self.path + 'opts.old.py']
        j = Job(application=dv)
        #j.outputsandbox = ['DVHistos.root']
        j.inputdata = self.input_data
        got_err = False
        try:
            j.submit()
        except JobError:
            got_err = True
        assert got_err, 'an error should have been raised'
        #assert sleep_until_completed(j,1800)
        # j.peek()
        # assert(os.path.exists(os.path.join(j.outputdir,'DVHistos.root')))

    def test_outputfiles_submit(self):
        j = Job(application=DaVinci(), backend=Dirac())
        j.application.platform = getDiracAppPlatform()
        j.outputfiles = [LocalFile('Something.root')]
        j.submit()
        j.kill()

    def test_cmt_exitcodes(self):
        dv = DaVinci()
        assert dv.cmt(
            'version') == 0, 'CMT command that should succeed, report non-zero exit code.'
        assert dv.cmt(
            'ThisIsNotValid') != 0, 'CMT command that should fail, report zero exit code.'
