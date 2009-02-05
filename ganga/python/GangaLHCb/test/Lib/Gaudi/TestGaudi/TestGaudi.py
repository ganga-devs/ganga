import os
import shutil
import tempfile
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Gaudi.Gaudi import GaudiExtras
from GangaLHCb.Lib.Gaudi.Gaudi import Gaudi
from GangaLHCb.Lib.Gaudi.GaudiUtils import available_apps
from GangaTest.Framework.utils import read_file,failureException
import Ganga.Utility.Config
import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()

class TestGaudi(GangaGPITestCase):

    def setUp(self):
        self.config=Ganga.Utility.Config.getConfig('LHCb')

    def test_Gaudi__auto__init__(self):
        dv = DaVinci()
        assert dv._impl.version, 'version not set automatically'
        assert dv._impl.platform, 'platform not set automatically'

    def test_Gaudi_master_configure(self):
        job = Job(application=Gauss(optsfile='./Gauss.opts'))
        gauss = job.application
        job.inputdata = ['dummy1.in','dummy2.in']
        job.outputdata = ['Gauss.sim']
        job.outputsandbox = ['GaussHistos.root','GaussMonitor.root']
        inputs,extra = gauss._impl.master_configure()
        assert extra.inputdata == job.inputdata._impl, 'input data error'
        assert extra.dataopts.rfind('dummy1.in') >= 0 and \
               extra.dataopts.rfind('dummy2.in') >= 0, 'dataopts string error'
        # provide basic test of where output goes - a more complete test is
        # run on the PythonOptionsParser methods.
        ok = extra.outputsandbox.count('GaussHistos.root') > 0 and \
             extra.outputsandbox.count('GaussMonitor.root') > 0
        assert ok, 'outputsandbox error'
        assert extra.outputdata.count('Gauss.sim') > 0, 'outputdata error'
        assert extra._LocalSite == self.config['LocalSite'], 'site error'
        assert extra._SEProtocol == self.config['SEProtocol'], 'protocol error'

    # this method currently does nothing
    #def test_Gaudi_configure(self):

    # not much to check here...as this method simply runs checks itself
    #def test_Gaudi__check_inputs(self):

    # test these 3 methods together
    def test_Gaudi_get_pack_AND_make_AND_cmt(self):
        #apps = available_apps() # takes too long to do them all
        apps = ['DaVinci']
        cmtuserpath = tempfile.mktemp()
        os.mkdir(cmtuserpath)
        for appname in apps:
            logger.info('processing %s' % appname)
            app = eval('%s()' % appname)            
            app.user_release_area = cmtuserpath
            package = 'Phys/%s %s' % (appname,app.version)
            # Test getpack
            logger.info('getpack...')
            app.getpack(package)
            f = os.path.join(cmtuserpath,'%s_%s' % (appname, app.version),
                             'Phys/%s' % appname,'cmt/requirements')
            assert os.path.exists(f), '%s must exist' % f
            read_file(f)            
            # Test make
            logger.info('make...')
            app.make()
            dir = os.path.join(cmtuserpath,'%s_%s' % (appname, app.version),
                               'Phys/%s' % appname,app._impl.platform)
            assert os.path.exists(dir), '%s must exist' % dir
            f = os.path.join(dir,'%ssetup.make' % appname)
            read_file(f)
            # Test CMT command
            logger.info('cmt...')
            shutil.rmtree(dir)
            app.cmt('br make')
            read_file(f)
            logger.info('done w/ this app.')
