import os
import shutil
import tempfile
from GangaTest.Framework.tests import GangaGPITestCase
#from GangaGaudi.Lib.Applications.GaudiBase import Francesc
from GangaTest.Framework.utils import read_file, failureException
import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()

try:
    import Ganga.Utility.Config.Config
    doConfig = not Ganga.Utility.Config.Config._after_bootstrap
except x:
    print(x)
    doConfig = True

if doConfig:
    from GangaLHCb.Lib.Applications.AppsBaseUtils import available_apps
    from GangaLHCb.Lib.Applications.GaudiPython import GaudiPython
    from GangaLHCb.Lib.Applications.Bender import Bender


class TestFrancesc(GangaGPITestCase):

    def setUp(self):
        pass

# def test_Francesc_get_gaudi_appname(self):
##         g = Gaudi()
##         assert g._impl.get_gaudi_appname() == None
##         gp = GaudiPython(project='DaVinci')
##         assert gp._impl.get_gaudi_appname() == 'DaVinci'
##         dv = DaVinci()
##         assert dv._impl.get_gaudi_appname() == 'DaVinci'
##         b = Bender()
##         assert b._impl.get_gaudi_appname() == 'Bender'

    # test this fully in Gaudi and GaudiPython tests
    # def test_Francesc__init(self):

    # in principle we could test that this catches everything
    # def test_Francesc__check_gaudi_inputs(self):

    def test_Francesc__getshell(self):
        # just check coverage...hard to check if the env is set properly
        from GangaLHCb.Lib.Applications.AppsBaseUtils import available_apps
        apps = available_apps()
        # apps.remove('Gaudi')
        for app in apps:
            instance = eval('%s()' % app)
            # if app == 'Gauss':
            #     instance._impl.platform = 'x86_64-slc5-gcc43-opt'
            instance._impl._getshell()

    # test these 3 methods together
    def test_Francesc_get_pack_AND_make_AND_cmt(self):
        # apps = available_apps() # takes too long to do them all
        apps = []  # apps = ['DaVinci']
        cmtuserpath = tempfile.mktemp()
        os.mkdir(cmtuserpath)
        for appname in apps:
            logger.info('processing %s' % appname)
            app = eval('%s()' % appname)
            app.user_release_area = cmtuserpath
            package = 'Phys/%s %s' % (appname, app.version)
            # Test getpack
            logger.info('getpack...')
            app.getpack(package)
            f = os.path.join(cmtuserpath, '%s_%s' % (appname, app.version),
                             'Phys/%s' % appname, 'cmt/requirements')
            assert os.path.exists(f), '%s must exist' % f
            read_file(f)
            # Test make
            logger.info('make...')
            app.make()
            tempDir = os.path.join(cmtuserpath, '%s_%s' % (appname, app.version),
                               'Phys/%s' % appname, app._impl.platform)
            assert os.path.exists(tempDir), '%s must exist' % tempDir
            f = os.path.join(tempDir, '%ssetup.make' % appname)
            read_file(f)
            # Test CMT command
            logger.info('cmt...')
            shutil.rmtree(tempDir)
            app.cmt('br make')
            read_file(f)
            logger.info('done w/ this app.')

    def test_Francesc__master_configure(self):
        pass
##         job = Job(application=DaVinci())
##         dv = job.application
##         job.inputdata = ['pfn:dummy1.in','pfn:dummy2.in']
# dv._impl._master_configure()
##         assert dv._impl.extra.inputdata == job.inputdata._impl
        # a more complete test requires a properly set up user release area

    def test_Francesc__configure(self):
        pass
##         job = Job(application=DaVinci())
# dv = job.application
##         job.inputdata = ['pfn:dummy1.in','pfn:dummy2.in']
# dv._impl._master_configure()
# dv._impl._configure()
##         data_path = os.path.join(job.getInputWorkspace().getPath(),'data.py')
##         assert os.path.isfile(data_path)
# f=file(data_path,'r')
##         buffer = f.read()
# f.close()
# assert buffer.rfind('dummy1.in') >= 0 and \
##                buffer.rfind('dummy2.in') >= 0
