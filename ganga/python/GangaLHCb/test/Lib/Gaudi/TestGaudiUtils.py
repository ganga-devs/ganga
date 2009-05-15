import inspect
from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.Core import ApplicationConfigurationError
from GangaLHCb.Lib.Gaudi.GaudiUtils import *

class TestGaudiUtils(GangaGPITestCase):
    """Tests methods defined in GangaLHCb/Lib/Gaudi/GaudiUtils.py"""

    def setUp(self):
        self.apps = available_apps()
        self.apps.remove('Gaudi')
        srcdir = os.path.dirname(inspect.getsourcefile(GaudiPython))
        self.optsfile = [File(os.path.join(srcdir,
                                           'options/GaudiPythonExample.py'))]

    def test_available_apps(self):
        assert type(self.apps) == type([]), 'available_apps must return a list'
        assert len(self.apps) > 0, 'no apps found'

    def test_available_packs(self):
        for app in self.apps:
            try:
                available_packs(app)
            except KeyError, err:                
                assert False, 'application %s has no packages' % app

    def test_available_versions(self):
        for app in self.apps:
            versions = available_versions(app)
            assert len(versions) > 0, 'application %s has no versions' % app

    def test_guess_version(self):
        for app in self.apps:
            version = guess_version(app)
            assert len(version) > 0, 'couldn''t guess version for %s' % app
            
    def test_get_user_platform(self):
        env = {'CMTCONFIG' : 'DUMMY'}
        platform = get_user_platform(env)
        assert platform != '', 'platform should be determned w/ CMTCONFIG set'
        env = {}
        # just make sure it doesn't die
        platform = get_user_platform(env)

    def test_update_cmtproject_path(self):
        env = {'CMTPROJECTPATH' : 'DUMMY'}
        update_cmtproject_path('SOMETHING',env)
        assert env['CMTPROJECTPATH'] == 'SOMETHING:DUMMY'

    def test_get_user_dlls(self):
        # FIXME: This only really tests coverage. A more involved test would
        # check if the files that should be found are found.
        dv = DaVinci() 
        dv._impl._getshell()
        get_user_dlls('DaVinci',dv.version,dv.user_release_area,
                      dv.platform,dv._impl.shell)

