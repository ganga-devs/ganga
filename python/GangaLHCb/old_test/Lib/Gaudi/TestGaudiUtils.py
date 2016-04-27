import inspect
from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.Core import ApplicationConfigurationError

try:
    import Ganga.Utility.Config.Config
    doConfig = not Ganga.Utility.Config.Config._after_bootstrap
except x:
    print(x)
    doConfig = True

if doConfig:
    from GangaLHCb.Lib.Applications.AppsBaseUtils import *


class TestGaudiUtils(GangaGPITestCase):

    """Tests methods defined in GangaLHCb/Lib/Gaudi/GaudiUtils.py"""

    def setUp(self):
        from GangaLHCb.Lib.Applications.AppsBaseUtils import available_apps
        self.apps = available_apps()
        # self.apps.remove('Gaudi')
        srcdir = os.path.dirname(inspect.getsourcefile(GaudiPython))
        self.optsfile = [File(os.path.join(srcdir,
                                           'options/GaudiPythonExample.py'))]

    def test_available_apps(self):
        assert type(self.apps) == type([]), 'available_apps must return a list'
        assert len(self.apps) > 0, 'no apps found'

    ## FIXME needsw to be updated
    def test_available_packs(self):
        for app in self.apps:
            try:
                from GangaLHCb.Lib.Applications.AppsBaseUtils import available_packs
                available_packs(app)
            except KeyError as err:
                assert False, 'application %s has no packages' % app

    ## FIXME needs to be updated
    def test_available_versions(self):
        for app in self.apps:
            from GangaLHCb.Lib.Applications.AppsBaseUtils import available_versions
            versions = available_versions(app)
            assert len(versions) > 0, 'application %s has no versions' % app

    ## FIXME needs to be updated
    def test_guess_version(self):
        for app in self.apps:
            from GangaLHCb.Lib.Applications.AppsBaseUtils import guess_version
            version = guess_version(app)
            assert len(version) > 0, 'couldn''t guess version for %s' % app

# def test_get_user_platform(self):
##         env = {'CMTCONFIG' : 'DUMMY'}
##         platform = get_user_platform(env)
##         assert platform != '', 'platform should be determned w/ CMTCONFIG set'
##         env = {}
# just make sure it doesn't die
##         platform = get_user_platform(env)

# def test_update_project_path(self):
##         env = {'CMTPROJECTPATH' : 'DUMMY'}
# update_project_path('SOMETHING',env)
##         assert env['CMTPROJECTPATH'] == 'SOMETHING:DUMMY'

# def test_get_user_dlls(self):
# FIXME: This only really tests coverage. A more involved test would
# check if the files that should be found are found.
##         dv = DaVinci()
# dv._impl._getshell()
# get_user_dlls('DaVinci',dv.version,dv.user_release_area,
# dv.platform,dv._impl.shell)
