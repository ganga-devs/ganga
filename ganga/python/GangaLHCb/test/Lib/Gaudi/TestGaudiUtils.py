import inspect
from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.Core import ApplicationConfigurationError
from GangaLHCb.Lib.Gaudi.GaudiUtils import *

class TestGaudiUtils(GangaGPITestCase):
    """Tests methods defined in GangaLHCb/Lib/Gaudi/GaudiUtils.py"""

    def setUp(self):
        self.apps = available_apps()
        self.apps.remove('Gaudi')
        self.apps.remove('Panoramix')
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
    
    def test_check_gaudi_inputs(self):
        for app in self.apps:
            try:
                check_gaudi_inputs(self.optsfile,app)
            except ApplicationConfigurationError, err:
                assert False, err
        
    def test_gaudishell_setenv(self):
        # just check coverage...hard to check if the env is set properly
        for app in self.apps:
            instance = eval('%s()' % app)
            shell = gaudishell_setenv(instance._impl)

    def test_collect_lhcb_filelist(self):
        l = collect_lhcb_filelist(['file1','file2','file3'])
        assert len(l) == 3, 'collect incorrect number of files from list'
        ds = LHCbDataset(files=['file1','file2','file3'])
        d = collect_lhcb_filelist(ds)
        good = (len(d) == len(ds.files))
        assert good, 'collect incorrect number of files from dataset'

    def test_jobid_as_string(self):
        j = Job(application=DaVinci())
        ok = jobid_as_string(j).rfind(str(j.id)) >= 0
        assert ok, 'job id string should contain the job id number'
        j.inputdata = ['a','b']
        j.splitter = SplitByFiles(filesPerJob=1)
        j.submit()
        jid = jobid_as_string(j.subjobs[0])
        ok = jid.rfind(str(j.id)) >= 0
        assert ok, 'subjob id string should contain master id number'
        ok = jid[len(jid)-1] == '0'
        assert ok, 'subjob id string should end w/ subjob id number'

    def test_dataset_to_options_string(self):
        s = dataset_to_options_string(None)
        assert s == '', 'None should return an empty string'
        s = dataset_to_options_string(LHCbDataset(['a','b','c']))
        assert s != '', 'dataset should not return an empty string'

    def test_get_user_platform(self):
        env = {'CMTCONFIG' : 'DUMMY'}
        platform = get_user_platform(env)
        assert platform != '', 'platform should be determned w/ CMTCONFIG set'
        env = {}
        # just make sure it doesn't die
        platform = get_user_platform(env)

    def test_create_lsf_runscript(self):
        # just get full coverage - the only way to test functionality is to
        # run full jobs...which should be done elsewhere in the testing.
        dv = DaVinci()
        j = Job(application=dv)
        create_lsf_runscript(app=dv,appname='',site='',protocol='',package='',
                             opts='dummy.opts',user_release_area='',
                             outputdata='',job=j,which='Gaudi')
        create_lsf_runscript(app=dv,appname='',site='',protocol='',package='',
                             opts='dummy.opts',user_release_area='',
                             outputdata='',job=j,which='GaudiPython')

    def test_get_user_release_area(self):
        env = {'User_release_area' : 'DUMMY'}
        ra = get_user_release_area('',env)
        assert ra == 'DUMMY','no supplied ura, ra should come from env'
        ra = get_user_release_area('SOMETHING',env)
        assert ra != 'DUMMY','supplied ura, ra should not come from env'

    def test_get_user_dlls(self):
        # FIXME: This only really tests coverage. A more involved test would
        # check if the files that should be found are found.
        dv = DaVinci() 
        shell = gaudishell_setenv(dv._impl)
        get_user_dlls('DaVinci',dv.version,dv.user_release_area,shell)

        

