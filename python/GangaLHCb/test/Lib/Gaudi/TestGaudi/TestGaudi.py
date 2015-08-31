import os
import shutil
import tempfile
from GangaTest.Framework.tests import GangaGPITestCase
#from GangaLHCb.Lib.Gaudi.Gaudi import GaudiExtras
#from GangaLHCb.Lib.Applications.AppsBase import *
from GangaTest.Framework.utils import read_file, failureException
import Ganga.Utility.logging
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename

logger = Ganga.Utility.logging.getLogger()

try:
    import Ganga.Utility.Config.Config
    doConfig = not Ganga.Utility.Config.Config._after_bootstrap
except x:
    print(x)
    doConfig = True

if doConfig:
    from GangaLHCb.Lib.Applications.AppsBaseUtils import available_apps


class TestGaudi(GangaGPITestCase):

    def setUp(self):
        pass

    def test_Gaudi__auto__init__(self):
        dv = DaVinci()
        assert dv._impl.version, 'version not set automatically'
        assert dv._impl.platform, 'platform not set automatically'
        assert dv._impl.package, 'package not set automatically'
        assert dv._impl.user_release_area, 'ura not set automatically'

    def test_Gaudi_prepare(self):
        # Test standalone preparation
        d = DaVinci()
        d.prepare()
        assert d.is_prepared is not None, 'is_prepared not correctly set'
        #assert d._impl.prep_inputbox, 'inputbox empty'

        # Now test as part of job
        j = Job(application=DaVinci())
        j.prepare()
        assert j.application.is_prepared is not None, 'is_prepared not correctly set'
        #assert j.application._impl.prep_inputbox, 'inputbox empty'

        job = Job(application=Gauss(optsfile='./Gauss-Job.py'))
        job.application.platform = 'x86_64-slc6-gcc48-opt'
        gauss = job.application
        job.inputdata = ['pfn:dummy1.in', 'pfn:dummy2.in']
        job.outputfiles = [
            'Gauss.sim', 'GaussHistos.root', 'GaussMonitor.root']
        #job.outputsandbox = ['GaussHistos.root','GaussMonitor.root']
        #inputs,extra = gauss._impl.master_configure()
        # provide basic test of where output goes - a more complete test is
        # run on the PythonOptionsParser methods.
        job.prepare()
# ok = job.application._impl.prep_outputbox.count('GaussHistos.root') > 0 and \
##              job.application._impl.prep_outputbox.count('GaussMonitor.root') > 0
        share_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                  'shared',
                                  getConfig('Configuration')['user'],
                                  job.application.is_prepared.name)
# ok = os.path.exists(os.path.join(job.application._impl.is_prepared.name,'GaussHistos.root')) and \
# os.path.exists(os.path.join(job.application._impl.is_prepared.name,'GaussMonitor.root'))
        assert os.path.exists(
            os.path.join(share_path, 'output', 'options_parser.pkl')), 'outputsandbox error'
        os.system('cd ' + share_path + '/inputsandbox/' +
                  ';tar -xzvf _input_sandbox_' + job.application.is_prepared.name + '.tgz')
        assert os.path.exists(os.path.join(
            share_path, 'inputsandbox', 'options.pkl')), 'pickled options file error'
        assert os.path.exists(
            os.path.join(share_path, 'debug', 'gaudi-env.py.gz')), 'zipped env file error'
        #assert job.application._impl.prep_outputdata.files.count('Gauss.sim') > 0,'outputdata error'
        #assert [f.name for f in job.application._impl.prep_inputbox].count('options.pkl') is not None, 'no options pickle file'
        #assert [f.name for f in job.application._impl.prep_inputbox].count('gaudi-env.py.gz') is not None, 'no evn file'

    def test_Gaudi_unprepare(self):
        d = DaVinci()
        d.prepare()
        assert d.is_prepared is not None, 'is_prepared not correctly set'
        #assert d._impl.prep_inputbox, 'inputbox empty'
        d.unprepare()
        assert d.is_prepared is None, 'is_prepared not correctly unset'
        #assert not d._impl.prep_inputbox, 'inputbox not cleared properly'

    def test_Gaudi_master_configure(self):
        pass
##         job = Job(application=Gauss(optsfile='./Gauss-Job.py'))
##         gauss = job.application
##         job.inputdata = ['pfn:dummy1.in','pfn:dummy2.in']
##         job.outputdata = ['Gauss.sim']
##         job.outputsandbox = ['GaussHistos.root','GaussMonitor.root']
##         inputs,extra = gauss._impl.master_configure()
# provide basic test of where output goes - a more complete test is
# run on the PythonOptionsParser methods.
# ok = extra.outputsandbox.count('GaussHistos.root') > 0 and \
##              extra.outputsandbox.count('GaussMonitor.root') > 0
##         assert ok, 'outputsandbox error'
##         assert extra.outputdata.files.count('Gauss.sim') > 0,'outputdata error'
##         assert extra.master_input_buffers['options.pkl'] is not None

    # this method currently does nothing
    # def test_Gaudi_configure(self):

    # not much to check here...as this method simply runs checks itself
    # def test_Gaudi__check_inputs(self):

    # Andrew's new method...hopefully he can provide a unit test for it
    # def test_Gaudi_readInputData(self):
