import os
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Gaudi.Gaudi import Gaudi
from GangaLHCb.Lib.Gaudi.PythonOptionsParser import PythonOptionsParser
from GangaLHCb.Lib.Gaudi.GaudiUtils import gaudishell_setenv
import Ganga.Utility.Config 

class TestPythonOptionsParser(GangaGPITestCase):

    def setUp(self):
        job = Job(application=Gauss())
        gauss = job.application._impl
        shell = gaudishell_setenv(gauss)
        optsfiles = ['./TestGaudi/Gauss.opts']
        self.extraopts = 'EventSelector(Input=[\"DATAFILE=\'LFN:dummy.dst\' TYP=\'POOL_ROOTTREE\' OPT=\'READ\'\"])'
        self.parser = PythonOptionsParser(optsfiles,self.extraopts,shell)
        self.job = job
        
    def test_PythonOptionsParser__get_opts_dict_and_pkl_string(self):
        options, opts_pkl_string = self.parser._get_opts_dict_and_pkl_string()
        assert options, 'failed to get options'
        assert opts_pkl_string, 'failed to get pkl string'

    def test_PythonOptionsParser__join_opts_files(self):
        joined_str = self.parser._join_opts_files()
        ok = joined_str.rfind(self.extraopts)
        assert ok, 'extraopts not added to options'

    def test_PythonOptionsParser_get_input_data(self):
        input = self.parser.get_input_data()
        assert len(input) == 1, 'problem collecting input data files'

    def test_PythonOptionsParser_get_output_files(self):
        config = Ganga.Utility.Config.getConfig('LHCb')
        # start w/ default
        config.setUserValue('outputsandbox_types',
                            ['NTupleSvc','HistogramPersistencySvc',
                             'MicroDSTStream'])
        sandbox, data = self.parser.get_output_files()
        ok = sandbox.count('GaussMonitor.root') == 1 and \
             sandbox.count('GaussHistos.root') == 1  and \
             data.count('Gauss.sim') == 1
        assert ok, 'collecting/sorting of output files failed (default)'
        # move the .sim file
        config.setUserValue('outputsandbox_types',
                            ['NTupleSvc','HistogramPersistencySvc',
                             'MicroDSTStream','GaussTape'])
        sandbox, data = self.parser.get_output_files()
        ok = sandbox.count('GaussMonitor.root') == 1 and \
             sandbox.count('GaussHistos.root') == 1  and \
             sandbox.count('Gauss.sim') == 1 and \
             data.count('Gauss.sim') == 0
        assert ok, 'collecting/sorting of output files failed (.sim->sandbox)'
        # move the .root files to data
        config.setUserValue('outputsandbox_types',['GaussTape'])
        sandbox, data = self.parser.get_output_files()
        ok = data.count('GaussMonitor.root') == 1 and \
             data.count('GaussHistos.root') == 1  and \
             sandbox.count('Gauss.sim') == 1 and \
             len(data) == 2 and len(sandbox) == 1
        assert ok, 'collecting/sorting of output files failed (.root->data)'

    def test_PythonOptionsParser_get_output(self):
        config = Ganga.Utility.Config.getConfig('LHCb')
        # previous method tests the configure stuff...let's just make sure we
        # can properly override it in a job
        config.setUserValue('outputsandbox_types',
                            ['NTupleSvc','HistogramPersistencySvc',
                             'MicroDSTStream'])
        j = self.job
        j.outputsandbox = []
        j.outputdata = None
        sandbox, data = self.parser.get_output(j)
        ok = sandbox.count('GaussMonitor.root') == 1 and \
             sandbox.count('GaussHistos.root') == 1  and \
             data.count('Gauss.sim') == 1
        assert ok, 'collecting/sorting of output files failed (default)'
        j.outputsandbox = ['Gauss.sim']
        sandbox, data = self.parser.get_output(j)
        ok = sandbox.count('Gauss.sim') == 1 and len(data) == 0
        assert ok, 'collecting/sorting of output files failed (.sim->sandbox)'
        j.outputsandbox = []
        j.outputdata = ['*.root']
        sandbox, data = self.parser.get_output(j)
        ok = len(sandbox) == 0 and data.count('GaussMonitor.root') == 1 and \
             data.count('GaussHistos.root') == 1
        assert ok, 'collecting/sorting of output files failed (.root->data)'
        # make sure if matches both goes to data
        j.outputsandbox = ['*.sim']
        j.outputdata = ['*.sim']
        sandbox, data = self.parser.get_output(j)
        ok = data.count('Gauss.sim') == 1 and sandbox.count('Gauss.sim') == 0
        assert ok, 'collecting/sorting of output files failed (matches both)'
        
