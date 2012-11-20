import os
import sys
import tempfile
from GangaTest.Framework.tests import GangaGPITestCase
import Ganga.Utility.Config
from GangaLHCb.Lib.DIRAC.DiracScript import *
from GangaLHCb.test import *

config = Ganga.Utility.Config.getConfig('DIRAC')

class TestDiracScript(GangaGPITestCase):

    def test_DiracExe_write(self):
        exe = Executable(exe='penguins',args=['66','87','71'])
        de = DiracExe(exe)
        s = de.write()
        assert s.find('66') >= 0 and s.find('87') >= 0 and s.find('71') >= 0,\
               'args not added properly'
        assert s.find('penguins') >= 0, 'exe not added properly'
        assert s.find('Ganga_Executable.log') >= 0, 'log not added properly'

    def test_DiracInputData_write(self):
        ds = LHCbDataset(['LFN:/some/file.dst'])
        ds.depth = 66
        did = DiracInputData(ds._impl)
        s = did.write()
        assert s.find('/some/file.dst') >= 0, 'data not added properly'
        assert s.find('66') >= 0, 'depth not added properly'

    def test_DiracRoot_write(self):
        root = Root(args=['66','87','71'],version='some.version')
        dr = DiracRoot(root,'some-script.C')
        s = dr.write()
        assert s.find('66') >= 0 and s.find('87') >= 0 and s.find('71') >= 0,\
               'args not added properly'
        assert s.find('some.version') >= 0, 'version not added properly'
        assert s.find('some-script.C') >= 0, 'script not added properly'
        root.usepython = True
        dr = DiracRoot(root,'some-script.py')
        s = dr.write()
        print 's =', s
        assert s.find('setRootPythonScript') >= 0, 'python mode not working'
        assert s.find('Ganga_Root.log') >= 0, 'log not added properly'
        
    def test_DiracApplication_write(self):
        app = DaVinci(version='some.version')
        da = DiracApplication(app._impl,'some-script.py')
        s = da.write()
        assert s.find('DaVinci') >= 0, 'app name not added properly'
        assert s.find('some.version') >= 0, 'version not added properly'
        assert s.find('some-script.py') >= 0, 'script not added properly'
        assert s.find('Ganga_DaVinci_some.version.log') >= 0, \
               'log not added properly'
    
        def test_DiracScript_write(self):
            ds = DiracScript()
            ds.cpu_time = 66
            ds.site = 'some.site'
            ds.input_sandbox = ['file1.in','file2.in']
            ds.output_sandbox = ['file1.out','file2.out']
            ds.name = 'some_name'
            ds.job_type = 'LHCbJob()'
            ds.outputdata = ['data1.out','data2.out']
            ds.dirac_opts = 'some_options'
            ds.platform = getDiracAppPlatform()
            ds.write('/tmp/dswrite.py')
            file = open('/tmp/dswrite.py')
            s = file.read()
            file.close()
            assert s.find('LHCbJob()') >= 0, 'job type not added properly'
            assert s.find('some_name') >= 0, 'name not added properly'
            assert s.find('66') >= 0, 'cpu time not added properly'
            assert s.find('some.site') >= 0, 'site not added properly'
            assert s.find(str(ds.input_sandbox)) >= 0, \
                   'input sandbox  not added properly'
            assert s.find(str(ds.output_sandbox)) >= 0, \
                   'output sandbox  not added properly'
            assert s.find(str(ds.outputdata)) >= 0, \
                   'output data  not added properly'
            assert s.find('some_options') >= 0, 'diracOpts not added properly'
            assert s.find(ds.platform) >= 0, 'platform not added properly'
            ds.platform = 'not.a.platform'
            except_thrown = False
            try:
                ds.write('/tmp/dswrite.py')
            except  BackendError, e:
                except_thrown = True
            assert except_thrown, 'exception not thrown for invalid platform'
            
            
