import inspect
from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.Core import ApplicationConfigurationError
from GangaLHCb.Lib.Gaudi.RTHUtils import *
from GangaLHCb.Lib.Gaudi.Francesc import GaudiExtras
import Ganga.Utility.Config 

class TestRTHUtils(GangaGPITestCase):

    def setUp(self):
        pass

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

    def test_gen_catalog(self):
        site = Ganga.Utility.Config.getConfig('LHCb')['LocalSite']
        data = LHCbDataset(["LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000001_5.dst"])
        catalog = gen_catalog(data,site)
        print 'catalog = ', catalog
        
    def test_get_master_input_sandbox(self):
        j = Job()
        j.inputsandbox = ['dummy.in']
        extra = GaudiExtras()
        extra.master_input_buffers['master.buffer'] = '###MASTERBUFFER###'
        extra.master_input_files = [File(name='master.in')]
        isbox = get_master_input_sandbox(j,extra)
        print 'isbox = ', isbox
        found_buffer = False
        found_file = False
        found_sboxfile = False
        for f in isbox:
            if f.name.find('dummy.in') >= 0: found_sboxfile = True
            elif f.name == 'master.in': found_file = True
            elif f.name == 'master.buffer': found_buffer = True

        assert found_sboxfile, 'job.inputsandbox not added to input sandbox'
        assert found_buffer, 'buffer not added to input sandbox'
        assert found_file, 'file not added to input sandbox'

    def test_get_input_sandbox(self):
        extra = GaudiExtras()
        extra.input_buffers['subjob.buffer'] = '###SUBJOBBUFFER###'
        extra.input_files = [File(name='subjob.in')]
        isbox = get_input_sandbox(extra)
        found_buffer = False
        found_file = False
        for f in isbox:
            if f.name == 'subjob.in': found_file = True
            elif f.name == 'subjob.buffer': found_buffer = True
        assert found_buffer, 'buffer not added to input sandbox'
        assert found_file, 'file not added to input sandbox'        
        
    def test_is_gaudi_child(self):
        assert is_gaudi_child(DaVinci()._impl)
        assert is_gaudi_child(Gaudi()._impl)
        assert not is_gaudi_child(GaudiPython()._impl)
        assert not is_gaudi_child(Bender()._impl)

    def test_create_runscript(self):
        # just check that it properly resolves Gaudi vs GaudiPython jobs
        dv = DaVinci()._impl
        gp = GaudiPython()._impl
        j = Job()
        script = create_runscript(dv,'stuff',j)
        assert script.find('gaudirun.py') >= 0
        script = create_runscript(gp,'stuff',j)
        assert script.find('gaudiPythonwrapper.py') >= 0
