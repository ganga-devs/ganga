import os
import sys
import tempfile
from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.GPIDev.Credentials import GridProxy
from Ganga.Core import BackendError
from GangaLHCb.Lib.Dirac.ExeDiracRunTimeHandler import ExeDiracRunTimeHandler
import Ganga.Utility.Config 
config = Ganga.Utility.Config.getConfig('DIRAC')

class TestDirac(GangaGPITestCase):

    def test_Dirac__checkset_CPUTime(self):
        d = Dirac()
        d._impl._checkset_CPUTime(200)
        assert d.CPUTime == 300
        d._impl._checkset_CPUTime(43000)
        assert d.CPUTime == 43200
        d._impl._checkset_CPUTime(220000)
        assert d.CPUTime == 216000

    def test_Dirac__handleGridProxy(self):
        d = Dirac()
        time_left =  float(GridProxy().timeleft("hours"))*3600
        
        config.setUserValue('extraProxytime',str(time_left - 3600))
        try:
            d._impl._handleGridProxy()
        except BackendError:
            assert False, 'An error should not have been thrown'

        config.setUserValue('extraProxytime',str(time_left + 3600))
        error = False
        try:
            d._impl._handleGridProxy()
        except BackendError:
            error = True

        assert error, 'An error should have been thrown.'

    # no reason to test this one
    #def test_Dirac__diracverbosity(self):

    # this is mostly just a wrapper for a number of other tested methods...not
    # sure what else i can add here.
    #def test_Dirac_submit(self):

    # this method is a wrapper for _handleGridProxy and _diracsubmit
    #def test_Dirac_resubmit(self):

    def test_Dirac__diracsubmit(self):
        j = Job(backend=Dirac())
        j.submit()
        assert j.backend._impl._diracsubmit(), 'DIRAC ID not obtained'
        j.kill()
        
    def test_Dirac_kill(self):
        j = Job(backend=Dirac())
        j.submit()
        try:
            j.backend._impl.kill()
        except BackendError:
            assert False, 'BackendError should not have been thrown.'

        j.kill()
        

    def test_Dirac_peek(self):
        j = Job(backend=Dirac())
        j.submit()
        stdout = sys.stdout
        tmpdir = tempfile.mktemp()
        f = tempfile.NamedTemporaryFile()
        #f = open('./__tmpstdout__','w')
        sys.stdout = f
        j.backend._impl.peek()        
        sys.stdout = stdout
        j.kill()
        f.flush()
        #f = open('./__tmpstdout__','r')
        fstdout = open(f.name,'r')
        s = fstdout.read()
        assert s, 'Something should have been written to std output'
        #f.close()
        #os.system('rm -f ./__tmpstdout__')

    # not sure how to test this w/o running a complete job
    #def test_Dirac_getOutput(self):

    def test_Dirac_master_prepare(self):
        j = Job(backend=Dirac())
        appmstr = j.application._impl.master_configure()[1]
        rt = ExeDiracRunTimeHandler()
        jobmstr = rt.master_prepare(j.application._impl,appmstr)
        ret = j.backend._impl.master_prepare(jobmstr)
        found = False
        for f in ret:
            if f.find('_master.tgz') >= 0:
                found = True
                break
        assert found, 'master .tgz sandbox file not found'
        
    # not sure how to unit-test this
    #def test_Dirac__add_outputdata(self):

    # probably have to just test this in the GPI tests
    #def test_Dirac_getOutputData(self):

    def test_Dirac__handleInputSandbox(self):
        j = Job(backend=Dirac())
        appmstr = j.application._impl.master_configure()[1]
        rt = ExeDiracRunTimeHandler()
        appsub = j.application._impl.configure(appmstr)[1]
        jobmstr = rt.master_prepare(j.application._impl,appmstr)
        jobsub = rt.prepare(j.application._impl,appsub,appmstr,jobmstr)
        j.backend._impl._handleInputSandbox(jobsub,jobmstr.inputbox)
        s = jobsub.script.script
        
        assert s.find('setInputSandbox(') >= 0, 'Sandbox command not found'
        assert s.find('jobscript.py') >= 0, 'jobscript.py not added to sandbox'

    def test_Dirac__handleOutputSandbox(self):
        j = Job(backend=Dirac())
        appmstr = j.application._impl.master_configure()[1]
        rt = ExeDiracRunTimeHandler()
        appsub = j.application._impl.configure(appmstr)[1]
        jobmstr = rt.master_prepare(j.application._impl,appmstr)
        jobsub = rt.prepare(j.application._impl,appsub,appmstr,jobmstr)
        j.backend._impl._handleOutputSandbox(jobsub,jobmstr.inputbox)
        s = jobsub.script.script
        
        assert s.find('setOutputSandbox(') >= 0, 'Sandbox command not found'
        assert s.find('__jobstatus__') >= 0, '__jobstatus__ not added'
        assert s.find('stdout') >= 0, 'stdout not added to sandbox'
        assert s.find('stderr') >= 0, 'stderr not added to sandbox'
        
    def test_Dirac__handleApplication(self):
        j = Job(backend=Dirac())
        appmstr = j.application._impl.master_configure()[1]
        rt = ExeDiracRunTimeHandler()
        appsub = j.application._impl.configure(appmstr)[1]
        jobmstr = rt.master_prepare(j.application._impl,appmstr)
        jobsub = rt.prepare(j.application._impl,appsub,appmstr,jobmstr)
        j.backend._impl._handleApplication(jobsub,jobmstr.inputbox)
        s = jobsub.script.script

        assert s.find('setExecutable') >= 0, 'Executable command not found'
        assert s.find('diracJobMain.py') >= 0, 'diracJobMain.py not found'

    # test this in GPI
    #def test_Dirac_updateMonitoringInformation(self):

