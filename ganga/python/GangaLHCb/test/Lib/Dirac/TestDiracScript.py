from os.path import join
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Dirac.DiracScript import DiracScript
import Ganga.Utility.Config
from Ganga.Core import BackendError

config = Ganga.Utility.Config.getConfig('DIRAC')

class TestDiracScript(GangaGPITestCase):

    def test_DiracScript___init__(self):
        ds = DiracScript()
        assert ds.script.find('LHCbJob()') >= 0, 'script not initialized'

    def test_DiracScript_append(self):
        ds = DiracScript()
        ds.append('dummyCommand()')
        assert ds.script.find('djob.dummyCommand()') >= 0, 'not added'

    def test_DiracScript_inputdata(self):
        data = LHCbDataset(['LFN:dummy1.file','LFN:dummy2.file'])
        ds = DiracScript()
        ds.inputdata(data)
        s = 'setInputData(' + str(['dummy1.file', 'dummy2.file']) + ')'
        assert ds.script.find(s) >= 0, 'data files not added properly'

    def test_DiracScript_outputdata(self):
        data = ['dummy1.file','dummy2.file']
        ds = DiracScript()
        ds.outputdata(data)
        s = 'setOutputData(' + str(['dummy1.file', 'dummy2.file']) + ')'
        assert ds.script.find(s) >= 0, 'data files not added properly'

    def test_DiracScript_platform(self):
        ds = DiracScript()
        pl = config['AllowedPlatforms'][0]
        try:
            ds.platform(pl)
            s = "setSystemConfig(%s)" % repr(pl)
            assert ds.script.find(s) >= 0, 'script error'
        except BackendError:
            assert False, 'BackendError should not have been raised'

        raised = False
        try:
            ds.platform('This is not a good platform')
        except BackendError:
            raised = True

        assert raised, 'BackendError should have been raised'
    
    def test_DiracScript_addPackage(self):
        ds = DiracScript()
        ds.addPackage('SomeApp','SomeVersion')
        s = "addPackage('%s','%s')" % ('SomeApp', 'SomeVersion')
        assert ds.script.find(s) >= 0
    
    def test_DiracScript_setName(self):
        ds = DiracScript()
        ds.setName('SomeName')
        s = "setName('%s')" % 'SomeName'
        assert ds.script.find(s) >= 0
        
    def test_DiracScript_setExecutable(self):
        ds = DiracScript()
        ds.setExecutable(command='SomeCommand',logFile=None)
        s = "setExecutable('%s')" % 'SomeCommand'
        assert ds.script.find(s) >= 0, 'error when logFile=None'
        ds = DiracScript()
        ds.setExecutable(command='SomeCommand',logFile='Anything')
        s = "setExecutable('%s', logFile = '%s')" % ('SomeCommand', 'Anything')
        assert ds.script.find(s) >= 0, 'error when logFile is not None'

    def test_DiracScript_runApplicationScript(self):
        ds = DiracScript()
        app = 'SomeApp'
        ver = 'SomeVersion'
        scr = 'SomeScript'
        ds.runApplicationScript(appName=app,appVersion=ver,scriptFile=scr,
                                logFile=None)
        s = "setApplicationScript('%s','%s','%s')" % (app,ver,scr)
        assert ds.script.find(s) >= 0, 'error when logFile=None'
        ds = DiracScript()
        ds.runApplicationScript(appName=app,appVersion=ver,scriptFile=scr,
                                logFile='Anything')
        s = "setApplicationScript('%s','%s','%s', logFile = '%s')" \
            % (app,ver,scr,'Anything')
        assert ds.script.find(s) >= 0, 'error when logFile is not None'

    def test_DiracScript_setDestination(self):
        ds = DiracScript()
        ds.setDestination(None) # shouldn't fail
        ds.setDestination('localhost')
        assert ds.mode == 'agent', 'agent mode not set'
        ds.setDestination('SomeWhere')
        s = 'setDestination("%s")' % 'SomeWhere'
        assert ds.script.find(s) >= 0, 'not added to script properly'

    def test_DiracScript_finalise(self):
        # An exception should be thrown for the English spelling of the method!
        ds = DiracScript()
        ds.finalise()
        assert ds.finalise, 'should have finalise = True'
        s = 'mydirac.submit'
        assert ds.script.find(s) >= 0, 'submit not written into script'
        
    def test_DiracScript_write(self):
        j = Job(backend=Dirac())
        ds = DiracScript()
        ds.write(j._impl)
        f = open(join(j._impl.getInputWorkspace().getPath(),'DIRACscript'))
        assert ds.script == f.read(), 'script not written correctly'

    # probably have to test this in the GPI tests
    #def test_DiracScript_execute(self):

    def test_DiracScript_commands(self):
        ds = DiracScript()
        ds.script = 'stuff'
        assert ds.commands() == 'stuff'
    
