from GangaTest.Framework.tests import GangaGPITestCase

from GangaLHCb.Lib.Dirac import DiracServices

class TestDiracServices(GangaGPITestCase):
    
    def testBookkeepingService(self):
        assert DiracServices.checkBookkeeping(), 'query should return cleanly'
        
    def testInputsandboxService(self):
        assert DiracServices.checkInputsandbox(), 'query should return cleanly'
        
    def testOutputsandboxService(self):
        assert DiracServices.checkOutputsandbox(), 'query should return cleanly'
    
    