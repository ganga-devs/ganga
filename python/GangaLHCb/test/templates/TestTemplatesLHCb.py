from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.GPI                 import templatesLHCb
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

class TestTemplatesLHCb(GangaGPITestCase):
    def _check(self, template):
        print template.name

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

dynamic_test_case = '''
def test_###NAME###(self):
    self._check(templatesLHCb('###NAME###'))
setattr(TestLHCbTemplates, 'test_###NAME###', test_###NAME###)
'''
for t in templatesLHCb:
    exec dynamic_test_case.replace('###NAME###', t.name)
