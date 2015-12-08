from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed
from Ganga.Utility.logging import getLogger
logger = getLogger()
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
try:
    from Ganga.GPI import templatesLHCb, Job
except ImportError:
    templatesLHCb=[]
    pass

class TestTemplatesLHCb(GangaGPITestCase):

    def _check(self, template):
        logger.info("------------------------------------------------")
        logger.info("-    Now checking template: '%s'" % template.name)
        logger.info("------------------------------------------------")
        j = Job(template)
        j.submit()
        self.assertTrue(sleep_until_completed(j))

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

dynamic_test_case = '''
def test_###NAME###(self):
    self._check(templatesLHCb('###NAME###'))
setattr(TestTemplatesLHCb, 'test_###NAME###', test_###NAME###)
'''
for t in templatesLHCb:
    exec(dynamic_test_case.replace('###NAME###', t.name))
