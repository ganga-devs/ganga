from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed,file_contains,write_file,sleep_until_state

import shutil
import tempfile
from os.path import join

import Ganga.Utility.Config
config = Ganga.Utility.Config.getConfig('DIRAC')


class TestBender(GangaGPITestCase):
        
    def testAutomaticList(self):
        bd = Bender()
        
        dir = tempfile.mkdtemp()
        name1 = join(dir,'script1.py')

        # Test that string assigned is converted into a list
        bd.script=name1
        assert bd.script[0].name==name1,\
               'String assigned should be converted into list.'

        shutil.rmtree(dir)

    def testInvalidPlatform(self):
        bd = Bender()
        bd.platform='FooBar'

        j = Job(application=bd,backend=Dirac())

        try:
            j.submit()
        except JobError:
            pass
        except Exception, e:
            assert False, 'Unexpected exception: '+str(e)
        else:
            j.remove()
            assert False, 'Invalid platform should throw exception'
