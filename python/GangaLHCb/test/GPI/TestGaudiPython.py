from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed,file_contains,write_file,sleep_until_state

import shutil
import tempfile
from os.path import join

import Ganga.Utility.Config
config = Ganga.Utility.Config.getConfig('DIRAC')


class TestGaudiPython(GangaGPITestCase):
    
    
    def testLocal(self):
        
        j = Job(application=GaudiPython(), backend=Local())
        
        j.submit()

        assert j.application.script != [],\
               'Submit should assign defaults script file'

        assert sleep_until_completed(j,600)

        fname = join(j.outputdir,'stdout')

        executionstring = 'Application Manager Configured successfully'
        assert file_contains(fname,executionstring),\
               'stdout should contain string: ' + executionstring

    def testDirac(self):
        gp = GaudiPython(platform=config['AllowedPlatforms'][0])
        j = Job(application=gp, backend=Dirac())
        j.submit()
        j.remove()
        
    def testScripts(self):
        gp = GaudiPython()
        
        dir = tempfile.mkdtemp()
        name1 = join(dir,'script1.py')
        name2 = join(dir,'script2.py')
        write_file(name1,'print "ABC"\nexecfile("script2.py")\n')
        write_file(name2,'print "DEF"\n')
        gp.script=[name1,name2]
        j = Job(application=gp, backend=Local())
        j.submit()
        assert sleep_until_completed(j,600)

        
        fname = join(j.outputdir,'stdout')
        assert file_contains(fname,'ABC'), 'First script file not executed'
        assert file_contains(fname,'DEF'),\
               'Inclusion of second script not working'
        
        shutil.rmtree(dir)

    def testAutomaticList(self):
        gp = GaudiPython()
        
        dir = tempfile.mkdtemp()
        name1 = join(dir,'script1.py')

        # Test that string assigned is converted into a list
        gp.script=name1
        assert gp.script[0].name==name1,\
               'String assigned should be converted into list.'

        shutil.rmtree(dir)

    def testInvalidPlatform(self):
        gp = GaudiPython()
        gp.platform='FooBar'

        j = Job(application=gp,backend=Dirac())

        try:
            j.submit()
        except JobError:
            pass
        except Exception, e:
            assert False, 'Unexpected exception: '+str(e)
        else:
            j.remove()
            assert False, 'Invalid platform should throw exception'

    def testValidPlatform(self):
        gp = GaudiPython()
        gp.platform='FooBar'
