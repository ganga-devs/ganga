from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed, file_contains, write_file, sleep_until_state
from Ganga.Utility.Config import setConfigOption

import shutil
import tempfile
from os.path import join

from GangaLHCb.test import *

#from GangaLHCb.test import *

import Ganga.Utility.Config
configDaVinci = Ganga.Utility.Config.getConfig('defaults_DaVinci')


class TestGaudiPython(GangaGPITestCase):

    def testLocal(self):

        j = Job(application=GaudiPython(), backend=Local())
        #j.application.version = configDaVinci['version']
        j.submit()

        assert j.application.script != [],\
            'Submit should assign defaults script file'

        assert sleep_until_completed(j, 600)

        fname = join(j.outputdir, 'stdout')
        print('file =', open(fname).read())
        executionstring = 'Welcome to ApplicationMgr'
        assert file_contains(fname, executionstring),\
            'stdout should contain string: ' + executionstring

    def testDirac(self):
        gp = GaudiPython(platform=getDiracAppPlatform())
        gp.version = configDaVinci['version']
        j = Job(application=gp, backend=Dirac())
        j.submit()
        j.remove()

    def testScripts(self):
        gp = GaudiPython()
        #gp.version = configDaVinci['version']
        tempDir = tempfile.mkdtemp()
        name1 = join(tempDir, 'script1.py')
        name2 = join(tempDir, 'script2.py')
        write_file(name1, 'print "ABC"\nexecfile("script2.py")\n')
        write_file(name2, 'print "DEF"\n')
        gp.script = [name1, name2]
        j = Job(application=gp, backend=Local())
        j.submit()
        assert sleep_until_completed(j, 600)

        fname = join(j.outputdir, 'stdout')
        print('file =', open(fname).read())
        assert file_contains(fname, 'ABC'), 'First script file not executed'
        assert file_contains(fname, 'DEF'),\
            'Inclusion of second script not working'

        shutil.rmtree(tempDir)

    def testAutomaticList(self):
        gp = GaudiPython()
        #gp.version = configDaVinci['version']
        tempDir = tempfile.mkdtemp()
        name1 = join(tempDir, 'script1.py')

        # Test that string assigned is converted into a list
        gp.script = name1
        assert gp.script[0].name == name1,\
            'String assigned should be converted into list.'

        shutil.rmtree(tempDir)

# we dont do this anymore
#    def testInvalidPlatform(self):
#        setConfigOption('LHCb','ignore_version_check',False)
#        gp = GaudiPython()
#        gp.platform='FooBar'
#        #gp.version = configDaVinci['version']
#        j = Job(application=gp,backend=Dirac())
#
#        try:
#            j.submit()
#        except JobError:
#            pass
#        except Exception, e:
#            assert False, 'Unexpected exception: '+str(e)
#        else:
#            j.remove()
#            assert False, 'Invalid platform should throw exception'

    def testSplit(self):
        gp = GaudiPython()
        #gp.version = configDaVinci['version']
        j = Job(application=gp, backend=Local())
        j.inputdata = LHCbDataset(['LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00016768/0000/00016768_00000006_1.dimuon.dst',
                                   'LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00016768/0000/00016768_00000007_1.dimuon.dst'])
        j.splitter = SplitByFiles()
        j.submit()
        assert sleep_until_completed(j, 600)

        executionstring = 'SUCCESS Reading Event record 1'
        for js in j.subjobs:
            fname = join(js.outputdir, 'stdout')
            print('file =', open(fname).read())
            assert file_contains(fname, executionstring),\
                'stdout should contain string: ' + executionstring
