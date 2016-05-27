import inspect
from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.Core import ApplicationConfigurationError
#from GangaLHCb.Lib.Gaudi.Francesc import GaudiExtras
import Ganga.Utility.Config

try:
    import Ganga.Utility.Config.Config
    doConfig = not Ganga.Utility.Config.Config._after_bootstrap
except x:
    print(x)
    doConfig = True

if doConfig:
    from GangaLHCb.Lib.RTHandlers.RTHUtils import *


class TestRTHUtils(GangaGPITestCase):

    def setUp(self):
        pass

    def test_jobid_as_string(self):
        from GangaLHCb.Lib.RTHandlers.RTHUtils import jobid_as_string
        j = Job(application=DaVinci())
        print('version =', j.application.version)
        ok = jobid_as_string(j).rfind(str(j.id)) >= 0
        assert ok, 'job id string should contain the job id number'
        j.inputdata = ['pfn:a', 'pfn:b']
        j.splitter = SplitByFiles(filesPerJob=1)
        j.submit()
        jid = jobid_as_string(j.subjobs[0])
        ok = jid.rfind(str(j.id)) >= 0
        assert ok, 'subjob id string should contain master id number'
        ok = jid[len(jid) - 1] == '0'
        assert ok, 'subjob id string should end w/ subjob id number'

# def test_get_master_input_sandbox(self):
##         j = Job()
##         j.inputsandbox = ['dummy.in']
##         extra = GaudiExtras()
# extra.master_input_buffers['master.buffer'] = '###MASTERBUFFER###'
##         extra.master_input_files = [File(name='master.in')]
##         isbox = get_master_input_sandbox(j,extra)
##         print('isbox = ', isbox)
##         found_buffer = False
##         found_file = False
##         found_sboxfile = False
# for f in isbox:
##             if f.name.find('dummy.in') >= 0: found_sboxfile = True
# elif f.name == 'master.in': found_file = True
# elif f.name == 'master.buffer': found_buffer = True

##         assert found_sboxfile, 'job.inputsandbox not added to input sandbox'
##         assert found_buffer, 'buffer not added to input sandbox'
##         assert found_file, 'file not added to input sandbox'

# def test_get_input_sandbox(self):
##         extra = GaudiExtras()
# extra.input_buffers['subjob.buffer'] = '###SUBJOBBUFFER###'
##         extra.input_files = [File(name='subjob.in')]
##         isbox = get_input_sandbox(extra)
##         found_buffer = False
##         found_file = False
# for f in isbox:
##             if f.name == 'subjob.in': found_file = True
# elif f.name == 'subjob.buffer': found_buffer = True
##         assert found_buffer, 'buffer not added to input sandbox'
##         assert found_file, 'file not added to input sandbox'

    def test_is_gaudi_child(self):
        from GangaLHCb.Lib.RTHandlers.RTHUtils import is_gaudi_child
        assert is_gaudi_child(DaVinci()._impl)
        #assert is_gaudi_child(Gaudi()._impl)
        assert not is_gaudi_child(GaudiPython()._impl)
        assert not is_gaudi_child(Bender()._impl)

    def test_create_runscript(self):
        from GangaLHCb.Lib.RTHandlers.RTHUtils import create_runscript
        # just check that it properly resolves Gaudi vs GaudiPython jobs
        script = """#!/usr/bin/env python

import os,sys
opts = '###OPTS###'
project_opts = '###PROJECT_OPTS###'
app = '###APP_NAME###'
app_upper = app.upper()
version = '###APP_VERSION###'
package = '###APP_PACKAGE###'
platform = '###PLATFORM###'


# check that options file exists
if not os.path.exists(opts):
    opts = 'notavailable'
    os.environ['JOBOPTPATH'] = opts
else:
    os.environ['JOBOPTPATH'] = os.path.join(os.environ[app + '_release_area'],
                                            app_upper,
                                            app_upper,
                                            version,
                                            package,
                                            app,
                                            version,
                                            'options',
                                            'job.opts')
    print 'Using the master optionsfile:', opts
    sys.stdout.flush()
    

# check that SetupProject.sh script exists, then execute it
os.environ['User_release_area'] = ''  
#os.environ['CMTCONFIG'] = platform  
f=os.popen('which SetupProject.sh')
setup_script=f.read()[:-1]
f.close()
if os.path.exists(setup_script):
    os.system('''/usr/bin/env bash -c '. `which LbLogin.sh` -c %s && source %s %s %s %s && printenv > \
env.tmp' ''' % (platform, setup_script,project_opts,app,version))
    for line in open('env.tmp').readlines():
        varval = line.strip().split('=')
        os.environ[varval[0]] = ''.join(varval[1:])
    os.system('rm -f env.tmp')
else:
    print 'Could not find %s. Your job will probably fail.' % setup_script
    sys.stdout.flush()
        
# add lib subdir in case user supplied shared libs where copied to pwd/lib
os.environ['LD_LIBRARY_PATH'] = '.:%s/lib:%s\' %(os.getcwd(),
                                                 os.environ['LD_LIBRARY_PATH'])
                                                 
#run
sys.stdout.flush()
os.environ['PYTHONPATH'] = '%s/InstallArea/python:%s' % \\
                            (os.getcwd(), os.environ['PYTHONPATH'])
os.environ['PYTHONPATH'] = '%s/InstallArea/%s/python:%s' % \\
                            (os.getcwd(), platform,os.environ['PYTHONPATH'])

cmdline = '''###CMDLINE###'''

# run command
os.system(cmdline)

###XMLSUMMARYPARSING###
"""
        print script
        print create_runscript()
        assert script == create_runscript()
