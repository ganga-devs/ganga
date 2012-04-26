#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Lib.File import File
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from DiracScript import *

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def exe_dirac_wrapper(cmdline):
    return """#!/usr/bin/env python
'''Script to run Executable application'''

from os import system, environ, pathsep, getcwd
import sys

# Main
if __name__ == '__main__':

    environ['PATH'] = getcwd() + (pathsep + environ['PATH'])        
    sys.exit(system('''%s''')/256)
  """ % cmdline

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class ExeDiracRTHandler(IRuntimeHandler):
    """The runtime handler to run plain executables on the Dirac backend"""

    def master_prepare(self,app,appconfig):        
        inputsandbox = app._getParent().inputsandbox[:]
        if type(app.exe) == File:
            os.system('chmod +x %s' % app.exe.name)
            inputsandbox.append(app.exe)
        c = StandardJobConfig('',inputsandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        j = app.getJobObject()
        script = self._create_exe_script(app)
        c = StandardJobConfig(script,[File(script)],app.args,
                              j.outputsandbox,app.env)

        dirac_script = DiracScript()
        dirac_script.job_type = 'LHCbJob()'
        #dirac_script.exe = DiracExe(app.exe,app.args)
        dirac_script.exe = DiracExe('exe-script.py',[])
        dirac_script.output_sandbox = j.outputsandbox[:]

        if j.inputdata: dirac_script.inputdata = DiracInputData(j.inputdata)
        if j.outputdata: dirac_script.outputdata = j.outputdata
        c.script = dirac_script
        
        return c

    def _create_exe_script(self,app):
        '''Creates the script that will be executed by DIRAC job. '''
        commandline = app.exe
        if type(app.exe) == File:
            commandline = os.path.basename(app.exe.name)
        if app.args:
            for arg in app.args: commandline += " %s" % arg        
        logger.debug('Command line: %s: ', commandline)
        wrapper = exe_dirac_wrapper(commandline)
        j = app.getJobObject()
        script = "%s/exe-script.py" % j.getInputWorkspace().getPath()
        file = open(script,'w')
        file.write(wrapper)
        file.close()
        os.system('chmod +x %s' % script)
        return script

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
