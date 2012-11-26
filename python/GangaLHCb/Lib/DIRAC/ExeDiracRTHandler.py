#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Lib.File import File
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from DiracScript import *
from Ganga.Utility.files import expandfilename
from Ganga.Utility.Config import getConfig
from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns, getWNCodeForOutputPostprocessing

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def exe_dirac_wrapper(cmdline,job):
    cmd = """#!/usr/bin/env python
'''Script to run Executable application'''

from os import system, environ, pathsep, getcwd
import sys

# Main
if __name__ == '__main__':

    environ['PATH'] = getcwd() + (pathsep + environ['PATH'])        
    rc = (system('''%s''')/256)
    ###OUTPUTFILESINJECTEDCODE###

    sys.exit(rc)
  """ % cmdline

    cmd = cmd.replace('###OUTPUTFILESINJECTEDCODE###',getWNCodeForOutputPostprocessing(job, '    '))
    return cmd
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])
class ExeDiracRTHandler(IRuntimeHandler):
    """The runtime handler to run plain executables on the Dirac backend"""

    def master_prepare(self,app,appconfig):        
        inputsandbox = app._getParent().inputsandbox[:]
        if type(app.exe) == File:
            exefile = os.path.join(shared_path,app.is_prepared.name,os.path.basename(app.exe.name))
            if not os.path.exists(exefile):
                exefile = app.exe.name
                if not os.path.exists(exefile):
                    msg = 'Executable must exist!'
                    raise ApplicationConfigurationError(None,msg)
                    
            os.system('chmod +x %s' % exefile)
            inputsandbox.append(File(os.path.join(shared_path,app.is_prepared.name,os.path.basename(exefile))))
        c = StandardJobConfig('',inputsandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        j = app.getJobObject()
        script = self._create_exe_script(app)
        c = StandardJobConfig(exe       = File(name = script),
                              inputbox  = [],#master inputsandbox added automatically
                              args      = app.args,
                              outputbox = j.outputsandbox,
                              env       = app.env)

        dirac_script = DiracScript()
        dirac_script.job_type = 'LHCbJob()'
        #dirac_script.exe = DiracExe(app.exe,app.args)
        dirac_script.exe = DiracExe('exe-script.py',[])
        dirac_script.output_sandbox = j.outputsandbox[:]
        dirac_script.output_sandbox += getOutputSandboxPatterns(j)

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
        j = app.getJobObject()
        wrapper = exe_dirac_wrapper(commandline, j)
        script = "%s/exe-script.py" % j.getInputWorkspace().getPath()
        file = open(script,'w')
        file.write(wrapper)
        file.close()
        os.system('chmod +x %s' % script)
        return script

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
