from Ganga.Core import BackendError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Lib.File import File
from Ganga.Utility.Config import getConfig
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

import DiracShared

class RootDiracRunTimeHandler(IRuntimeHandler):
    """The runtime handler to run ROOT jobs on the Dirac backend"""

    def __init__(self):
        pass


    def master_prepare(self,app,appconfig):

        inputsandbox=app._getParent().inputsandbox[:]

        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        c = StandardJobConfig('',inputsandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        job = app.getJobObject()

        from DiracScript import DiracScript
        diracScript=DiracScript()

        runScript, inputsandbox, script = self._DiracWrapper(app)
        argList = [str(s) for s in app.args]

        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        c = StandardJobConfig(runScript,inputsandbox,argList,app._getParent().outputsandbox,None)

        #architecture = self.checkArch()
        rootConfig = getConfig('ROOT')
        architecture = rootConfig['arch']
        logger.info('Root architecture to use is ', architecture)

        import RootVersions
        logFile = 'Root_%s.log' % app.version
        version=RootVersions.getDaVinciVersion(app.version)
        
        diracScript.platform(architecture)
        diracScript.runApplicationScript('DaVinci',version,\
                                         DiracShared.getGenericRunScript(),logFile)
        diracScript.setName('Ganga_ROOT_%s' % app.version)

        if job.inputdata:
            diracScript.inputdata([f.name for f in job.inputdata.files])

        if job.outputdata:
            diracScript.outputdata([f.name for f in job.outputdata.files])

        c.script=diracScript
        c.logfile=logFile

        return c


    def _DiracWrapper(self,app):
        from os.path import join,split
        from os import environ,pathsep
        from Ganga.GPIDev.Lib.File import  FileBuffer,File
        from Ganga.Lib.Root import defaultScript,defaultPyRootScript,randomString
        import string

        script=app.script
        if script==File():
            if not app.usepython:
                script=File(defaultScript())
            else:
                script=File(defaultPyRootScript())

        commandline = ''
        scriptPath  = join('.',script.subdir,split(script.name)[1])
        if not app.usepython:
            # Arguments to the ROOT script needs to be a comma separated list
            # enclosed in (). Strings should be enclosed in escaped double quotes.
            arglist = []
            for arg in app.args:
                if type(arg)==type('str'):
                    arglist.append('\\\\"'+arg+'\\\\"')
                else:
                    arglist.append(arg)
            rootarg='\('+string.join([str(s) for s in arglist],',')+'\)'

            #use root
            commandline='\'root.exe -b -q '+ scriptPath + \
                       rootarg + '\''
        else:
            #use python
            pyarg = string.join([str(s) for s in app.args],' ')
            commandline = '\'%(PYTHONCMD)s ' + scriptPath + ' ' + pyarg + ' -b \''

        logger.debug( "Command line: %s: ", commandline )

        # Write a wrapper script
        wrapperscript= """#!/usr/bin/env python
'''Script to run root with cint or python.'''
def setEnvironment(key, value, update=False):
    '''Sets an environment variable. If update=True, it preends it to
    the current value with os.pathsep as the seperator.'''
    from os import environ,pathsep
    if update and environ.has_key(key):
        value += (pathsep + environ[key])#prepend
    environ[key] = value

# Main
if __name__ == '__main__':

    from os import curdir, system, environ, pathsep, sep
    from os.path import join
    import sys    

    commandline = ###COMMANDLINE###    
    scriptPath = '###SCRIPTPATH###'
    usepython = ###USEPYTHON###
    version = '###ROOTVERSION###'

    sys.stdout.flush()
    sys.stderr.flush()

    #see HowtoPyroot in the root docs
    setEnvironment('LD_LIBRARY_PATH',curdir,True)
    from os import environ
    rootsys=environ['ROOTSYS']

    if usepython:

        pythonCmd = 'python'
        commandline = commandline % {'PYTHONCMD':pythonCmd}

        setEnvironment('PYTHONPATH',join(rootsys,'lib'),True)

    #exec the script
    print 'Executing ',commandline
    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(system(commandline)/256)
  """

        wrapperscript = wrapperscript.replace('###COMMANDLINE###',commandline)
        wrapperscript = wrapperscript.replace('###ROOTVERSION###',app.version)
        wrapperscript = wrapperscript.replace('###SCRIPTPATH###',scriptPath)
        wrapperscript = wrapperscript.replace('###USEPYTHON###',str(app.usepython))

        logger.debug('Script to run on worker node\n'+wrapperscript)
        scriptName = "rootwrapper_generated_%s.py" % randomString()
        runScript = FileBuffer( scriptName, wrapperscript, executable = 1 )

        inputsandbox=[script]
        return runScript,inputsandbox,script
