#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from os.path import join,split
from os import environ,pathsep
from Ganga.GPIDev.Lib.File import  FileBuffer,File
from Ganga.Lib.Root import defaultScript,defaultPyRootScript,randomString
import string

from Ganga.Core import BackendError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.GPIDev.Lib.File import File
from Ganga.Utility.Config import getConfig
import Ganga.Utility.logging
import DiracShared
import DiracUtils
import RootVersions
from DiracScript import DiracScript

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class RootDiracRunTimeHandler(IRuntimeHandler):
    """The runtime handler to run ROOT jobs on the Dirac backend"""

    def __init__(self):
        pass

    def master_prepare(self,app,appconfig):
        inputsandbox=app._getParent().inputsandbox[:]
        c = StandardJobConfig('',inputsandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        job = app.getJobObject()
        diracScript=DiracScript()

        runScript, inputsandbox, script = self._DiracWrapper(app)
        argList = [str(s) for s in app.args]

        c = StandardJobConfig(runScript,inputsandbox,argList,
                              app._getParent().outputsandbox,None)

        #architecture = self.checkArch()
        rootConfig = getConfig('ROOT')
        architecture = rootConfig['arch']
        logger.info('Root architecture to use is ', architecture)

        logFile = 'Root_%s.log' % app.version
        version=RootVersions.getDaVinciVersion(app.version)
        
        diracScript.platform(architecture)
        diracScript.runApplicationScript('DaVinci',version,
                                         DiracShared.getGenericRunScript(job),
                                         logFile)
        diracScript.setName('Ganga_ROOT_%s' % app.version)

        if job.inputdata:
            diracScript.inputdata(job.inputdata)
            if hasattr(job.inputdata,'depth'):
                diracScript.ancestordepth(job.inputdata.depth)

        if job.outputdata:
            diracScript.outputdata([f.name for f in job.outputdata.files])

        c.script=diracScript
        c.logfile=logFile

        return c

    def _DiracWrapper(self,app):
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
            # enclosed in (). Strings should be enclosed in escaped double
            # quotes.
            arglist = []
            for arg in app.args:
                if type(arg)==type('str'):
                    arglist.append('\\\\"'+arg+'\\\\"')
                else:
                    arglist.append(arg)
            rootarg='\('+string.join([str(s) for s in arglist],',')+'\)'

            #use root
            commandline = '\'root.exe -b -q '+ scriptPath + rootarg + '\''
        else:
            #use python
            pyarg = string.join(['"%s"' % str(s) for s in app.args],' ')
            commandline = '\'%(PYTHONCMD)s ' + scriptPath + ' ' + pyarg + \
                          ' -b \''

        logger.debug( "Command line: %s: ", commandline )
        wrapperscript = DiracUtils.root_dirac_wrapper(commandline,scriptPath,
                                                      app.usepython)

        logger.debug('Script to run on worker node\n'+wrapperscript)
        scriptName = "rootwrapper_generated_%s.py" % randomString()
        runScript = FileBuffer(scriptName, wrapperscript, executable=1)

        inputsandbox=[script]
        return runScript,inputsandbox,script

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
