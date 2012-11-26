#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig
from DiracScript import *
from DiracUtils import *
import Ganga.Utility.logging
from Ganga.Core import ApplicationConfigurationError
from Ganga.Utility.files import expandfilename
logger = Ganga.Utility.logging.getLogger()
from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns
rootVersions = None

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])
class RootDiracRTHandler(IRuntimeHandler):
    """The runtime handler to run ROOT jobs on the Dirac backend"""

    def master_prepare(self,app,appconfig):
        # check file is set OK
        if not app.script.name:
            msg = 'Root.script.name must be set.'
            raise ApplicationConfigurationError(None,msg)
        sharedir_scriptpath = os.path.join(shared_path,app.is_prepared.name,os.path.basename(app.script.name))
        scriptname = sharedir_scriptpath
        if not os.path.exists(sharedir_scriptpath):
            scriptname = app.script.name
            if not os.path.exists(app.script.name):
                msg = 'Script must exist!'
                raise ApplicationConfigurationError(None,msg)
            
        # check root version
        global rootVersions
        if not rootVersions:
            from Dirac import Dirac
            result = Dirac.execAPI('result = DiracCommands.getRootVersions()')
            if not result_ok(result):
                logger.error('Could not obtain available ROOT versions: %s' \
                             % str(result))
                logger.error('ROOT version will not be validated.')
            else: rootVersions = result['Value']
        if rootVersions:
            found = False
            versions = []
            for v in rootVersions:
                versions.append(v)
                if app.version.find(v) >= 0:
                    found = True
                    break
            if not found:
                msg = 'Invalid ROOT version: %s.  Valid versions: %s' \
                      % (app.version, str(versions))
                raise ApplicationConfigurationError(None,msg)
        inputsandbox = app._getParent().inputsandbox[:]
        c = StandardJobConfig(scriptname,inputsandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        j = app.getJobObject()
        c = StandardJobConfig(jobmasterconfig.exe,[],[],j.outputsandbox,{})
        if jobmasterconfig.exe.find(app.is_prepared.name) <0:
            # copy script to input dir if not in sharedir
            input_dir = j.getInputWorkspace().getPath()
            script_name = '%s/%s' % (input_dir,os.path.basename(jobmasterconfig.exe))
            os.system('cp %s %s/.' % (jobmasterconfig.exe,input_dir))
            c.exe=script_name
            c.processValues()
            
        dirac_script = DiracScript()
        dirac_script.job_type = 'LHCbJob()'
        dirac_script.exe = DiracRoot(app,c.exe)
        dirac_script.platform = getConfig('ROOT')['arch']
        dirac_script.output_sandbox = j.outputsandbox[:]
        dirac_script.output_sandbox += getOutputSandboxPatterns(j)
        
        if j.inputdata: dirac_script.inputdata = DiracInputData(j.inputdata)
        if j.outputdata: dirac_script.outputdata = j.outputdata
        c.script = dirac_script
        
        return c

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
