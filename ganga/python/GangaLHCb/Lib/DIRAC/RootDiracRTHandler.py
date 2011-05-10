#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig
from DiracScript import *
from DiracUtils import *
import Ganga.Utility.logging
from Ganga.Core import ApplicationConfigurationError

logger = Ganga.Utility.logging.getLogger()

rootVersions = None

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class RootDiracRTHandler(IRuntimeHandler):
    """The runtime handler to run ROOT jobs on the Dirac backend"""

    def master_prepare(self,app,appconfig):
        # check file is set OK
        if not app.script.name:
            msg = 'Root.script.name must be set.'
            raise ApplicationConfigurationError(None,msg)
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
        c = StandardJobConfig('',inputsandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        j = app.getJobObject()
        input_dir = j.getInputWorkspace().getPath()
        script_name = '%s/%s' % (input_dir,os.path.basename(app.script.name))
        c = StandardJobConfig(script_name,[],[],j.outputsandbox,{})

        # copy script to input dir
        os.system('cp %s %s/.' % (app.script.name,input_dir))

        dirac_script = DiracScript()
        dirac_script.job_type = 'LHCbJob()'
        dirac_script.exe = DiracRoot(app,script_name)
        dirac_script.platform = getConfig('ROOT')['arch']
        dirac_script.output_sandbox = j.outputsandbox[:]

        if j.inputdata: dirac_script.inputdata = DiracInputData(j.inputdata)
        if j.outputdata: dirac_script.outputdata = j.outputdata
        c.script = dirac_script
        
        return c

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
