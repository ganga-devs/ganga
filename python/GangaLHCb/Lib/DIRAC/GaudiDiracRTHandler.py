#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig
from DiracUtils import *
from DiracScript import *
from GangaLHCb.Lib.Gaudi.RTHUtils import *
from Ganga.GPIDev.Lib.File import FileBuffer, File

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def gaudi_dirac_wrapper(cmdline):
    return """#!/usr/bin/env python
'''Script to run Gaudi application'''

from os import curdir, system, environ, pathsep, sep, getcwd
from os.path import join
import sys

def prependEnv(key, value):
    if environ.has_key(key): value += (pathsep + environ[key])
    environ[key] = value

# Main
if __name__ == '__main__':

    prependEnv('LD_LIBRARY_PATH', getcwd() + '/lib')
    prependEnv('PYTHONPATH', getcwd() + '/python')
        
    sys.exit(system(%s)/256)
  """ % cmdline

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiDiracRTHandler(IRuntimeHandler):
    """The runtime handler to run Gaudi jobs on the Dirac backend"""

    def master_prepare(self,app,appconfig):
        # check version
        result = Dirac.execAPI('result = DiracCommands.getSoftwareVersions()')
        if not result_ok(result):
            logger.error('Could not obtain available versions: %s' \
                         % str(result))
            logger.error('Version/platform will not be validated.')
        else:
            soft_info = result['Value'][app.get_gaudi_appname()]
            if not app.version in soft_info:
                versions = []
                for v in soft_info: versions.append(v)
                msg = 'Invalid version: %s.  Valid versions: %s' \
                      % (app.version, str(versions))
                raise ApplicationConfigurationError(None,msg)
            platforms = soft_info[app.version]
            if not app.platform in platforms:
                msg = 'Invalid platform: %s. Valid platforms: %s' % \
                      (app.platform,str(platforms))
                raise ApplicationConfigurationError(None,msg)
        sandbox = get_master_input_sandbox(app.getJobObject(),app.extra) 
        c = StandardJobConfig('',sandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        if app.extra.inputdata and app.extra.inputdata.hasLFNs():        
            cat_opts = '\nFileCatalog().Catalogs = ' \
                       '["xmlcatalog_file:pool_xml_catalog.xml"]\n'
            app.extra.input_buffers['data.py'] += cat_opts

        script = self._create_gaudi_script(app)
        sandbox = get_input_sandbox(app.extra)
        outputsandbox = app.extra.outputsandbox 
        c = StandardJobConfig(script,sandbox,[],outputsandbox,None)

        dirac_script = DiracScript()
        dirac_script.job_type = 'LHCbJob()'
        dirac_script.exe = DiracApplication(app,script)
        dirac_script.platform = app.platform
        dirac_script.output_sandbox = outputsandbox

        if app.extra.inputdata:
            dirac_script.inputdata = DiracInputData(app.extra.inputdata)
          
        if app.extra.outputdata:
            dirac_script.outputdata = app.extra.outputdata

        c.script = dirac_script        
        return c

    def _create_gaudi_script(self,app):
        '''Creates the script that will be executed by DIRAC job. '''
        commandline = "'python ./gaudipython-wrapper.py'"
        if is_gaudi_child(app):
            commandline = "'gaudirun.py options.pkl data.py'"
        logger.debug('Command line: %s: ', commandline)
        wrapper = gaudi_dirac_wrapper(commandline)
        j = app.getJobObject()
        script = "%s/gaudi-script.py" % j.getInputWorkspace().getPath()
        file = open(script,'w')
        file.write(wrapper)
        file.close()
        os.system('chmod +x %s' % script)
        return script

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
