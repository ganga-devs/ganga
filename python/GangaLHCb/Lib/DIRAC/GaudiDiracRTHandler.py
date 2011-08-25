#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig
from DiracUtils import *
from DiracScript import *
from GangaLHCb.Lib.Gaudi.RTHUtils import *
from Ganga.GPIDev.Lib.File import FileBuffer, File
from GangaLHCb.Lib.LHCbDataset.OutputData import OutputData
from GangaLHCb.Lib.Gaudi.GaudiJobConfig import GaudiJobConfig

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def gaudi_dirac_wrapper(cmdline,platform):
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
    prependEnv('PYTHONPATH', getcwd() + '/InstallArea/python')
    prependEnv('PYTHONPATH', getcwd() + '/InstallArea/%s/python')
        
    sys.exit(system(%s)/256)
  """ % (platform,cmdline)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

gaudiSoftwareVersions = None

class GaudiDiracRTHandler(IRuntimeHandler):
    """The runtime handler to run Gaudi jobs on the Dirac backend"""

    def master_prepare(self,app,appmasterconfig):
        ## check version
        global gaudiSoftwareVersions
        if not gaudiSoftwareVersions:
            from Dirac import Dirac
            result = \
                   Dirac.execAPI('result=DiracCommands.getSoftwareVersions()')
            if not result_ok(result):
                logger.error('Could not obtain available versions: %s' \
                             % str(result))
                logger.error('Version/platform will not be validated.')
            else:
                gaudiSoftwareVersions = result['Value']
        if gaudiSoftwareVersions:
            if app.get_gaudi_appname() in gaudiSoftwareVersions:
                soft_info = gaudiSoftwareVersions[app.get_gaudi_appname()]
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

        import pickle
        file=open(os.path.join(app.is_prepared.name,'outputsandbox.pkl'),'rb')
        outputsandbox = pickle.load(file)
        file.close()
        ## Pickup outputdata defined in the options file
        file=open(os.path.join(app.is_prepared.name,'outputdata.pkl'),'rb')
        outdata = pickle.load(file)
        file.close()

        ## Note EITHER the master inputsandbox OR the job.inputsandbox is added to
        ## the subjob inputsandbox depending if the jobmasterconfig object is present
        ## or not... Therefore combine the job.inputsandbox with appmasterconfig. Currently emtpy.
        inputsandbox = app.getJobObject().inputsandbox[:]
        inputsandbox += appmasterconfig.getSandboxFiles()

        return GaudiJobConfig(inputbox=inputsandbox,outputbox=outputsandbox,outputdata=outdata)

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):

        job=app.getJobObject()
        inputsandbox=[]
        
        if job.inputdata:
            data_str = job.inputdata.optionsString()
            if job.inputdata.hasLFNs():        
                cat_opts = '\nfrom Gaudi.Configuration import FileCatalog\nFileCatalog().Catalogs = ' \
                           '["xmlcatalog_file:pool_xml_catalog.xml"]\n'
                data_str += cat_opts
            
            inputsandbox.append(FileBuffer('data.py',data_str).create())

        #sandbox = get_input_sandbox(app.extra)
        inputsandbox += appsubconfig.getSandboxFiles()

        outputsandbox = job.outputsandbox[:]
        outputsandbox += jobmasterconfig.getOutputSandboxFiles()
        outputsandbox += appsubconfig.getOutputSandboxFiles()



        script = self._create_gaudi_script(app)

        #c = StandardJobConfig(script,sandbox,[],outputsandbox,None)

        dirac_script = DiracScript()
        dirac_script.job_type = 'LHCbJob()'
        dirac_script.exe = DiracApplication(app,script)
        dirac_script.platform = app.platform
        dirac_script.output_sandbox = outputsandbox

        if job.inputdata:
            dirac_script.inputdata = DiracInputData(job.inputdata)

        outputdata = OutputData()
        outputdata.files += jobmasterconfig.outputdata
        if job.outputdata:
            dirac_script.outputdata = job.outputdata

        c = StandardJobConfig(script,inputbox=inputsandbox,outputbox=outputsandbox)
        c.script = dirac_script
        return c
        #return StandardJobConfig(dirac_script,inputbox=inputsandbox,outputbox=outputsandbox)

    def _create_gaudi_script(self,app):
        '''Creates the script that will be executed by DIRAC job. '''
        commandline = "'python ./gaudipython-wrapper.py'"
        if is_gaudi_child(app):
            commandline = '\"\"\"gaudirun.py '
            for arg in app.args:
                commandline+=arg+' '
            commandline+='options.pkl data.py\"\"\"'
        logger.debug('Command line: %s: ', commandline)
        wrapper = gaudi_dirac_wrapper(commandline,app.platform)
        j = app.getJobObject()
        script = "%s/gaudi-script.py" % j.getInputWorkspace().getPath()
        file = open(script,'w')
        file.write(wrapper)
        file.close()
        os.system('chmod +x %s' % script)
        return script

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
