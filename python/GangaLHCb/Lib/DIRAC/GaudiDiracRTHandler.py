#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
#from Ganga.Utility.Config import getConfig
from DiracUtils import *
from DiracScript import *
from GangaLHCb.Lib.RTHandlers.RTHUtils import *
from GangaLHCb.Lib.RTHandlers.RTHUtils import getXMLSummaryScript
from Ganga.GPIDev.Lib.File import FileBuffer, File
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *
from GangaLHCb.Lib.LHCbDataset.OutputData import OutputData
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename
#from GangaLHCb.Lib.Applications.GaudiJobConfig import GaudiJobConfig
from GangaGaudi.Lib.RTHandlers.GaudiRunTimeHandler import GaudiRunTimeHandler
import pickle
import Ganga.Utility.Config
from Ganga.Utility.util import unique
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def gaudi_dirac_wrapper(cmdline,platform):
    script = """#!/usr/bin/env python
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

    rc = system(%s)/256

    ###XMLSUMMARYPARSING###

    sys.exit(rc)
""" % (platform,cmdline)
    
    script = script.replace('###XMLSUMMARYPARSING###',getXMLSummaryScript())
    return script

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

gaudiSoftwareVersions = None

class GaudiDiracRTHandler(GaudiRunTimeHandler):
    """The runtime handler to run Gaudi jobs on the Dirac backend"""

    def _additional_master_prepare(self,
                                   app,
                                   appmasterconfig,
                                   inputsandbox,
                                   outputsandbox):
        config = Ganga.Utility.Config.getConfig('LHCb')
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
        if gaudiSoftwareVersions and (not config['ignore_version_check']):
            if app.appname in gaudiSoftwareVersions:
                soft_info = gaudiSoftwareVersions[app.appname]
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

        job=app.getJobObject()

        
        share_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                  'shared',
                                  getConfig('Configuration')['user'],
                                  app.is_prepared.name,
                                  'output',
                                  'options_parser.pkl')

        outdata=[]
        if os.path.exists(share_path):
#        if not os.path.exists(share_path):
           # raise GangaException('could not find the parser')
           f=open(share_path,'r+b')
           parser = pickle.load(f)
           f.close()

           outbox, outdata = parser.get_output(job)
           outputsandbox += outbox[:]          

        if job.outputdata: outdata += job.outputdata.files
                                 
        ## Note EITHER the master inputsandbox OR the job.inputsandbox is added to
        ## the subjob inputsandbox depending if the jobmasterconfig object is present
        ## or not... Therefore combine the job.inputsandbox with appmasterconfig.


        # add summary.xml
        outputsandbox += ['summary.xml','__parsedxmlsummary__']

        r = StandardJobConfig(inputbox   = unique(inputsandbox ),
                              outputbox  = unique(outputsandbox) )

        r.outputdata = unique(outdata)
        
        return r          

##         return GaudiJobConfig(inputbox   = unique(inputsandbox ),
##                               outputbox  = unique(outputsandbox),
##                               outputdata = outdata)

    def _additional_prepare(self,
                            app,
                            appsubconfig,
                            appmasterconfig,
                            jobmasterconfig,
                            inputsandbox,
                            outputsandbox):

        job=app.getJobObject()

        indata = job.inputdata
        ## splitters ensure that subjobs pick up inputdata from job over that in optsfiles
        ## but need to take sare of unsplit jobs
        if not job.master:
            share_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                      'shared',
                                      getConfig('Configuration')['user'],
                                      app.is_prepared.name,
                                      'inputdata',
                                      'options_data.pkl')

            if not indata:
                if os.path.exists(share_path):
                    f=open(share_path,'r+b')
                    indata = pickle.load(f)
                    f.close()
        

        data_str=''
        if indata:
            data_str = indata.optionsString()
            if indata.hasLFNs():        
                cat_opts = '\nfrom Gaudi.Configuration import FileCatalog\nFileCatalog().Catalogs = ' \
                           '["xmlcatalog_file:pool_xml_catalog.xml"]\n'
                data_str += cat_opts

        if hasattr(job,'_splitter_data'):
            data_str += job._splitter_data
        inputsandbox.append(FileBuffer('data.py',data_str))


        script = self._create_gaudi_script(app)

        dirac_script = DiracScript()
        dirac_script.job_type = 'LHCbJob()'
        dirac_script.exe = DiracApplication(app,script)
        dirac_script.platform = app.platform
        dirac_script.output_sandbox = outputsandbox

        if indata:
            dirac_script.inputdata = DiracInputData(indata)

        outputdata = OutputData()
        if jobmasterconfig: outputdata.files += jobmasterconfig.outputdata

        dirac_script.outputdata = outputdata

        c = StandardJobConfig( script,
                               inputbox  = unique(inputsandbox ),
                               outputbox = unique(outputsandbox) )
        c.script = dirac_script

        return c


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
        script = os.path.join(j.getInputWorkspace().getPath(),"gaudi-script.py")
        file = open(script,'w')
        file.write(wrapper)
        file.close()
        os.system('chmod +x %s' % script)
        return script

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
