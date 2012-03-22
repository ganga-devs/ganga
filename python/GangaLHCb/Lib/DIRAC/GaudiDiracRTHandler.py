#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig
from DiracUtils import *
from DiracScript import *
from GangaLHCb.Lib.Gaudi.RTHUtils import *
from Ganga.GPIDev.Lib.File import FileBuffer, File
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *
from GangaLHCb.Lib.LHCbDataset.OutputData import OutputData
from GangaLHCb.Lib.Gaudi.GaudiJobConfig import GaudiJobConfig
import Ganga.Utility.Config

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

        job=app.getJobObject()
        ## catch errors from not preparing properly
        if not hasattr(app,'is_prepared') or app.is_prepared is None:
            logger.warning('Application is not prepared properly, ignoring inputdata, outputsandbox and outputdata defined in options file(s)')
            raise GangaException(None,'Application not prepared properly')

        
        
        inputsandbox=job.inputsandbox[:]
        outputsandbox=job.outputsandbox[:]
        outdata=OutputData()
        indata=LHCbDataset()

        if job.outputdata: outdata.files += job.outputdata.files
        #if job.outputdata: outdata=job.outputdata.files[:]
        
        
        ## Here add any sandbox files coming from the appmasterconfig
        ## currently none. catch the case where None is passes (as in tests)
        if appmasterconfig:            
            inputsandbox += appmasterconfig.getSandboxFiles()
            outputsandbox += appmasterconfig.outputbox
            indata = LHCbDataset(appmasterconfig.inputdata.files)
            outdata.files += appmasterconfig.outputdata.files
                                 
        ## Note EITHER the master inputsandbox OR the job.inputsandbox is added to
        ## the subjob inputsandbox depending if the jobmasterconfig object is present
        ## or not... Therefore combine the job.inputsandbox with appmasterconfig.
##         if ( indata.files and indata.hasLFNs() ) or ( job.inputdata and job.inputdata.hasLFNs() ):
##             xml_catalog_str = indata.getCatalog()
##             inputsandbox.append(FileBuffer('catalog.xml',xml_catalog_str))
            
     
##         outputsandbox=job.outputsandbox[:]
##         outdata=[]
##         indata=LHCbDataset()
##         if job.outputdata: outdata=job.outputdata.files[:]

##         ## catch errors from not preparing properly
##         if hasattr(app,'is_prepared') and app.is_prepared is not None:
##             import pickle
##             ## Pickup inputdata defined in the options file
##             f_idata = os.path.join(app.is_prepared.name,'inputdata.pkl')
##             if os.path.isfile(f_idata):
##                 file=open(f_idata,'rb')
##                 indata.files += [strToDataFile(name) for name in pickle.load(file)]
##                 file.close()
##             ## Pickup outputsandbox defined in the options file
##             f_osandbox = os.path.join(app.is_prepared.name,'outputsandbox.pkl')
##             if os.path.isfile(f_osandbox):
##                 file=open(f_osandbox,'rb')
##                 outputsandbox += pickle.load(file)
##                 file.close()
##             ## Pickup outputdata defined in the options file
##             f_odata = os.path.join(app.is_prepared.name,'outputdata.pkl')
##             if os.path.isfile(f_odata):
##                 file=open(f_odata,'rb')
##                 outdata += pickle.load(file)
##                 file.close()
##         else:
##             logger.warning('Application is not prepared properly, ignoring outputsandbox and outputdata defined in options file(s)')

##         ## Note EITHER the master inputsandbox OR the job.inputsandbox is added to
##         ## the subjob inputsandbox depending if the jobmasterconfig object is present
##         ## or not... Therefore combine the job.inputsandbox with appmasterconfig. Currently emtpy.
##         inputsandbox = app.getJobObject().inputsandbox[:]
##         if appmasterconfig: inputsandbox += appmasterconfig.getSandboxFiles()

        return GaudiJobConfig(inputbox=inputsandbox,
                              outputbox=outputsandbox,
                              outputdata=outdata,
                              inputdata=indata)

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):

        job=app.getJobObject()
    
        data_str=''
        if job.inputdata:
            data_str = job.inputdata.optionsString()
            if job.inputdata.hasLFNs():        
                cat_opts = '\nfrom Gaudi.Configuration import FileCatalog\nFileCatalog().Catalogs = ' \
                           '["xmlcatalog_file:pool_xml_catalog.xml"]\n'
                data_str += cat_opts
        elif jobmasterconfig.inputdata:
            data_str = jobmasterconfig.inputdata.optionsString()
            if jobmasterconfig.inputdata.hasLFNs():
                cat_opts = '\nfrom Gaudi.Configuration import FileCatalog\nFileCatalog().Catalogs = ' \
                           '["xmlcatalog_file:pool_xml_catalog.xml"]\n'
                data_str += cat_opts

        ## Unlike in the applications prepare method, buffers are created into
        ## files later on in job submission when put in inputsandbox which only
        ## accepts File objects.
        ## Additional as data.py could be created in OptionsFileSplitter,
        ## Need to add the existing data.py content to the end of this, if present.
        #OLD inputsandbox.append(FileBuffer('data.py',data_str).create())
##        existing_data = [file for file in inputsandbox if file.name is 'data.py']
##         if existing_data:
##             if len(existing_data) is not 1:
##                 logger.warning('Appear to have more than one data.py file in inputsandbox, contact ganga developers!')
##             if not isType(existing_data[0],File):
##                 logger.error('Found data.py in inputsandbox but not of type \'File\', contact ganga developers!')
##                 raise TypeMismatchError('Expected file data.py to be of type File.')
##             else:
##                 existing_data[0]._contents=data_str + existing_data[0].getContents()
##         else:
##             inputsandbox.append(FileBuffer(os.path.join(job.getInputWorkspace(),'data.py'),data_str))

        inputsandbox=[]
        
        def existingDataFilter(file):
            return file.name.find('/tmp/')>=0 and file.name.find('_data.py')>=0
        
        existingDataFile = filter(existingDataFilter,job.inputsandbox)
        
        if len(existingDataFile) is 1:
            # data_path = os.path.join(job.getInputWorkspace().getPath(),'data.py')
       # if os.path.isfile(data_path) and not os.path.islink(data_path):
            data_path = existingDataFile[0].name
            f=file(data_path,'r')
            existing_data = f.read()
            data_str+=existing_data
            f.close()
            del job.inputsandbox[job.inputsandbox.index(existingDataFile[0])]
            #os.remove(data_path)
        elif len(existingDataFile) is not 0:
            logger.error('There seems to be more than one existing data file in the inputsandbox!')
        inputsandbox.append(FileBuffer('data.py',data_str))

        ## Add the job.inputsandbox as splitters create subjobs that are
        ## seperate Job objects and therefore have their own job.inputsandbox
        ## which can be appended to in the splitters.
        inputsandbox += job.inputsandbox[:]

        ## Here add any sandbox files coming from the appsubconfig
        ## currently none.
        #sandbox = get_input_sandbox(app.extra)
        if appsubconfig: inputsandbox += appsubconfig.getSandboxFiles()

      

##         data_path = os.path.join(job.getInputWorkspace().getPath(),'data.py')
##         if os.path.isfile(data_path) and not os.path.islink(data_path):
##             f=file(data_path,'r')
##             existing_data = f.read()
##             data_str+=existing_data
##             f.close()
##             os.remove(data_path)
##         inputsandbox.append(FileBuffer(data_path,data_str))


        outputsandbox = job.outputsandbox[:]
        if jobmasterconfig: outputsandbox += jobmasterconfig.getOutputSandboxFiles()
        if appsubconfig: outputsandbox += appsubconfig.getOutputSandboxFiles()



        script = self._create_gaudi_script(app)

        #c = StandardJobConfig(script,sandbox,[],outputsandbox,None)

        dirac_script = DiracScript()
        dirac_script.job_type = 'LHCbJob()'
        dirac_script.exe = DiracApplication(app,script)
        dirac_script.platform = app.platform
        dirac_script.output_sandbox = outputsandbox

        if job.inputdata:
            dirac_script.inputdata = DiracInputData(job.inputdata)
        elif jobmasterconfig.inputdata.files:
            dirac_script.inputdata = DiracInputData(jobmasterconfig.inputdata)

        outputdata = OutputData()
        if jobmasterconfig: outputdata.files += jobmasterconfig.outputdata
        if job.outputdata:
            outputdata.files += job.outputdata
        dirac_script.outputdata = outputdata

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
