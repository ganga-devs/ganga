'''SuperB setup of Application and RTHandler Ganga methods'''

import ConfigParser
import datetime
import os
import subprocess
import sys

from GangaCore.Core.exceptions import (GangaException,
                                   ApplicationConfigurationError)
from GangaCore.Core import FileWorkspace
from GangaCore.GPIDev.Adapters.IApplication import IApplication
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Schema import *
from GangaCore.GPIDev.Lib.File import File
from GangaCore.Lib.Executable import Executable
from GangaCore.Lib.LCG import LCG
from GangaCore.Lib.Mergers.Merger import TextMerger
from GangaCore.Utility.Config import *
import GangaCore.Utility.logging

import db
import SBDatasetManager
import SBInputDataset
import SBOutputDataset
import SBSubmission
import objectid
import utils


class SBApp(IApplication):
    '''This class manages the job submission environment and remote 
    worker node user executable interaction.'''
    
    _schema = Schema(Version(2, 0), {
        'executable' : SimpleItem(defvalue='',
                               typelist=['str'],
                               doc='Executable *relative* path after analysis \
                               software unpacking. eg: analysisExe.sh'),
        'software_dir' : SimpleItem(defvalue='', 
                                typelist=['str'], 
                                doc='Local absolute path to analysis \
                                software directory. eg: /home/user/software')
        } )
    _category = 'applications'
    _exportmethods = []
    _name = 'SBApp'
    
    def __init__(self):
        super(SBApp, self).__init__()
    
    def master_configure(self):
        '''This method creates the tar.bz2 archive of user sw directory.
        Such a method is called one time per master job'''
        
        logger.debug('SBApp master_configure called.')
        
        self.now = datetime.datetime.now().strftime("%Y%m%d")
        self.os_arch = os.environ['SBROOT'].split('/')[-1]
        self.user_id = utils.getOwner()
        
        j = self.getJobObject()
        
        # check the target SE status using gridmon DB (updated by nagios monitoring system)
        sql = 'SELECT se_host, nagios_test_service FROM se WHERE name_grid = %s'
        local_SE = db.gridmon(sql, (getConfig('SuperB')['submission_site'], ))
        if local_SE[0]['nagios_test_service'] == 'CRITICAL':
            raise GangaException('Local storage element %s is down.' % local_SE[0]['se_host'])
        #   logger.error('Local storage element %s seems died for gridmon.' % local_SE[0]['se_host'])
        #else:
        #    logger.error('Local storage element %s is back alive for gridmon. !! uncomment exception !!' % local_SE[0]['se_host'])
        
        # create the software directory
        if self.software_dir != '':
            if not os.path.isdir(self.software_dir):
                raise ApplicationConfigurationError('software_dir must be a directory.')
            
            # make the tar file and update sw_archive parameter
            self.software_dir = os.path.normpath(self.software_dir)
            (head, tail) = os.path.split(self.software_dir)
            self.filename = tail
            self.sw_archive = os.path.join(j.inputdir, tail + '.tar.bz2')
            
            logger.info('Creating archive: %s ...', self.sw_archive)
            logger.info('From: %s', head)
            logger.info('Of: %s', tail)
            
            #savedir = os.getcwd()
            #os.chdir(self.software_dir)
            
            #retcode = subprocess.call("tar -cjf %s * 2>/dev/null" % self.sw_archive, shell=True)
            retcode = subprocess.call("tar -cjf %s -C %s %s 2>/dev/null" % (self.sw_archive, head, tail), shell=True)
            if retcode < 0:
                raise ApplicationConfigurationError('Error %d while creating archive.' % retcode)
            
            #os.chdir(savedir)
        else:
            raise ApplicationConfigurationError('software_dir cannot be empty.')
        
        if self.executable == '':
            raise ApplicationConfigurationError('executable cannot be empty.')
        
        # checking that j.inputdata is a valid object
        if not isinstance(j.inputdata, (SBInputDataset.SBInputPersonalProduction,
                                        SBInputDataset.SBInputProductionAnalysis,
                                        SBInputDataset.SBInputPureAnalysis)):
            msg = 'j.inputdata %s is not allowed' % str(type(j.inputdata))
            raise ApplicationConfigurationError( msg)
        
        # checking that j.inputdata (the input dataset) is a valid dataset
        j.inputdata.check()
        
        # checking that j.outputdata (the output dataset) is valid
        if isinstance(j.outputdata, SBOutputDataset.SBOutputDataset):
            j.outputdata.check()
        
        # creating temp dataset
        self.temp_dataset = str(objectid.ObjectId())
        free_string = '%s_%s_%s' % (j.id, j.name, self.filename)
        
        sql = '''INSERT INTO analysis_dataset
            (owner, dataset_id, session, parameters, status)
            VALUES (%s, decode(%s, 'hex'), %s, %s, 'temp');
            
            INSERT INTO analysis_dataset_site
            (dataset_id, site)
            VALUES (decode(%s, 'hex'), %s);'''
        params = (utils.getOwner(), 
            self.temp_dataset, 
            'analysis', 
            {'free_string': free_string},
            self.temp_dataset,
            getConfig('SuperB')['submission_site'])
        db.write(sql, params)
        
        # merger
        j.merger = TextMerger()
        j.merger.files.extend(['severus.log', 'output_files.txt'])
        j.merger.ignorefailed = True
        j.merger.compress = True
        
        j.splitter = SBSubmission.SBSubmission()
        
        return (0, None)
    
    def configure(self, masterappconfig):
        '''It creates the SuperB job wrapper config file and manages application 
        string in Executable() method to be ready to launch the job wrapper.
        It also configures the input and output sandbox. Such a method is called one
         time per master job.'''
        
        logger.debug('SBApp configure called.')
        
        job = self._getParent()
        masterjob = job._getParent()
        
        # creating the configuration file needed by the job wrapper
        config = ConfigParser.RawConfigParser()
        
        config.add_section('REST')
        config.set('REST', 'PROXYVAR', 'X509_USER_PROXY')
        config.set('REST', 'NAME', 'restsbk5')
        config.set('REST', 'HOSTNAME', 'bbr-serv09.cr.cnaf.infn.it')
        config.set('REST', 'PORT', '8443')
        
        config.add_section('EXPORT_VARS')
        config.set('EXPORT_VARS', 'LCG_GFAL_INFOSYS', 'egee-bdii.cnaf.infn.it:2170')
        
        config.add_section('OPTIONS')
        config.set('OPTIONS', 'MODULENAME', 'Analysis')
        # setting PRODSERIES and REQUESTNAME as workaround (this parameters are useless for analysis)
        # TODO: fix asap modifying the job wrapper
        config.set('OPTIONS', 'PRODSERIES', '2011_Full_grid_test')
        config.set('OPTIONS', 'REQUESTNAME', '6b88b34759fab9e2d7e8efe73636dc35')
        config.set('OPTIONS', 'OS_ARCH', self.os_arch)
        
        config.add_section('SOFTWARE')
        config.set('SOFTWARE', 'EXEPATH', self.executable)
        (head, tail) = os.path.split(self.sw_archive)
        config.set('SOFTWARE', 'SWPATH', tail)
        
        config.add_section('INPUT')
        config.set('INPUT', 'INPUTMODE', job.inputdata.input_mode)
        if job.inputdata.input_mode == 'none':
            path = ''
        elif job.inputdata.input_mode == 'list':
            (head, path) = os.path.split(job.inputsandbox[0].name)
        elif job.inputdata.input_mode == 'dir':
            path = job.inputdata.input_path[0]
        else:
            raise ApplicationConfigurationError('input_mode not recognized.')
        config.set('INPUT', 'INPUTPATH', path)
        
        config.add_section('ANALYSIS')
        config.set('ANALYSIS', 'SUBJOB_ID', job.id)
        config.set('ANALYSIS', 'DATASET', self.temp_dataset)
        if isinstance(job.inputdata, SBInputDataset.SBInputPersonalProduction):
            config.set('ANALYSIS', 'TAG', job.inputdata.soft_version)
            config.set('ANALYSIS', 'SIMULATION', job.inputdata.session)
        
        config.add_section('SITE')
        config.set('SITE', 'USELOCALNAME', 'true')
        
        config.add_section('OUTPUT')
        # <identity_certificato>/<dataset_id>/<date_idjob_usertaballsenzaestensione_jobname>/output/subjobs.id_nomefile.root
        dirTree = ("%s/%%DATASET_ID%%/%s_%s_%s" % (self.user_id, self.now, masterjob.id, self.filename))
        if masterjob.name != '':
            dirTree += ("_%s" % masterjob.name)
        dirTree += '/output'
        config.set('OUTPUT', 'LFN', 'lfn:/grid/superbvo.org/analysis/' + dirTree)
        config.set('OUTPUT', 'OUTPUTPATH', dirTree)
        config.set('OUTPUT', 'LOG', '')
        
        config.add_section('OUTPUT_DATASET')
        if isinstance(job.outputdata, SBOutputDataset.SBOutputDataset):
            for key, value in job.outputdata.pairs.items():
                config.set('OUTPUT_DATASET', key, value)
        
        config.add_section('TARGETSITE')
        config.set('TARGETSITE', 'SITENAME', getConfig('SuperB')['submission_site'])
        config.set('TARGETSITE', 'USELOCALNAME', 'false')
        
        fileConf = os.path.join(job.inputdir, "file_%d.conf" % job.id)
        f = open(fileConf, "w")
        config.write(f)
        f.close()
        
        severus_dir = getConfig('SuperB')['severus_dir']
        severus = os.path.join(severus_dir, 'severus.tgz')
        severus_wrapper = os.path.join(severus_dir, 'severus_wrapper.sh')
        
        # updating the information of application, inputsandbox and outputsandbox  objects 
        # (the job wrapper, the configuration file, the user software, run number and 
        # verbosity flag as command line parameters, the output log and a list of output files)
        job.application = Executable()
        job.application.exe = File(severus_wrapper)
        # severus verbose (the string 'test' as third parameter)
        job.application.args = [File(fileConf), 10000000, 'test']
        # severus quiet (missing third parameter)
        #job.application.args = [File(fileConf), 10000000]
        job.inputsandbox.append(File(severus))
        job.inputsandbox.append(File(self.sw_archive))
        job.outputsandbox.append('severus.log')
        job.outputsandbox.append('output_files.txt')
        
        return (None, None)




def convertIntToStringArgs(args):
    result = []
    for arg in args:
        if isinstance(arg,int):
            result.append(str(arg))
        else:
            result.append(arg)
    return result

class RTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        logger.debug('RTHandler prepare called.')
        
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        return StandardJobConfig(app.exe,
                                 app._getParent().inputsandbox,
                                 convertIntToStringArgs(app.args),
                                 app._getParent().outputsandbox,
                                 app.env)


class LCGRTHandler(IRuntimeHandler):
    
    def master_prepare(self, app, appmasterconfig):
        #Get the string jdl requirement per site and set it up for ganga job
        
        logger.debug('LCGRTHandler master_prepare called.')
        j = app._getParent()
        
        if len(j.inputdata.run_site) == 0:
            j.inputdata.getDefaultRunSite()
        
        # check CEs status  of the chosen site using gridmon DB (updated by nagios monitoring system)
        sql = '''SELECT name_grid, 
                status_other, 
                banned 
            FROM site 
            WHERE false'''
        for site in j.inputdata.run_site:
            sql += ' OR name_grid = \'%s\' ' % site
        sql += 'ORDER BY name_grid'
        
        sites = db.gridmon(sql)
        
        for site in sites:
            if site['banned'] == True:
                logger.error('%s is not available due to technical problems.' % site['name_grid'])
                sites.remove(site)
            
            if site['status_other'] != 'OK':
                logger.error('%s is not available because of administrative policies.' % site['name_grid'])
                sites.remove(site)
        
        if len(sites) == 0:
            raise ApplicationConfigurationError('No sites available, try later again.')
        
        sql = '''SELECT ce_host
            FROM ce
            WHERE (nagios_test_service = 'OK' OR nagios_test_service = 'WARNING') AND (false'''
        for site in sites:
            sql += ' OR name_grid = \'%s\'' % site['name_grid']
        sql += ' ) ORDER BY name_grid, ce_host'
        
        ces = db.gridmon(sql)
        
        requirements = 'False'
        
        for ce in ces:
            requirements += ' || '
            requirements += '(other.GlueCEInfoHostName == "%s")' % ce['ce_host']
        
        # converting from unicode string to normal python string (why is it necessary?)
        requirements = str(requirements)
        
        # cleaning useless flags
        requirements = requirements.replace('False || ', '')
        
        # debugging print
        logger.debug('Requirements: %s' % requirements)
        
        # if all sites are down and/or requirements are not defined, stop submission
        if requirements == 'False':
            raise ApplicationConfigurationError('Requirements are empty.')
        
        j.backend.requirements.other += [requirements]
        
        # setting WALLTIME (if the user hasn't setted it before)
        if j.backend.requirements.walltime == 0:
            j.backend.requirements.walltime = 24 * 60 # dobbiamo fornire un valore in minuti
        
        #from GangaCore.Lib.LCG import LCGJobConfig
        #return LCGJobConfig(app.exe,app._getParent().inputsandbox,convertIntToStringArgs(app.args),app._getParent().outputsandbox,app.env)
    
    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        logger.debug('LCGRTHandler prepare called.')
        
        job = app._getParent()
        masterjob = job._getParent()
        
        job.backend.requirements.other = masterjob.backend.requirements.other
        job.backend.requirements.walltime = masterjob.backend.requirements.walltime
        
        from GangaCore.Lib.LCG import LCGJobConfig
        return LCGJobConfig(app.exe, 
                            app._getParent().inputsandbox, 
                            convertIntToStringArgs(app.args), 
                            app._getParent().outputsandbox, 
                            app.env)


class gLiteRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        logger.debug('gLiteRTHandler prepare called.')
        
        from GangaCore.Lib.gLite import gLiteJobConfig
        return gLiteJobConfig(app.exe,
                              app._getParent().inputsandbox,
                              convertIntToStringArgs(app.args),
                              app._getParent().outputsandbox,
                              app.env)


from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers


allHandlers.add('SBApp','LSF', RTHandler)
allHandlers.add('SBApp','Local', RTHandler)
allHandlers.add('SBApp','PBS', RTHandler)
allHandlers.add('SBApp','SGE', RTHandler)
allHandlers.add('SBApp','Condor', RTHandler)
allHandlers.add('SBApp','LCG', LCGRTHandler)
allHandlers.add('SBApp','gLite', gLiteRTHandler)
allHandlers.add('SBApp','TestSubmitter', RTHandler)
allHandlers.add('SBApp','Interactive', RTHandler)
allHandlers.add('SBApp','Batch', RTHandler)
allHandlers.add('SBApp','Cronus', RTHandler)
allHandlers.add('SBApp','Remote', LCGRTHandler)
allHandlers.add('SBApp','CREAM', LCGRTHandler)

logger = GangaCore.Utility.logging.getLogger()
