###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AMAAthenaPandaRTHandler.py,v 1.17 2009/01/29 17:22:27 dvanders Exp $
###############################################################################
# AMAAthena Panda Runtime Handler
#
# ATLAS/ARDA

import os
import os.path
import commands
import re
import urllib
import gzip
import tarfile
import tempfile

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from GangaAtlas.Lib.ATLASDataset import DQ2OutputDataset

from GangaPanda.Lib.Athena.AthenaPandaRTHandler import AthenaPandaRTHandler

# PandaTools
from pandatools import Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

## system command executor with subprocess
def execSyscmdSubprocess(cmd, wdir=os.getcwd()):

    import subprocess

    exitcode = -999

    mystdout = ''
    mystderr = ''

    try:

        ## resetting essential env. variables
        my_env = os.environ

        my_env['LD_LIBRARY_PATH'] = ''
        my_env['PATH'] = ''
        my_env['PYTHONPATH'] = ''

        if my_env.has_key('LD_LIBRARY_PATH_ORIG'):
            my_env['LD_LIBRARY_PATH'] = my_env['LD_LIBRARY_PATH_ORIG']

        if my_env.has_key('PATH_ORIG'):
            my_env['PATH'] = my_env['PATH_ORIG']

        if my_env.has_key('PYTHONPATH_ORIG'):
            my_env['PYTHONPATH'] = my_env['PYTHONPATH_ORIG']

        child = subprocess.Popen(cmd, cwd=wdir, env=my_env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        (mystdout, mystderr) = child.communicate()

        exitcode = child.returncode

    finally:
        pass

    return (exitcode, mystdout, mystderr)

def ama_convert_joption(config_file, option_file, flags):
    '''converts ama configuration to job option by using the AMAUtilsTool module
    '''

    #import imp
    
    #- backup sys.path
    #sys_path_org = sys.path

    #- extend sys.path (with an expectation that the PYTHONPATH contains
    #-                  the path to AMAAthena.AMAUtilsTool module from AMAAthena)
    #sys.path = os.environ['PYTHONPATH'].split(':') + sys.path

    #print sys.path

    #fp_1 = None
    #fp_2 = None

    try:

        #fp_1, pathname, description = imp.find_module('AMAAthena')

        #if pathname:
            #sys.path = [ pathname ] + sys.path
            #fp_2, pathname, description = imp.find_module('AMAUtilsTool', [ pathname ])
            #print pathname
        
        #if imp.load_module('AMAAthena.AMAUtilsTool', fp_2, pathname, description):

        #    print sys.modules['AMAAthena.AMAUtilsTool']

            #- load AMAUtilsTool
            #from GangaAtlas.Lib.AMAAthena.AMAUtilsTool import AMAUtilsTool
            #from AMAAthena.AMAUtilsTool import AMAUtilsTool
            #amatool = AMAUtilsTool()
            #amatool.CreateJobOptions(config_file, option_file, flags)

        #else:
        #    raise ApplicationConfigurationError(None, 'Cannot find AMAAthena.AMAUtilsTool module')

        ## NOTE: the AMAUtilsTool module needs to be synchronized with the version from AMAAthena
        from GangaAtlas.Lib.AMAAthena.AMAUtilsTool import AMAUtilsTool
        amatool = AMAUtilsTool()
        amatool.CreateJobOptions(config_file, option_file, flags)

    finally:

        #if fp_1:
        #    fp_1.close()

        #if fp_2:
        #    fp_2.close()

        #- restore sys.path
        #sys.path = sys_path_org

        pass

    #print sys.path

    if os.path.exists(option_file):
        return True
    else:
        return False

def fake():
    import os
    for k,v in os.environ.items():
        print k,v

def ama_convert_joption_exec(config_file, option_file, flags):
    '''converts ama configuration to job option by calling an external helper script'''

    isDone = False

    os.environ['LD_LIBRARY_PATH_ORIG'] = os.environ['LD_LIBRARY_PATH']
    os.environ['PYTHONPATH_ORIG'] = os.environ['PYTHONPATH']
    os.environ['PATH_ORIG'] = os.environ['PATH']

    cmd  = 'AMAConfigfileConverter '
    cmd += config_file + ' ' + option_file + ' ' + ' '.join(flags)

    logger.debug(cmd)

    (exitcode, myout, myerr) = execSyscmdSubprocess(cmd)

    if exitcode == 0 and os.path.exists(option_file):
        isDone = True
    else:
        logger.debug(myout)
        logger.error(myerr)
        isDone = False

    return isDone

def get_sample_name(job):
    
    if job.name:
        return job.name
    else:
        return 'NoSampleName'

## summary file
def get_summary_lfn(job):

    #sample_name = get_sample_name(job)

    #max_events = -1
    #if job.application.max_events:
    #    max_events = job.application.max_events

    #conf_name = os.path.basename(job.application.driver_config.config_file.name)

    #flags = re.split('[\s|:]+', job.application.driver_flags)

    #try:
    #    flags.remove('')
    #except ValueError:
    #    pass

    #flag_tag = '_'.join( map(lambda x:x.replace('=',''), flags) )

    ## REMARK: the max_events and flags are not passed to AMA as they are already converted into a normal Athena job option
    ##         it needs to be optimized!!
    #summary_lfn = 'summary/summary_%s__nEvts_-1.root' % sample_name
    #summary_lfn = 'workDir/%s/summary/summary_%s__nEvts_-1.root' % (re.sub(r'\/$','', job.application.atlas_run_dir),  sample_name)

    #summary_lfn = 'ama_summary_%s.tgz' % job.getFQID('_')
    summary_lfn = 'ama_summary.tgz'

    #if not flag_tag:
    #    #summary_lfn = 'summary/summary_%s_confFile_%s_nEvts_%s.root' % ( sample_name, conf_name, str(max_events) )
    #    summary_lfn = 'workDir/summary/summary_%s__nEvts_%s.root' % ( sample_name, str(max_events) )
    #else:
    #    #summary_lfn = 'summary/summary_%s_confFile_%s_%s_nEvts_%s.root' % ( sample_name, conf_name, flag_tag, str(max_events) )
    #    summary_lfn = 'workDir/summary/summary_%s__%s_nEvts_%s.root' % ( sample_name, flag_tag, str(max_events) )

    return summary_lfn

## append additional files in the user_area and re-make the gzipped tarball
def renew_userarea_tarball(app, ama_config_opt):

    ick = False

    if app.user_area and os.path.exists(app.user_area.name) and tarfile.is_tarfile(app.user_area.name):

        re_gzipfile = re.compile('(.*)\.gz$')
        match = re_gzipfile.match(app.user_area.name)

        if ( match ):

            ## unzip the tarball
            tar_fpath = match.group(1)

            zip_file = gzip.open( app.user_area.name, 'rb' )
            tar_file = open( tar_fpath, 'wb' )
            while True:
                d = zip_file.read(8096)

                if not d:
                    break
                else:
                    tar_file.write(d)

            tar_file.close()
            zip_file.close()

            ## modify the tarball
            f_tar = tarfile.open(tar_fpath, mode='a')

            tar_member_names = f_tar.getnames()

            # append more files in tarball
            #  - option files
            #  - external ama option files converted from AMA config
            my_opt = app.option_file + [ File(ama_config_opt) ]
            for opt in my_opt:
                if os.path.exists( opt.name ):

                    my_arcname = '%s/%s' % ( re.sub(r'\/$','', app.atlas_run_dir), os.path.basename(opt.name) )

                    ## add the file only when the member wasn't included
                    if tar_member_names.count(my_arcname) == 0:
                        f_tar.add( opt.name, arcname=my_arcname, recursive=False )

            f_tar.close()

            ## zip the tarball again
            zip_file = gzip.open( app.user_area.name, 'wb' )
            tar_file = open( tar_fpath, 'rb' )
            while True:
                d = tar_file.read(8096)

                if not d:
                    break
                else:
                    zip_file.write(d)

            zip_file.close()
            tar_file.close()

            ick = True

    return ick

class AMAAthenaPandaRTHandler(AthenaPandaRTHandler):
    '''AMAAthena Panda Runtime Handler'''

    def master_prepare(self,app,appconfig):
        '''Prepare the master job'''

        jspec = None

        job = app._getParent()

        ## converting AMA config file into a job option and append it into app.job_option
        logger.debug('converting AMA config file to job option ...')

        ama_config_file = app.driver_config.config_file
        ama_flags       = re.split('[\s|:]+', app.driver_flags)
        ama_sample_name = job.name

        if not ama_sample_name:
            ama_sample_name = 'NoSampleName'

        ## remove empty flags
        try:
            ama_flags.remove('')
        except ValueError:
            pass

        ama_config_opttmp  = tempfile.mktemp(suffix='ama_config_joption')
        ama_config_optfile = os.path.join(job.inputdir, 'ama_config_joption.py')

        logger.debug('converting AMA configuration to Athena job option file')
        
        #if ama_convert_joption(ama_config_file.name, ama_config_opttmp, ama_flags):
        if ama_convert_joption_exec(ama_config_file.name, ama_config_opttmp, ama_flags):
            
            if os.path.exists( ama_config_opttmp ):

                try:
                    ## modify the ama_config_opttmp and create the final ama_config_optfile in job's workdir
                    f = open( ama_config_optfile , 'w' )
                    f.write( 'SampleName = \'%s\'\n' % ama_sample_name )
                    f.write( 'ConfigFile = \'%s\'\n' % os.path.basename(ama_config_file.name) )
                    f.write( 'FlagList = \'%s\'\n' % ' '.join(ama_flags) )
                    f.write( 'EvtMax = %d\n' % app.max_events )

                    ft = open( ama_config_opttmp, 'r' )

                    while True:
                        d = ft.read( 8096 )

                        if not d:
                            break
                        else:
                            f.write(d)

                    ft.close()
                    f.close()

                    ## add additional job option files and configuration files into the tarball
                    logger.debug('re-packing Panda input sandbox')
                    if not renew_userarea_tarball(app, ama_config_optfile):
                        logger.error('cannot recreate userarea tarball. run j.application.prepare() first')
                        return None

                    if app.atlas_exetype not in [ 'ATHENA' ]:
                        raise ApplicationConfigurationError(None,"AMAAthena supports only ATHENA type executable. Set application.atlas_exetype = 'ATHENA' ")

                    ## prepare the master job relying on the AthenaPandaRTHander
                    jspec = AthenaPandaRTHandler.master_prepare(self, app, appconfig)

                    ## update the job options after the master_prepare
                    self.job_options += ' ' + os.path.basename( ama_config_optfile )

                finally:
                    os.remove( ama_config_opttmp )

        else:
            logger.error('Fail to convert AMA configuration to jobOptions')

        return jspec

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        '''prepare the subjob specific configuration'''

        job = app._getParent()
        logger.debug('AMAAthenaPandaRTHandler prepare called for %s', job.getFQID('.'))

#       in case of a simple job get the dataset content, otherwise subjobs are filled by the splitter
        if job.inputdata and not job._getRoot().subjobs:
            if not job.inputdata.names:
                for guid, lfn in job.inputdata.get_contents():
                    job.inputdata.guids.append(guid)
                    job.inputdata.names.append(lfn)

        site = job._getRoot().backend.site
        job.backend.site = site
        job.backend.actualCE = site
        cloud = job._getRoot().backend.requirements.cloud
        job.backend.requirements.cloud = cloud

#       if no outputdata are given
        if not job.outputdata:
            job.outputdata = DQ2OutputDataset()
            job.outputdata.datasetname = job._getRoot().outputdata.datasetname

        if not job.outputdata.datasetname:
            job.outputdata.datasetname = job._getRoot().outputdata.datasetname

        if not job.outputdata.datasetname:
            raise ApplicationConfigurationError(None,'DQ2OutputDataset has no datasetname')

        cacheVer = ''
        if app.atlas_project and app.atlas_production:
            cacheVer = "-" + app.atlas_project + "_" + app.atlas_production

        jspec = JobSpec()
        jspec.jobDefinitionID   = job._getRoot().id
        jspec.jobName           = commands.getoutput('uuidgen')
        jspec.AtlasRelease      = 'Atlas-%s' % app.atlas_release
        jspec.homepackage       = 'AnalysisTransforms'+cacheVer#+nightVer
        jspec.transformation    = '%s/runAthena-00-00-11' % Client.baseURLSUB
        if job.inputdata:
            jspec.prodDBlock    = job.inputdata.dataset[0]
        else:
            jspec.prodDBlock    = 'NULL'
        jspec.destinationDBlock = job.outputdata.datasetname
        if job.outputdata.location:
            if not job._getRoot().subjobs or job.id == 0:
                logger.warning('You have specified outputdata.location. Note that Panda may not support writing to a user-defined output location.')
            jspec.destinationSE = job.outputdata.location
        else:
            jspec.destinationSE = site
        jspec.prodSourceLabel   = 'user'
        jspec.assignedPriority  = 1000
        jspec.cloud             = cloud
        # memory
        if job.backend.requirements.memory != -1:
            jspec.minRamCount = job.backend.requirements.memory
        jspec.computingSite     = site

#       library (source files)
        flib = FileSpec()
        flib.lfn            = self.library
#        flib.GUID           =
        flib.type           = 'input'
#        flib.status         =
        flib.dataset        = self.libDataset
        flib.dispatchDBlock = self.libDataset
        jspec.addFile(flib)

#       input files FIXME: many more input types
        for guid, lfn in zip(job.inputdata.guids,job.inputdata.names):
            finp = FileSpec()
            finp.lfn            = lfn
            finp.GUID           = guid
#            finp.fsize =
#            finp.md5sum =
            finp.dataset        = job.inputdata.dataset[0]
            finp.prodDBlock     = job.inputdata.dataset[0]
            finp.dispatchDBlock = job.inputdata.dataset[0]
            finp.type           = 'input'
            finp.status         = 'ready'
            jspec.addFile(finp)

#       output files
        outMap = {}

        my_extOut = self.extOutFile + [ get_summary_lfn(job) ]

        if my_extOut:

            self.indexEXT += 1
            for idx, name in enumerate( my_extOut ):
                fout = FileSpec()
                fout.dataset           = job.outputdata.datasetname
                fout.lfn               = '%s.EXT%d._%05d.%s' % (job.outputdata.datasetname,idx,self.indexEXT,name)
                fout.type              = 'output'
                fout.destinationDBlock = jspec.destinationDBlock
                fout.destinationSE     = jspec.destinationSE
                jspec.addFile(fout)
                if not 'IROOT' in outMap:  # this is not a typo!
                    outMap['IROOT'] = []
                outMap['IROOT'].append((name,fout.lfn))

        #if self.runConfig.output.outNtuple:
        #    self.indexNT += 1
        #    for name in self.runConfig.output.outNtuple:
        #        fout = FileSpec()
        #        fout.dataset           = job.outputdata.datasetname
        #        fout.lfn               = '%s.%s._%05d.root' % (job.outputdata.datasetname,name,self.indexNT)
        #        fout.type              = 'output'
        #        fout.destinationDBlock = jspec.destinationDBlock
        #        fout.destinationSE    = jspec.destinationSE
        #        jspec.addFile(fout)
        #        if not 'ntuple' in outMap:
        #            outMap['ntuple'] = []
        #        outMap['ntuple'].append((name,fout.lfn))

        #if self.runConfig.output.outHist:
        #    self.indexHIST += 1
        #    fout = FileSpec()
        #    fout.dataset           = job.outputdata.datasetname
        #    fout.lfn               = '%s.hist._%05d.root' % (job.outputdata.datasetname,self.indexHIST)
        #    fout.type              = 'output'
        #    fout.destinationDBlock = jspec.destinationDBlock
        #    fout.destinationSE     = jspec.destinationSE
        #    jspec.addFile(fout)
        #    outMap['hist'] = fout.lfn

        #FIXME: if options.outMeta != []:

#       log files

        flog = FileSpec()
        flog.lfn = '%s._$PANDAID.log.tgz' % job.outputdata.datasetname
        flog.type = 'log'
        flog.dataset           = job.outputdata.datasetname
        flog.destinationDBlock = job.outputdata.datasetname
        flog.destinationSE     = job.backend.site
        jspec.addFile(flog)

#       job parameters
        param = ''
        # FIXME if not options.nobuild:
        param =  '-l %s ' % self.library
        param += '-r %s ' % self.rundirectory
        # set jobO parameter
        param += '-j "%s" ' % urllib.quote(self.job_options)
        # DBRelease
        if app.atlas_dbrelease != '':
            tmpItems = app.atlas_dbrelease.split(':')
            tmpDbrDS  = tmpItems[0]
            tmpDbrLFN = tmpItems[1]
            # instantiate  FileSpec
            fileName = tmpDbrLFN
            vals     = self.dbrFiles[tmpDbrLFN]
            file = FileSpec()
            file.lfn            = fileName
            file.GUID           = vals['guid']
            file.fsize          = vals['fsize']
            file.md5sum         = vals['md5sum']
            file.dataset        = tmpDbrDS
            file.prodDBlock     = tmpDbrDS
            file.dispatchDBlock = tmpDbrDS
            file.type       = 'input'
            file.status     = 'ready'
            jspec.addFile(file)
            # set DBRelease parameter
            param += '--dbrFile %s ' % file.lfn
            
        param += '-i "%s" ' % job.inputdata.names
        param += '-m "[]" ' #%minList FIXME
        param += '-n "[]" ' #%cavList FIXME
        #FIXME
        #if bhaloList != []:
        #    param += '--beamHalo "%s" ' % bhaloList
        #if bgasList != []:
        #    param += '--beamGas "%s" ' % bgasList
        param += '-o "%s" ' % outMap
        if self.runConfig.input.inColl:
            param += '-c '
        if self.runConfig.input.inBS:
            param += '-b '
        if self.runConfig.input.backNavi:
            param += '-e '
        #if self.config['shipinput']:
        #    param += '--shipInput '
        #FIXME options.rndmStream
        nEventsToSkip = 0
        if app.max_events > 0:
            param += '-f "theApp.EvtMax=%d;EventSelector.SkipEvents=%s" ' % (app.max_events,nEventsToSkip)
        # addPoolFC
        #if self.config['addPoolFC'] != "":
        #    param += '--addPoolFC %s ' % self.config['addPoolFC']
        # use corruption checker
        if job.backend.requirements.corCheck:
            param += '--corCheck '
        # disable to skip missing files
        if job.backend.requirements.notSkipMissing:
            param += '--notSkipMissing '
        # given PFN
        #if self.config['pfnList'] != '':
        #    param += '--givenPFN '
        # create symlink for MC data
        #if self.config['mcData'] != '':
        #    param += '--mcData %s ' % self.config['mcData']
        # source URL
        matchURL = re.search("(http.*://[^/]+)/",Client.baseURLSSL)
        if matchURL != None:
            param += " --sourceURL %s " % matchURL.group(1)
        # use ARA
        #if job.backend.ares:
        #    job.backend.ara = True
        #if job.backend.ara:
        #    param += '--trf '
        #    param += '--ara '

        jspec.jobParameters = param

        return jspec

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('AMAAthena','Panda',AMAAthenaPandaRTHandler)

from Ganga.Utility.Config import getConfig
config = getConfig('Athena')
configDQ2 = getConfig('DQ2')

from Ganga.Utility.logging import getLogger
logger = getLogger()
