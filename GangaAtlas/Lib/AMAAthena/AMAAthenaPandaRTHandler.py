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
import shutil
import commands
import re
import urllib
import gzip
import tarfile
import tempfile

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from GangaAtlas.Lib.Athena import dm_util

from GangaAtlas.Lib.ATLASDataset import DQ2OutputDataset

from GangaPanda.Lib.Athena.AthenaPandaRTHandler import AthenaPandaRTHandler

from GangaAtlas.Lib.AMAAthena.AMAAthenaCommon import *

def fake():
    import os
    for k,v in os.environ.items():
        print k,v

## append additional files in the user_area and re-make the gzipped tarball
def renew_userarea_tarball(app, extra_joptions=[]):

    ick = False

    def __check_and_add_joptions__(tar_fpath, jo_paths):

        tmpdir = tempfile.mkdtemp()
        
        jo_paths_update = {}

        ## exam the job option files in the tarball and generates a update list
        f_tar = None
        try:
            f_tar = tarfile.open(tar_fpath, mode='r')
            
            for jo_path in jo_paths:

                add_to_arch = False
                
                if not os.path.exists(jo_path):
                    raise ApplicationConfigurationError(None, 'job option file not found: %s' % jo_path)

                my_arcname = '%s/%s' % ( re.sub(r'\/$','', app.atlas_run_dir), os.path.basename(jo_path) )

                try:
                    t_info = f_tar.getmember( my_arcname )
                    f_tar.extract( t_info, tmpdir )

                    if dm_util.get_md5sum( jo_path ) != dm_util.get_md5sum( os.path.join(tmpdir, t_info.name) ):
                        add_to_arch = True

                except KeyError:
                    add_to_arch = True

                if add_to_arch:                    
                    jo_paths_update[jo_path] = my_arcname
        finally:
            if f_tar:
                f_tar.close()
            shutil.rmtree(tmpdir)

        ## re-open the tarball to append job option files in it
        f_tar = None
        try:
            f_tar = tarfile.open(tar_fpath, mode='a')

            for jo_path, arcname in jo_paths_update.items():
                logger.debug( 'add %s into user area tarball' % jo_path )
                f_tar.add( jo_path, arcname=arcname, recursive=False)

        finally:
            if f_tar:
                f_tar.close()

        return


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
            my_jo_paths = map( lambda x:x.name, app.option_file + map( lambda x:File(x), extra_joptions ) )

            try:
                __check_and_add_joptions__( tar_fpath, my_jo_paths )
                
            finally:

                ## zip the tarball again even the tarball update was failed
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

                ## remove the tar file in any case
                try:
                    shutil.rmtree( tar_fpath )
                except Exception:
                    pass

            ick = True

    return ick

class AMAAthenaPandaRTHandler(AthenaPandaRTHandler):
    '''AMAAthena Panda Runtime Handler'''

    def master_prepare(self,app,appconfig):
        '''Prepare the master job'''

        # PandaTools
        from pandatools import Client
        from taskbuffer.JobSpec import JobSpec
        from taskbuffer.FileSpec import FileSpec

        jspec = None

        ## add additional job option files and configuration files into the tarball
        logger.debug('re-packing Panda input sandbox')
        if not renew_userarea_tarball(app, extra_joptions=[]):
            logger.error('cannot recreate userarea tarball. run j.application.prepare() first')
            return None

        if app.atlas_exetype not in [ 'ATHENA' ]:
            raise ApplicationConfigurationError(None,"AMAAthena supports only ATHENA type executable. Set application.atlas_exetype = 'ATHENA' ")

        ## prepare the master job relying on the AthenaPandaRTHander
        jspec = AthenaPandaRTHandler.master_prepare(self, app, appconfig)

        return jspec


    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        '''prepare the subjob specific configuration'''

        # PandaTools
        from pandatools import Client
        from taskbuffer.JobSpec import JobSpec
        from taskbuffer.FileSpec import FileSpec

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
