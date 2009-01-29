###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaPandaRTHandler.py,v 1.15 2009-01-29 14:14:05 dvanders Exp $
###############################################################################
# Athena LCG Runtime Handler
#
# ATLAS/ARDA

import os, sys, pwd, commands, re, shutil, urllib, time, string 

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from GangaAtlas.Lib.ATLASDataset import DQ2Dataset, DQ2OutputDataset

from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

# PandaTools
import Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
import AthenaUtils


def getDBDatasets(jobO,trf,dbRelease):
    # get DB datasets
    dbrFiles  = {}
    dbrDsList = []
    if trf or dbRelease != '':
        if trf:
            # parse jobO for TRF
            tmpItems = jobO.split()
        else:
            # mimic a trf parameter to reuse following algorithm
            tmpItems = ['%DB='+dbRelease]
        # look for DBRelease
        for tmpItem in tmpItems:
            match = re.search('%DB=([^:]+):(.+)$',tmpItem)
            if match:
                tmpDbrDS  = match.group(1)
                tmpDbrLFN = match.group(2)
                # get files in the dataset
                if not tmpDbrDS in dbrDsList:
                    logger.info("Querying files in dataset:%s" % tmpDbrDS)
                    try:
                        tmpList = Client.queryFilesInDataset(tmpDbrDS,False)
                    except:
                        raise ApplicationConfigurationError(None,"ERROR : error while looking up dataset %s. Perhaps this dataset does not exist?"%tmpDbrDS)
                    # append
                    for tmpLFN,tmpVal in tmpList.iteritems():
                        dbrFiles[tmpLFN] = tmpVal
                    dbrDsList.append(tmpDbrDS)
                # check
                if not dbrFiles.has_key(tmpDbrLFN):
                    raise ApplicationConfigurationError(None,"ERROR : %s is not in %s"%(tmpDbrLFN,tmpDbrDS))
    return dbrFiles,dbrDsList


class AthenaPandaRTHandler(IRuntimeHandler):
    '''Athena Panda Runtime Handler'''


    def master_prepare(self,app,appconfig):
        '''Prepare the master job'''

        job = app._getParent()
        logger.debug('AthenaPandaRTHandler master_prepare called for %s', job.getFQID('.')) 

        usertag = configDQ2['usertag'] 
        self.libDataset = '%s.%s.ganga.%s_%d.lib._%06d' % (usertag,gridProxy.identity(),commands.getoutput('hostname').split('.')[0],int(time.time()),job.id)
        sources = 'sources.%s.tar.gz' % commands.getoutput('uuidgen') 
        self.library = '%s.lib.tgz' % self.libDataset

        # validate parameters
        # check DBRelease
        if job.backend.dbRelease != '' and job.backend.dbRelease.find(':') == -1:
            raise ApplicationConfigurationError(None,"ERROR : invalid argument for backend.dbRelease. Must be 'DatasetName:FileName'")
         
#       parse job options file
        if job.backend.ares:
            job.backend.ara = True
        if not job.backend.ara:
            logger.info('Parsing job options file ...')
        job_option_files = ' '.join([ opt_file.name for opt_file in app.option_file ])
        upperSupStream = [s.upper() for s in job.backend.supStream]
        shipInput = False
        trf = job.backend.ara
        ret, self.runConfig = AthenaUtils.extractRunConfig(job_option_files,upperSupStream,job.backend.useAIDA,shipInput,trf)
        if not ret:
            raise ApplicationConfigurationError(None,"ERROR: Unable to extract run configuration")
        if not job.backend.ara:
            logger.info('Detected runConfig: %s'%self.runConfig)

#       unpack library
        logger.debug('Creating source tarball ...')        
        tmpdir = '/tmp/%s' % commands.getoutput('uuidgen')
        os.mkdir(tmpdir)

        if not job.backend.ara:
            if not app.user_area.name:
                raise ApplicationConfigurationError(None,'app.user_area.name is null')

            rc, output = commands.getstatusoutput('tar xzf %s -C %s' % (app.user_area.name,tmpdir))
            if rc:
                logger.error('Unpacking user area failed with status %d.',rc)
                logger.error(output)
                raise ApplicationConfigurationError(None,'Unpacking user area failed.')

            rc, output = commands.getstatusoutput('find %s -name run -printf "%%P\n"' % tmpdir)
            if rc:
                logger.error('Finding run directory failed with status %d',rc)
                logger.error(output)
                raise ApplicationConfigurationError(None,'Finding run directory failed.')

            lines = output.splitlines()
            if not lines:
                logger.error('No run directory found.')
                raise ApplicationConfigurationError(None,'No run directory found.')
            if len(lines)>1:
                logger.error('More then one run directory found.')
                raise ApplicationConfigurationError(None,'More then one run directory found.')
            self.rundirectory = lines[0]
        else:
            ret,retval=AthenaUtils.getAthenaVer()
            currentDir = os.path.realpath(os.getcwd())
            workArea = retval['workArea']
            
            sString=re.sub('[\+]','.',workArea)
            runDir = re.sub('^%s' % sString, '', currentDir)
            if runDir == currentDir:
                print "ERROR : you need to run pathena in a directory under %s" % workArea
                sys.exit(EC_Config)
            elif runDir == '':
                runDir = '.'
            elif runDir.startswith('/'):
                runDir = runDir[1:]
            runDir = runDir+'/'

            self.rundirectory = runDir

#       add option files

        dir = os.path.join(tmpdir,self.rundirectory)
        for opt_file in app.option_file:
            try:
                shutil.copy(opt_file.name,dir)
            except IOError:
                os.makedirs(dir)
                shutil.copy(opt_file.name,dir)

#       now tar it up again

        inpw = job.getInputWorkspace()
        rc, output = commands.getstatusoutput('tar czf %s -C %s .' % (inpw.getPath(sources),tmpdir))
        if rc:
            logger.error('Packing sources failed with status %d',rc)
            logger.error(output)
            raise ApplicationConfigurationError(None,'Packing sources failed.')

        shutil.rmtree(tmpdir)

#       upload sources

        logger.debug('Uploading source tarball ...')
        try:
            cwd = os.getcwd()
            os.chdir(inpw.getPath())
            rc, output = Client.putFile(sources)
            if output != 'True':
                logger.error('Uploading sources %s failed. Status = %d', sources, rc)
                logger.error(output)
                raise ApplicationConfigurationError(None,'Uploading archive failed')
        finally:
            os.chdir(cwd)     

#       input dataset

        if job.inputdata:
            if job.inputdata._name <> 'DQ2Dataset':
                raise ApplicationConfigurationError(None,'PANDA application supports only DQ2Datasets')

        if not job.inputdata.dataset:
           raise ApplicationConfigurationError(None,'You did not set job.inputdata.dataset')

        if len(job.inputdata.dataset) > 1:
           raise ApplicationConfigurationError(None,'GangaPanda does not currently support input containers')

        logger.info('Input dataset %s',job.inputdata.dataset[0])

#       output dataset

        if job.outputdata:
            if job.outputdata._name <> 'DQ2OutputDataset':
                raise ApplicationConfigurationError(None,'PANDA application supports only DQ2OutputDataset')
            if not job.outputdata.datasetname:
                job.outputdata.datasetname = '%s.%s.ganga.%d.%s' % (usertag,gridProxy.identity(),job.id,time.strftime("%Y%m%d",time.localtime()))

        else:
            job.outputdata = DQ2OutputDataset()
            job.outputdata.datasetname = '%s.%s.ganga.%d.%s' % (usertag,gridProxy.identity(),job.id,time.strftime("%Y%m%d",time.localtime()))

        if not job.outputdata.datasetname.startswith('%s.%s.ganga.'%(usertag,gridProxy.identity())):
            raise ApplicationConfigurationError(None,'outputdata.datasetname must start with %s.%s.ganga.'%(usertag,gridProxy.identity()))

        logger.info('Output dataset %s',job.outputdata.datasetname)

        if job.outputdata.outputdata and not job.backend.ara:
            raise ApplicationConfigurationError(None,'job.outputdata.outputdata is not required for normal athena user analyses (i.e. job.backend.ara = False)"')

        # ARA
        # output files for ARA
        if job.backend.ara and not job.outputdata.outputdata:
            raise ApplicationConfigurationError(None,'job.outputdata.outputdata is required for ARA jobs (i.e. job.backend.ara = True)"')
        self.extOutFile = []
        for tmpName in job.outputdata.outputdata:
            if tmpName != '':
                self.extOutFile.append(tmpName)

        # add extOutFiles
        for tmpName in job.backend.extOutFile:
            if tmpName != '':
                self.extOutFile.append(tmpName)

#       job options

        self.job_options = ''
        if app.options and not job.backend.ara:
            self.job_options += '-c %s ' % app.options
    
        self.job_options += ' '.join([os.path.basename(fopt.name) for fopt in app.option_file])

        # ARA uses trf I/F
        if job.backend.ara:
            if job.backend.ares:
                self.job_options = "athena.py " + self.job_options
            elif self.job_options.endswith(".C"):
                self.job_options = "root -l " + self.job_options
            else:
                self.job_options = "python " + self.job_options

        cacheVer = ''
        if app.atlas_project and app.atlas_production:
            cacheVer = "-" + app.atlas_project + "_" + app.atlas_production

        # run brokerage here if not splitting
        if not job.splitter:
            from GangaPanda.Lib.Panda.Panda import runPandaBrokerage
            runPandaBrokerage(job)
        elif job.splitter._name <> 'DQ2JobSplitter':
            raise ApplicationConfigurationError(None,'Panda splitter must be DQ2JobSplitter')
        
        if job.backend.site == 'AUTO':
            raise ApplicationConfigurationError(None,'site is still AUTO after brokerage!')

#       create build job
        jspec = JobSpec()
        jspec.jobDefinitionID   = job.id
        jspec.jobName           = commands.getoutput('uuidgen')
        jspec.AtlasRelease      = 'Atlas-%s' % app.atlas_release
        jspec.homepackage       = 'AnalysisTransforms'+cacheVer#+nightVer
        jspec.transformation    = '%s/buildJob-00-00-03' % Client.baseURLSUB
        jspec.destinationDBlock = self.libDataset
        jspec.destinationSE     = job.backend.site
        jspec.prodSourceLabel   = 'panda'
        jspec.assignedPriority  = 2000
        jspec.computingSite     = job.backend.site
        jspec.cloud             = job.backend.cloud
        jspec.jobParameters     = '-i %s -o %s' % (sources,self.library)
        matchURL = re.search('(http.*://[^/]+)/',Client.baseURLSSL)
        if matchURL:
            jspec.jobParameters += ' --sourceURL %s' % matchURL.group(1)

        fout = FileSpec()
        fout.lfn  = self.library
        fout.type = 'output'
        fout.dataset = self.libDataset
        fout.destinationDBlock = self.libDataset
        jspec.addFile(fout)

        flog = FileSpec()
        flog.lfn = '%s.log.tgz' % self.libDataset
        flog.type = 'log'
        flog.dataset = self.libDataset
        flog.destinationDBlock = self.libDataset
        jspec.addFile(flog)

        self.dbrFiles,self.dbrDsList = getDBDatasets(self.job_options,'',job.backend.dbRelease)

#       prepare output files

        self.indexFiles   = 0
        self.indexCavern  = 0
        self.indexMin     = 0
        self.indexBHaloA  = 0
        self.indexBHaloC  = 0
        self.indexBGasH   = 0
        self.indexBGasC   = 0
        self.indexBGasO   = 0
        self.indexNT      = 0
        self.indexHIST    = 0
        self.indexRDO     = 0
        self.indexESD     = 0
        self.indexAOD     = 0
        self.indexAANT    = 0
        self.indexTAG     = 0
        self.indexTHIST   = 0
        self.indexIROOT   = 0
        self.indexEXT     = 0
        self.indexStream1 = 0
        self.indexStream2 = 0
        self.indexStreamG = 0
        self.indexBS      = 0
        self.indexMeta    = 0

        return jspec

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        '''prepare the subjob specific configuration'''
 
        job = app._getParent()
        logger.debug('AthenaPandaRTHandler prepare called for %s', job.getFQID('.'))

#       in case of a simple job get the dataset content, otherwise subjobs are filled by the splitter
        if job.inputdata and not job._getRoot().subjobs:
            if not job.inputdata.names:
                for guid, lfn in job.inputdata.get_contents():
                    job.inputdata.guids.append(guid)
                    job.inputdata.names.append(lfn)

        site = job._getRoot().backend.site
        job.backend.site = site
        job.backend.actualCE = site
        cloud = job._getRoot().backend.cloud
        job.backend.cloud = cloud

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
        if job.backend.memory != -1:
            jspec.minRamCount = job.backend.memory
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
        if self.runConfig.output.outNtuple:
            self.indexNT += 1
            for name in self.runConfig.output.outNtuple:
                fout = FileSpec()
                fout.dataset           = job.outputdata.datasetname 
                fout.lfn               = '%s.%s._%05d.root' % (job.outputdata.datasetname,name,self.indexNT)
                fout.type              = 'output'
                fout.destinationDBlock = jspec.destinationDBlock
                fout.destinationSE    = jspec.destinationSE
                jspec.addFile(fout)
                if not 'ntuple' in outMap:
                    outMap['ntuple'] = []
                outMap['ntuple'].append((name,fout.lfn))

        if self.runConfig.output.outHist:
            self.indexHIST += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.hist._%05d.root' % (job.outputdata.datasetname,self.indexHIST)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['hist'] = fout.lfn

        if self.runConfig.output.outRDO:
            self.indexRDO += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.RDO._%05d.pool.root' % (job.outputdata.datasetname,self.indexRDO)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['RDO'] = fout.lfn

        if self.runConfig.output.outESD:
            self.indexESD += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.ESD._%05d.pool.root' % (job.outputdata.datasetname,self.indexESD)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['ESD'] = fout.lfn

        if self.runConfig.output.outAOD:
            self.indexAOD += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.AOD._%05d.pool.root' % (job.outputdata.datasetname,self.indexAOD)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['AOD'] = fout.lfn

        if self.runConfig.output.outTAG:
            self.indexTAG += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.TAG._%05d.coll.root' % (job.outputdata.datasetname,self.indexTAG)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['TAG'] = fout.lfn

        if self.runConfig.output.outAANT:
            self.indexAANT += 1
            sNameList = []
            for aName,sName in self.runConfig.output.outAANT:
                fout = FileSpec()
                fout.dataset           = job.outputdata.datasetname
                fout.lfn               = '%s.%s._%05d.root' % (job.outputdata.datasetname,sName,self.indexAANT)
                fout.type              = 'output'
                fout.destinationDBlock = jspec.destinationDBlock
                fout.destinationSE     = jspec.destinationSE
                if not sName in sNameList:
                    sNameList.append(sName)
                    jspec.addFile(fout)
                if not 'AANT' in outMap:
                    outMap['AANT'] = []
                outMap['AANT'].append((aName,sName,fout.lfn))

        if self.runConfig.output.outTHIST:
            self.indexTHIST += 1
            for name in self.runConfig.output.outTHIST:
                fout = FileSpec()
                fout.dataset           = job.outputdata.datasetname
                fout.lfn               = '%s.%s._%05d.root' % (job.outputdata.datasetname,name,self.indexTHIST)
                fout.type              = 'output'
                fout.destinationDBlock = jspec.destinationDBlock
                fout.destinationSE     = jspec.destinationSE
                jspec.addFile(fout)
                if not 'THIST' in outMap:
                    outMap['THIST'] = []
                outMap['THIST'].append((name,fout.lfn))   

        if self.runConfig.output.outIROOT:
            self.indexIROOT += 1
            for idx, name in enumerate(self.runConfig.output.outIROOT):
                fout = FileSpec()
                fout.dataset           = job.outputdata.datasetname
                fout.lfn               = '%s.iROOT%d._%05d.%s' % (job.outputdata.datasetname,idx,self.indexIROOT,name)
                fout.type              = 'output'
                fout.destinationDBlock = jspec.destinationDBlock
                fout.destinationSE     = jspec.destinationSE
                jspec.addFile(fout)
                if not 'IROOT' in outMap:
                    outMap['IROOT'] = []
                outMap['IROOT'].append((name,fout.lfn))   

        if self.extOutFile:
            self.indexEXT += 1
            for idx, name in enumerate(self.extOutFile):
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

        if self.runConfig.output.outStream1:
            self.indexStream1 += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.Stream1._%05d.pool.root' % (job.outputdata.datasetname,self.indexStream1)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['Stream1'] = fout.lfn

        if self.runConfig.output.outStream2:
            self.indexStream2 += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.Stream2._%05d.pool.root' % (job.outputdata.datasetname,self.indexStream2)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['Stream2'] = fout.lfn

        if self.runConfig.output.outBS:
            self.indexBS += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.BS._%05d.data' % (job.outputdata.datasetname,self.indexBS)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['BS'] = fout.lfn

        if self.runConfig.output.outStreamG:
            self.indexStreamG += 1
            for name in self.runConfig.output.outStreamG:
                fout = FileSpec()
                fout.dataset           = job.outputdata.datasetname
                fout.lfn               = '%s.%s._%05d.root' % (job.outputdata.datasetname,name,self.indexStreamG)
                fout.type              = 'output'
                fout.destinationDBlock = jspec.destinationDBlock
                fout.destinationSE     = jspec.destinationSE
                jspec.addFile(fout)
                if not 'StreamG' in outMap:
                    outMap['StreamG'] = []
                outMap['StreamG'].append((name,fout.lfn))
        
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
        if job.backend.dbRelease != '':
            tmpItems = job.backend.dbRelease.split(':')
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
        if job.backend.corCheck:
            param += '--corCheck '
        # disable to skip missing files
        if job.backend.notSkipMissing:
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
        if job.backend.ara:
            param += '--trf '
            param += '--ara '

 
        jspec.jobParameters = param
        
        return jspec

from Ganga.GPIDev.Credentials import GridProxy
gridProxy = GridProxy()

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('Athena','Panda',AthenaPandaRTHandler)


from Ganga.Utility.Config import getConfig, ConfigError
config = getConfig('Athena')
configDQ2 = getConfig('DQ2')

from Ganga.Utility.logging import getLogger
logger = getLogger()
