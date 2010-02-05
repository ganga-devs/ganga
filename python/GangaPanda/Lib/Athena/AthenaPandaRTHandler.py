###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaPandaRTHandler.py,v 1.32 2009-05-29 13:27:14 dvanders Exp $
###############################################################################
# Athena LCG Runtime Handler
#
# ATLAS/ARDA

import os, sys, pwd, commands, re, shutil, urllib, time, string, exceptions

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from GangaAtlas.Lib.ATLASDataset import DQ2Dataset, DQ2OutputDataset
from GangaPanda.Lib.Panda.Panda import runPandaBrokerage, uploadSources, getLibFileSpecFromLibDS
from Ganga.Core import BackendError

def getDBDatasets(jobO,trf,dbrelease):
    from pandatools import Client

    # get DB datasets
    dbrFiles  = {}
    dbrDsList = []
    if trf or dbrelease != '':
        if trf:
            # parse jobO for TRF
            tmpItems = jobO.split()
        else:
            # mimic a trf parameter to reuse following algorithm
            tmpItems = ['%DB='+dbrelease]
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

        from pandatools import Client
        from pandatools import AthenaUtils
        from taskbuffer.JobSpec import JobSpec
        from taskbuffer.FileSpec import FileSpec


        job = app._getParent()
        logger.debug('AthenaPandaRTHandler master_prepare called for %s', job.getFQID('.')) 

        usertag = configDQ2['usertag'] 
        self.username = gridProxy.identity(safe=True)
        username = self.username
        if job.backend.libds:
            self.libDataset = job.backend.libds
            self.fileBO = getLibFileSpecFromLibDS(self.libDataset)
            self.library = self.fileBO.lfn
        else:
            self.libDataset = '%s.%s.ganga.%s_%d.lib._%06d' % (usertag,username,commands.getoutput('hostname').split('.')[0],int(time.time()),job.id)
            self.library = '%s.lib.tgz' % self.libDataset
            try:
                Client.addDataset(self.libDataset,False)
            except exceptions.SystemExit:
                raise BackendError('Panda','Exception in Client.addDataset %s: %s %s'%(self.libDataset,sys.exc_info()[0],sys.exc_info()[1]))


        # validate application
        if not app.atlas_release:
            raise ApplicationConfigurationError(None,"application.atlas_release is not set. Did you run application.prepare()")
        self.dbrelease = app.atlas_dbrelease
        if self.dbrelease != '' and self.dbrelease.find(':') == -1:
            raise ApplicationConfigurationError(None,"ERROR : invalid argument for DB Release. Must be 'DatasetName:FileName'")
        self.runConfig = AthenaUtils.ConfigAttr(app.atlas_run_config)
        for k in self.runConfig.keys():
            self.runConfig[k]=AthenaUtils.ConfigAttr(self.runConfig[k])
        if not app.atlas_run_dir:
            raise ApplicationConfigurationError(None,"application.atlas_run_dir is not set. Did you run application.prepare()")
        self.rundirectory = app.atlas_run_dir
        self.cacheVer = ''
        if app.atlas_project and app.atlas_production:
            self.cacheVer = "-" + app.atlas_project + "_" + app.atlas_production
        if not app.atlas_exetype in ['ATHENA','PYARA','ARES','ROOT']:
            raise ApplicationConfigurationError(None,"Panda backend supports only application.atlas_exetype in ['ATHENA','PYARA','ARES','ROOT']")
        if app.atlas_exetype == 'ATHENA' and not app.user_area.name and not job.backend.libds:
            raise ApplicationConfigurationError(None,'app.user_area.name is null')

        # validate inputdata
        if job.inputdata:
            if job.inputdata._name == 'DQ2Dataset':
                logger.info('Input dataset(s) %s',job.inputdata.dataset)
            else: 
                raise ApplicationConfigurationError(None,'Panda backend supports only inputdata=DQ2Dataset()')
        else:
            logger.info('Proceeding without an input dataset.')

        # validate outputdata
        today = time.strftime("%Y%m%d",time.localtime())
        if job.outputdata:
            if job.outputdata._name <> 'DQ2OutputDataset':
                raise ApplicationConfigurationError(None,'Panda backend supports only DQ2OutputDataset')
            if not job.outputdata.datasetname:
                job.outputdata.datasetname = '%s.%s.ganga.%d.%s' % (usertag,username,job.id,today)
        else:
            logger.info('Adding missing DQ2OutputDataset')
            job.outputdata = DQ2OutputDataset()
            job.outputdata.datasetname = '%s.%s.ganga.%d.%s' % (usertag,username,job.id,today)
        if not job.outputdata.datasetname.startswith('%s.%s.ganga.'%(usertag,username)):
            logger.info('outputdata.datasetname must start with %s.%s.ganga. Prepending it for you.'%(usertag,username))
            job.outputdata.datasetname = '%s.%s.ganga.'%(usertag,username)+job.outputdata.datasetname
        logger.info('Output dataset %s',job.outputdata.datasetname)
        try:
            Client.addDataset(job.outputdata.datasetname,False)
        except exceptions.SystemExit:
            raise BackendError('Panda','Exception in Client.addDataset %s: %s %s'%(job.outputdata.datasetname,sys.exc_info()[0],sys.exc_info()[1]))

        # handle different atlas_exetypes
        self.job_options = ''
        if app.atlas_exetype == 'TRF':
            #self.job_options = app.option_file.name + app.trf_parameters
            raise ApplicationConfigurationError(None,"Sorry TRF on Panda backend not yet supported")
        elif app.atlas_exetype == 'ATHENA':
            if job.outputdata.outputdata:
                raise ApplicationConfigurationError(None,"job.outputdata.outputdata must be empty if atlas_exetype='ATHENA' (outputs are auto-detected)")
            if app.options:
                self.job_options += '-c %s ' % app.options
            self.job_options += ' '.join([os.path.basename(fopt.name) for fopt in app.option_file])
        elif app.atlas_exetype in ['PYARA','ARES','ROOT']:
            if not job.outputdata.outputdata:
                raise ApplicationConfigurationError(None,"job.outputdata.outputdata is required for atlas_exetype in ['PYARA','ARES','TRF','ROOT']")
            self.job_options += ' '.join([os.path.basename(fopt.name) for fopt in app.option_file])
            if app.atlas_exetype == 'PYARA':
                self.job_options = "python " + self.job_options
            elif app.atlas_exetype == 'ARES':
                self.job_options = "athena.py " + self.job_options
            elif app.atlas_exetype == 'ROOT':
                self.job_options = "root -l " + self.job_options
        if self.job_options == '':
            raise ApplicationConfigurationError(None,"No Job Options found!")
        logger.info('Running job options: %s'%self.job_options)

        # add extOutFiles
        self.extOutFile = []
        for tmpName in job.outputdata.outputdata:
            if tmpName != '':
                self.extOutFile.append(tmpName)
        for tmpName in job.backend.extOutFile:
            if tmpName != '':
                self.extOutFile.append(tmpName)

        # run brokerage here if not splitting
        if not job.splitter:
            runPandaBrokerage(job)
        elif job.splitter._name <> 'DQ2JobSplitter' and job.splitter._name <> 'AnaTaskSplitterJob':
            raise ApplicationConfigurationError(None,'Panda splitter must be DQ2JobSplitter')
        if job.backend.site == 'AUTO':
            raise ApplicationConfigurationError(None,'site is still AUTO after brokerage!')

        # validate dbrelease
        self.dbrFiles,self.dbrDsList = getDBDatasets(self.job_options,'',self.dbrelease)

        # upload sources
        if app.user_area.name and not job.backend.libds:
            uploadSources(os.path.dirname(app.user_area.name),os.path.basename(app.user_area.name))

        # create build job
        jspec = JobSpec()
        jspec.jobDefinitionID   = job.id
        jspec.jobName           = commands.getoutput('uuidgen')
        jspec.AtlasRelease      = 'Atlas-%s' % app.atlas_release
        jspec.homepackage       = 'AnalysisTransforms'+self.cacheVer#+nightVer
        jspec.transformation    = '%s/buildJob-00-00-03' % Client.baseURLSUB
        jspec.destinationDBlock = self.libDataset
        jspec.destinationSE     = job.backend.site
        jspec.prodSourceLabel   = configPanda['prodSourceLabelBuild']
        jspec.processingType    = configPanda['processingType']
        jspec.assignedPriority  = configPanda['assignedPriorityBuild']
        jspec.computingSite     = job.backend.site
        jspec.cloud             = job.backend.requirements.cloud
        jspec.jobParameters     = '-o %s' % (self.library)
        if app.user_area.name:
            jspec.jobParameters     += ' -i %s' % (os.path.basename(app.user_area.name))
        matchURL = re.search('(http.*://[^/]+)/',Client.baseURLSSL)
        if matchURL:
            jspec.jobParameters += ' --sourceURL %s' % matchURL.group(1)
        jspec.cmtConfig         = app.atlas_cmtconfig

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

        # prepare output files
        self.indexFiles   = 0
        self.indexCavern  = 0
        self.indexMin     = 0
        self.indexBHalo   = 0
        self.indexBHaloA  = 0
        self.indexBHaloC  = 0
        self.indexBGas    = 0
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
        self.indexSelBS   = 0
        self.indexMeta    = 0
        self.indexMS      = 0

        return jspec

    # get maximum index
    def getIndex(list,pattern):
        maxIndex = 0
        for item in list:
            match = re.match(pattern,item)
            if match != None:
                tmpIndex = int(match.group(1))
                if maxIndex < tmpIndex:
                    maxIndex = tmpIndex
        return maxIndex

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        '''prepare the subjob specific configuration'''

        from pandatools import Client
        from taskbuffer.JobSpec import JobSpec
        from taskbuffer.FileSpec import FileSpec

 
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
        
        usertag = configDQ2['usertag'] 
        username = self.username
        if not job.outputdata.datasetname.startswith('%s.%s.ganga.'%(usertag,username)):
            job.outputdata.datasetname = '%s.%s.ganga.'%(usertag,username)+job.outputdata.datasetname
        
        if job.inputdata:
            if len(job.inputdata.dataset) > 1:
                raise ApplicationConfigurationError(None,'Multiple input datasets per subjob not supported. Use a container dataset?')

        jspec = JobSpec()
        jspec.jobDefinitionID   = job._getRoot().id
        jspec.jobName           = commands.getoutput('uuidgen')  
        jspec.AtlasRelease      = 'Atlas-%s' % app.atlas_release
        jspec.homepackage       = 'AnalysisTransforms'+self.cacheVer#+nightVer
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
        jspec.prodSourceLabel   = configPanda['prodSourceLabelRun']
        jspec.processingType    = configPanda['processingType']
        jspec.assignedPriority  = configPanda['assignedPriorityRun']
        jspec.cloud             = cloud
        jspec.computingSite     = site
        if job.backend.requirements.memory != -1:
            jspec.minRamCount = job.backend.requirements.memory
        if job.backend.requirements.cputime != -1:
            jspec.maxCpuCount = job.backend.requirements.cputime
        jspec.cmtConfig         = app.atlas_cmtconfig

#       library (source files)
        if not job.backend.libds:
            flib = FileSpec()
            flib.lfn            = self.library
            flib.type           = 'input'
            flib.dataset        = self.libDataset
            flib.dispatchDBlock = self.libDataset
        else:
            flib = FileSpec()
            flib.lfn            = self.fileBO.lfn
            flib.GUID           = self.fileBO.GUID
            flib.type           = 'input'
            flib.status         = self.fileBO.status
            flib.dataset        = self.fileBO.destinationDBlock
            flib.dispatchDBlock = self.fileBO.destinationDBlock
        jspec.addFile(flib)

#       input files FIXME: many more input types
        if job.inputdata:
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
        if self.runConfig.output and self.runConfig.output.outNtuple:
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

        if self.runConfig.output and self.runConfig.output.outHist:
            self.indexHIST += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.hist._%05d.root' % (job.outputdata.datasetname,self.indexHIST)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['hist'] = fout.lfn

        if self.runConfig.output and self.runConfig.output.outRDO:
            self.indexRDO += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.RDO._%05d.pool.root' % (job.outputdata.datasetname,self.indexRDO)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['RDO'] = fout.lfn

        if self.runConfig.output and self.runConfig.output.outESD:
            self.indexESD += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.ESD._%05d.pool.root' % (job.outputdata.datasetname,self.indexESD)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['ESD'] = fout.lfn

        if self.runConfig.output and self.runConfig.output.outAOD:
            self.indexAOD += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.AOD._%05d.pool.root' % (job.outputdata.datasetname,self.indexAOD)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['AOD'] = fout.lfn

        if self.runConfig.output and self.runConfig.output.outTAG:
            self.indexTAG += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.TAG._%05d.coll.root' % (job.outputdata.datasetname,self.indexTAG)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['TAG'] = fout.lfn

        if self.runConfig.output and self.runConfig.output.outAANT:
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

        if self.runConfig.output and self.runConfig.output.outTHIST:
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

        if self.runConfig.output and self.runConfig.output.outIROOT:
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

        if self.runConfig.output and self.runConfig.output.outStream1:
            self.indexStream1 += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.Stream1._%05d.pool.root' % (job.outputdata.datasetname,self.indexStream1)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['Stream1'] = fout.lfn

        if self.runConfig.output and self.runConfig.output.outStream2:
            self.indexStream2 += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.Stream2._%05d.pool.root' % (job.outputdata.datasetname,self.indexStream2)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['Stream2'] = fout.lfn

        if self.runConfig.output and self.runConfig.output.outBS:
            self.indexBS += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.BS._%05d.data' % (job.outputdata.datasetname,self.indexBS)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['BS'] = fout.lfn

        if self.runConfig.output and self.runConfig.output.outSelBS:
            self.indexSelBS += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.%s._%05d.data' % (job.outputdata.datasetname,self.runConfig.output.outSelBS,self.indexSelBS)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['BS'] = fout.lfn
            if not 'IROOT' in outMap:  # this is not a typo!
                outMap['IROOT'] = []
            outMap['IROOT'].append(('%s.*.data' % self.runConfig.output.outSelBS,fout.lfn))

        if self.runConfig.output and self.runConfig.output.outStreamG:
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
        
        if self.runConfig.output and self.runConfig.output.outMeta:
            iMeta = 0
            self.indexMeta += 1
            for sName,sAsso in self.runConfig.output.outMeta:
                foundLFN = ''
                if sAsso == 'None':
                    # non-associated metadata
                    fout = FileSpec()
                    fout.lfn  = '%s.META%s._%05d.root' % (job.outputdata.datasetname,iMeta,self.indexMeta)
                    fout.type = 'output'
                    fout.dataset = job.outputdata.datasetname
                    fout.destinationDBlock = jspec.destinationDBlock
                    fout.destinationSE = jspec.destinationSE
                    jspec.addFile(fout)
                    iMeta += 1
                    foundLFN = fout.lfn
                elif outMap.has_key(sAsso):
                    # Stream1,2
                    foundLFN = outMap[sAsso]
                elif sAsso in ['StreamRDO','StreamESD','StreamAOD']:
                    # RDO,ESD,AOD
                    stKey = re.sub('^Stream','',sAsso)
                    if outMap.has_key(stKey):
                        foundLFN = outMap[stKey]
                else:
                    # general stream
                    if outMap.has_key('StreamG'):
                        for tmpStName,tmpLFN in outMap['StreamG']:
                            if tmpStName == sAsso:
                                foundLFN = tmpLFN
                if foundLFN != '':
                    if not outMap.has_key('Meta'):
                        outMap['Meta'] = []
                    outMap['Meta'].append((sName,foundLFN))

        if self.runConfig.output and self.runConfig.output.outMS:
            self.indexMS += 1
            for sName,sAsso in self.runConfig.output.outMS:
                fout = FileSpec()
                fout.lfn  = '%s.%s._%05d.pool.root' % (job.outputdata.datasetname,sName,self.indexMS)
                fout.type = 'output'
                fout.dataset = job.outputdata.datasetname
                fout.destinationDBlock = jspec.destinationDBlock
                fout.destinationSE = jspec.destinationSE
                jspec.addFile(fout)
                if not outMap.has_key('IROOT'):
                    outMap['IROOT'] = []
                outMap['IROOT'].append((sAsso,fout.lfn))

        if self.runConfig.output and self.runConfig.output.outUserData:
            for sAsso in self.runConfig.output.outUserData:
                # look for associated LFN
                foundLFN = ''
                if outMap.has_key(sAsso):
                    # Stream1,2
                    foundLFN = outMap[sAsso]
                elif sAsso in ['StreamRDO','StreamESD','StreamAOD']:
                    # RDO,ESD,AOD
                    stKey = re.sub('^Stream','',sAsso)
                    if outMap.has_key(stKey):
                        foundLFN = outMap[stKey]
                else:
                    # general stream
                    if outMap.has_key('StreamG'):
                        for tmpStName,tmpLFN in outMap['StreamG']:
                            if tmpStName == sAsso:
                                foundLFN = tmpLFN
                if foundLFN != '':
                    if not outMap.has_key('UserData'):
                        outMap['UserData'] = []
                    outMap['UserData'].append(foundLFN)

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
        if self.dbrelease != '':
            tmpItems = self.dbrelease.split(':')
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
        if job.inputdata:    
            param += '-i "%s" ' % job.inputdata.names
        else:
            param += '-i "[]" '
        param += '-m "[]" ' #%minList FIXME
        param += '-n "[]" ' #%cavList FIXME
        #FIXME
        #if bhaloList != []:
        #    param += '--beamHalo "%s" ' % bhaloList
        #if bgasList != []:
        #    param += '--beamGas "%s" ' % bgasList
        param += '-o "%s" ' % outMap
        if self.runConfig.input and self.runConfig.input.inColl: 
            param += '-c '
        if self.runConfig.input and self.runConfig.input.inBS: 
            param += '-b '
        if self.runConfig.input and self.runConfig.input.backNavi: 
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
        if app.atlas_exetype in ['PYARA','ARES','ROOT']:
            param += '--trf '
            param += '--ara '
        if job.backend.accessmode == 'FILE_STAGER':
            param += '--accessmode=filestager'
 
        jspec.jobParameters = param
        
        return jspec

from Ganga.GPIDev.Credentials import GridProxy
gridProxy = GridProxy()

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('Athena','Panda',AthenaPandaRTHandler)


from Ganga.Utility.Config import getConfig, ConfigError
config = getConfig('Athena')
configDQ2 = getConfig('DQ2')
configPanda = getConfig('Panda')

from Ganga.Utility.logging import getLogger
logger = getLogger()
