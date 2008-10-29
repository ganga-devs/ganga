###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaPandaRTHandler.py,v 1.11 2008-10-29 15:26:52 dvanders Exp $
###############################################################################
# Athena LCG Runtime Handler
#
# ATLAS/ARDA

import os, sys, pwd, commands, re, shutil, urllib, time 

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

def extractConfiguration(jobOptions,trf=False):

    config = {
       'outHist'     : False,
       'outNtuple'   : [],
       'outRDO'      : False,
       'outESD'      : False,
       'outAOD'      : False,
       'outTAG'      : False,
       'outAANT'     : [],
       'outTHIST'    : [],
       'outIROOT'    : [],
       'outStream1'  : False,
       'outStream2'  : False,
       'outBS'       : False,
       'outStreamG'  : [],
       'outMeta'     : [],
       'inBS'        : False,
       'inColl'      : False,
       'inMinBias'   : False,
       'inCavern'    : False,
       'inBeamGas'   : False,
       'inBeamHalo'  : False,
       'backNavi'    : False,
       'shipFiles'   : [],
       'rndmStream'  : [],
       'rndmNumbers' : [],
       'extOutFile'  : [],

# need to be made user options:
       'shipinput'   : False,
       'memory'      : -1,
       'addPoolFC'   : '',
       'pfnList'        : '',
       'mcData'         : ''
    }

    if not trf:
        # run ConfigExtractor for normal jobO
        jobOpt1 = os.path.join(os.environ['CONFIGEXTRACTOR_PATH'],'FakeAppMgr.py')
        jobOpt2 = os.path.join(os.environ['CONFIGEXTRACTOR_PATH'],'ConfigExtractor.py') 
        rc, output = commands.getstatusoutput('athena.py %s %s %s' % (jobOpt1,jobOptions,jobOpt2))
        if rc>>8 :
            logger.warning('Return code of athena was %d from the ConfigExtractor. This is probably harmless.' % (rc>>8))

        fail = True
        for line in output.split('\n'):
            if not line.startswith('ConfigExtractor >'): continue
            fail = False
            item = line[18:].split()

            if   item[0] == 'Output=HIST':
                config['outHist'] = True
            elif item[0] == 'Output=NTUPLE':
                config['outNtuple'].append(item[1])
            elif item[0] == 'Output=RDO':
                config['outRDO'] = True
            elif item[0] == 'Output=ESD':
                config['outESD'] = True
            elif item[0] == 'Output=AOD':
                config['outAOD'] = True
            elif item[0] == 'Output=TAG':
                config['outTAG'] = True
            elif item[0] == 'Output=AANT':
                config['outAANT'].append(tuple(item[1:]))
            elif item[0] == 'Output=THIST':
                config['outTHIST'].append(item[1])
            elif item[0] == 'Output=IROOT':
                config['outIROOT'].append(item[1])
            elif item[0] == 'Output=STREAM1':
                config['outStream1'] = True
            elif item[0] == 'Output=STREAM2':
                config['outStream2'] = True
            elif item[0] == 'Output=BS':
                config['outBS'] = True
            elif item[0] == 'Output=STREAMG':
                config['outStreamG'].append(item[1])
            elif item[0] == 'Output=META':
                config['outMeta'].append(tuple(item[1:]))
            elif item[0] == 'Input=BS':
                config['inBS'] = True
            elif item[0] == 'Input=COLL':
                config['inColl'] = True
            elif item[0] == 'Input=MINBIAS':
                config['inMinBias'] = True
            elif item[0] == 'Input=CAVERN':
                config['inCavern'] = True
            elif item[0] == 'Input=BEAMHALO':
                config['inBeamHalo'] = True
            elif item[0] == 'Input=BEAMGAS':
                config['inBeamGas'] = True
            elif item[0] == 'BackNavigation=ON':
                config['backNavi'] = True
            elif item[0] == 'RndmStream':
                config['rndmStream'].append(item[1])
            elif item[0] == 'RndmGenFile':
                config['rndmGenFile'].append(item[-1])
            elif item[0] == 'InputFiles':
                if config['shipinput']:
                    config['shipFiles'].append(item[1])
                else:
                    continue
            elif item[0] == 'CondInput':
                if config['addPoolFC'] == "":
                    config['addPoolFC'].append(item[-1])
                else:
                    config['addPoolFC'].append(",%s" % item[-1])
        if fail: raise ApplicationConfigurationError(None,'Extractor could not parse job')

    else:
        # parse parameters for trf
        for tmpItem in jobOptions.split():
            match = re.search('^\%OUT\.(.+)',tmpItem)
            if match:
                # append basenames to extOutFile
                config['extOutFile'].append(match.group(1))

    if logger.isEnabledFor(10):
       for key, value in config.iteritems():
           if value:
               logger.debug('%s : %s',key,value) 

    return config


def alternateCmtExtraction():
    import os
    import commands

    # save current dir
    currentDir = os.path.realpath(os.getcwd())

    # get project parameters
    out = commands.getoutput('cmt show projects')
    lines = out.split('\n')
    # remove CMT warnings
    tupLines = tuple(lines)
    lines = []
    for line in tupLines:
        if not line.startswith('#'):
            lines.append(line)
    if len(lines)<2:
        print out
        raise ApplicationConfigurationError(None,"ERROR : cmt show projects")

    # private work area
    res = re.search('\(in ([^\)]+)\)',lines[0])
    if res==None:
        print lines[0]
        raise ApplicationConfigurationError(None,"ERROR : could not get path to private work area")
    workArea = os.path.realpath(res.group(1))

    # get Athena version and group area
    athenaVer = ''
    groupArea = ''
    cacheVer  = ''
    nightVer  = ''
    for line in lines[1:]:
        res = re.search('\(in ([^\)]+)\)',line)
        if res != None:
            items = line.split()
            if items[0] in ('dist','AtlasRelease','AtlasOffline'):
                # Atlas release
                athenaVer = os.path.basename(res.group(1))
                # nightly
                if athenaVer.startswith('rel'):
                   if re.search('/bugfix',line) != None:
                      nightVer  = '/bugfix'
                   elif re.search('/dev',line) != None:
                      nightVer  = '/dev'
                   else:
                      raise ApplicationConfigurationError(None, "ERROR : unsupported nightly %s" % line)
                break
            elif items[0] in ['AtlasProduction','AtlasPoint1','AtlasTier0','AtlasP1HLT']:
                # production cache
                cacheVer = '-%s_%s' % (items[0],os.path.basename(res.group(1)))
            else:
            # group area
                groupArea = os.path.realpath(res.group(1))
    # error
    if athenaVer == '':
        for line in lines:
            print line
        raise ApplicationConfigurationError(None,"ERROR : could not get Athena version")

    return [currentDir,workArea,athenaVer,groupArea,cacheVer,nightVer]

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

#       parse job options file

        logger.info('Parsing job options file ...')
        job_option_files = ' '.join([ opt_file.name for opt_file in app.option_file ])

        self.config = extractConfiguration(job_option_files,job.backend.ara)

#       unpack library
        logger.info('Creating source tarball ...')        
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
            [currentDir,workArea,ignore,ignore,ignore,ignore] = alternateCmtExtraction()
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

        logger.info('Uploading source tarball ...')
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

        logger.info('Output datasetname %s',job.outputdata.datasetname)

        # ARA
        if job.outputdata.outputdata and not job.backend.ara:
            raise ApplicationConfigurationError(None,'job.outputdata.outputdata is not required when job.backend.ara is True"')
        # output files for ARA
        if job.backend.ara and not job.outputdata.outputdata:
            raise ApplicationConfigurationError(None,'job.outputdata.outputdata is needed when job.backend.ara is True"')
        for tmpName in job.outputdata.outputdata:
            if tmpName != '':
                self.config['extOutFile'].append(tmpName)

        # add extOutFiles
        for tmpName in job.backend.extOutFile:
            if tmpName != '':
                self.config['extOutFile'].append(tmpName)

#       job options

        self.job_options = ''
        if app.options and not job.backend.ara:
            self.job_options += '-c %s ' % app.options
    
        self.job_options += ' '.join([os.path.basename(fopt.name) for fopt in app.option_file])

        # ARA uses trf I/F
        if job.backend.ara:
            if self.job_options.endswith(".C"):
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
            jspec.destinationSE = job.outputdata.location
        else:
            jspec.destinationSE = site
        jspec.prodSourceLabel   = 'user'
        jspec.assignedPriority  = 1000
        jspec.cloud             = cloud
        # memory
        if self.config['memory'] != -1:
            jspec.minRamCount = self.config['memory']
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
        if self.config['outNtuple']:
            self.indexNT += 1
            for name in self.config['outNtuple']:
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

        if self.config['outHist']:
            self.indexHIST += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.hist._%05d.root' % (job.outputdata.datasetname,self.indexHIST)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['hist'] = fout.lfn

        if self.config['outRDO']:
            self.indexRDO += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.RDO._%05d.pool.root' % (job.outputdata.datasetname,self.indexRDO)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['RDO'] = fout.lfn

        if self.config['outESD']:
            self.indexESD += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.ESD._%05d.pool.root' % (job.outputdata.datasetname,self.indexESD)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['ESD'] = fout.lfn

        if self.config['outAOD']:
            self.indexAOD += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.AOD._%05d.pool.root' % (job.outputdata.datasetname,self.indexAOD)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['AOD'] = fout.lfn

        if self.config['outTAG']:
            self.indexTAG += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.TAG._%05d.coll.root' % (job.outputdata.datasetname,self.indexTAG)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['TAG'] = fout.lfn

        if self.config['outAANT']:
            self.indexAANT += 1
            sNameList = []
            for aName,sName in self.config['outAANT']:
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

        if self.config['outTHIST']:
            self.indexTHIST += 1
            for name in self.config['outTHIST']:
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

        if self.config['outIROOT']:
            self.indexIROOT += 1
            for idx, name in enumerate(self.config['outIROOT']):
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

        if self.config['extOutFile']:
            self.indexEXT += 1
            for idx, name in enumerate(self.config['extOutFile']):
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

        if self.config['outStream1']:
            self.indexStream1 += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.Stream1._%05d.pool.root' % (job.outputdata.datasetname,self.indexStream1)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['Stream1'] = fout.lfn

        if self.config['outStream2']:
            self.indexStream2 += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.Stream2._%05d.pool.root' % (job.outputdata.datasetname,self.indexStream2)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['Stream2'] = fout.lfn

        if self.config['outBS']:
            self.indexBS += 1
            fout = FileSpec()
            fout.dataset           = job.outputdata.datasetname
            fout.lfn               = '%s.BS._%05d.data' % (job.outputdata.datasetname,self.indexBS)
            fout.type              = 'output'
            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationSE     = jspec.destinationSE
            jspec.addFile(fout)
            outMap['BS'] = fout.lfn

        if self.config['outStreamG']:
            self.indexStreamG += 1
            for name in self.config['outStreamG']:
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
        param += '-j "%s" ' % urllib.quote(self.job_options)
        param += '-i "%s" ' % job.inputdata.names
        param += '-m "[]" ' #%minList FIXME
        param += '-n "[]" ' #%cavList FIXME
        #FIXME
        #if bhaloList != []:
        #    param += '--beamHalo "%s" ' % bhaloList
        #if bgasList != []:
        #    param += '--beamGas "%s" ' % bgasList
        param += '-o "%s" ' % outMap
        if self.config['inColl']: 
            param += '-c '
        if self.config['inBS']: 
            param += '-b '
        if self.config['backNavi']: 
            param += '-e '
        if self.config['shipinput']: 
            param += '--shipInput '
        #FIXME options.rndmStream
        nEventsToSkip = 0
        if app.max_events > 0:
            param += '-f "theApp.EvtMax=%d;EventSelector.SkipEvents=%s" ' % (app.max_events,nEventsToSkip)
        # addPoolFC
        if self.config['addPoolFC'] != "":
            param += '--addPoolFC %s ' % self.config['addPoolFC']
        # use corruption checker
        if job.backend.corCheck:
            param += '--corCheck '
        # disable to skip missing files
        if job.backend.notSkipMissing:
            param += '--notSkipMissing '
        # given PFN 
        if self.config['pfnList'] != '':
            param += '--givenPFN '
        # create symlink for MC data
        if self.config['mcData'] != '':
            param += '--mcData %s ' % self.config['mcData']
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
