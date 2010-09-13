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

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2outputdatasetname
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_set_dataset_lifetime
from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2

from Ganga.Utility.GridShell import getShell


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

def execute_build_job(trafo, sources, parameters):
    tmpdir = '/tmp/%s' % commands.getoutput('uuidgen')
    os.system("mkdir %s && cd %s" % (tmpdir, tmpdir))

    status, output = commands.getstatusoutput("cd %s && wget %s && ln -s %s ." % (tmpdir, trafo, sources))
    if status != 0:
        print output
        raise ApplicationConfigurationError(None,"Failure in setting up local build job!")

    tf = os.path.basename(trafo)
    logger.warning("Locally executing build job. This can take minutes, please be patient.")
    status, output = commands.getstatusoutput("cd %s && chmod +x %s && ./%s %s" % (tmpdir, tf, tf, parameters))
    if status != 0:
        print output
        raise ApplicationConfigurationError(None,"Failure in creating library package with local build job!")
   
    return tmpdir
    

class AthenaPandaRTHandler(IRuntimeHandler):
    '''Athena Panda Runtime Handler'''

    def make_local_libds(self, jspec, sources, nobuild):
        if nobuild:
            tmpdir = '/tmp/%s' % commands.getoutput('uuidgen')
            os.system("mkdir %s" % (tmpdir))
            status, output = commands.getstatusoutput("cd %s && cp %s %s" % (tmpdir, sources, self.library))
        else:
            tmpdir = execute_build_job(jspec.transformation, sources, jspec.jobParameters)

        setupstr = ["export OLDPATH=$PATH"]
        setupstr.append('source /afs/cern.ch/atlas/offline/external/GRID/ddm/DQ2Clients/setup.sh 2>&1 > /dev/null')
        setupstr.append('export VOMS_PROXY_INFO_DONT_VERIFY_AC=1')
        setupstr.append("export PATH=$OLDPATH")
        from pandatools import Client
        site = Client.PandaSites[jspec.destinationSE]["ddm"]
        cmd = "dq2-put -a -d -L %s -s %s -f %s %s" % (site, tmpdir, self.library, self.libDataset)
        cmdline = "%s; %s" % (";".join(setupstr), cmd)
        print cmdline
        shell = getShell("EDG")
        status, output = shell.cmd1(cmdline)
        if status != 0:
            print output
            raise ApplicationConfigurationError(None,"Failure in uploading library package with local build job!")

    def master_prepare(self,app,appconfig):
        '''Prepare the master job'''

        from pandatools import Client
        from pandatools import AthenaUtils
        from taskbuffer.JobSpec import JobSpec
        from taskbuffer.FileSpec import FileSpec

        # get Athena versions
        rc, out = AthenaUtils.getAthenaVer()
        # failed
        if not rc:
            raise ApplicationConfigurationError(None, 'CMT could not parse correct environment ! \n Did you start/setup ganga in the run/ or cmt/ subdirectory of your athena analysis package ?')
        self.userarea = out['workArea']

        job = app._getParent()
        logger.debug('AthenaPandaRTHandler master_prepare called for %s', job.getFQID('.')) 

        if job.backend.libds == "LOCAL":
            local_libds = True
            local_libds_nobuild = not app.athena_compile
            job.backend.libds = None
        elif job.backend.libds == "NOBUILD":
            local_libds = True
            local_libds_nobuild = True
            job.backend.libds = None
        else:
            local_libds = False

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
        if not app.atlas_exetype in ['ATHENA','PYARA','ARES','ROOT','EXE']:
            raise ApplicationConfigurationError(None,"Panda backend supports only application.atlas_exetype in ['ATHENA','PYARA','ARES','ROOT','EXE']")
        if app.atlas_exetype == 'ATHENA' and not app.user_area.name and not job.backend.libds:
            raise ApplicationConfigurationError(None,'app.user_area.name is null')

        # validate inputdata
        if job.inputdata:
            if job.inputdata._name == 'DQ2Dataset':
                self.inputdatatype='DQ2'
                logger.info('Input dataset(s) %s',job.inputdata.dataset)
            elif job.inputdata._name == 'AMIDataset':
                self.inputdatatype='DQ2'
                job.inputdata.dataset = job.inputdata.search()
                logger.info('Input dataset(s) %s',job.inputdata.dataset)
            elif job.inputdata._name == 'EventPicking':
                self.inputdatatype='DQ2'
                logger.info('Input dataset(s) %s',job.inputdata.dataset)
            elif job.inputdata._name == 'ATLASTier3Dataset':
                self.inputdatatype='Tier3'
                logger.info('Input dataset is a Tier3 PFN list')
            else: 
                raise ApplicationConfigurationError(None,'Panda backend supports only inputdata=DQ2Dataset()')
        else:
            logger.info('Submitting without an input dataset.')

        # handle different atlas_exetypes
        self.job_options = ''
        if app.atlas_exetype == 'TRF':
            #self.job_options = app.option_file.name + app.trf_parameters
            raise ApplicationConfigurationError(None,"Sorry TRF on Panda backend not yet supported")
        elif app.atlas_exetype == 'ATHENA':
            
            if len(app.atlas_environment) > 0 and app.atlas_environment[0].find('DBRELEASE_OVERRIDE')==-1:
                logger.warning("Passing of environment variables to Athena using Panda not supported. Ignoring atlas_environment setting.")
                
            if job.outputdata.outputdata:
                raise ApplicationConfigurationError(None,"job.outputdata.outputdata must be empty if atlas_exetype='ATHENA' and Panda backend is used (outputs are auto-detected)")
            if app.options:
                if app.options.startswith('-c'):
                    self.job_options += ' %s ' % app.options
                else:
                    self.job_options += ' -c %s ' % app.options
            self.job_options += ' '.join([os.path.basename(fopt.name) for fopt in app.option_file])
        elif app.atlas_exetype in ['PYARA','ARES','ROOT','EXE']:

            if not job.outputdata.outputdata:
                raise ApplicationConfigurationError(None,"job.outputdata.outputdata is required for atlas_exetype in ['PYARA','ARES','TRF','ROOT'] and Panda backend")
            self.job_options += ' '.join([os.path.basename(fopt.name) for fopt in app.option_file])

            # sort out environment variables
            env_str = ""
            if len(app.atlas_environment) > 0:
                for env_var in app.atlas_environment:
                    env_str += "export %s ; " % env_var
            else: 
                env_str = ""

            if app.atlas_exetype == 'PYARA':
                self.job_options = env_str + '/bin/echo %IN | sed \'s/,/\\\\\\n/g\' > input.txt; python ' + self.job_options
            elif app.atlas_exetype == 'ARES':
                self.job_options = env_str + '/bin/echo %IN | sed \'s/,/\\\\\\n/g\' > input.txt; athena.py ' + self.job_options
            elif app.atlas_exetype == 'ROOT':
                self.job_options = env_str + '/bin/echo %IN | sed \'s/,/\\\\\\n/g\' > input.txt; root -b -q ' + self.job_options
            elif app.atlas_exetype == 'EXE':
                self.job_options = env_str + '/bin/echo %IN | sed \'s/,/\\\\\\n/g\' > input.txt; ' + self.job_options

            if app.options:
                self.job_options += ' %s ' % app.options

        if self.job_options == '':
            raise ApplicationConfigurationError(None,"No Job Options found!")
        logger.info('Running job options: %s'%self.job_options)

        # validate inputdata
        if job.inputdata:
            if job.inputdata._name == 'DQ2Dataset':
                self.inputdatatype='DQ2'
                logger.info('Input dataset(s) %s',job.inputdata.dataset)
            elif job.inputdata._name == 'AMIDataset':
                self.inputdatatype='DQ2'
                job.inputdata.dataset = job.inputdata.search()
                logger.info('Input dataset(s) %s',job.inputdata.dataset)
            elif job.inputdata._name == 'EventPicking':
                self.inputdatatype='DQ2'
                logger.info('Input dataset(s) %s',job.inputdata.dataset)
            elif job.inputdata._name == 'ATLASTier3Dataset':
                self.inputdatatype='Tier3'
                logger.info('Input dataset is a Tier3 PFN list')
            else: 
                raise ApplicationConfigurationError(None,'Panda backend supports only inputdata=DQ2Dataset()')
        else:
            self.inputdatatype='None'
            logger.info('Proceeding without an input dataset.')

        # run brokerage here if not splitting'
        if self.inputdatatype=='DQ2':
            if not job.splitter:
                runPandaBrokerage(job)
            elif job.splitter._name <> 'DQ2JobSplitter' and job.splitter._name <> 'AnaTaskSplitterJob':
                raise ApplicationConfigurationError(None,'Splitting with Panda+DQ2Dataset requires DQ2JobSplitter')
        elif self.inputdatatype=='Tier3':
            if job.splitter and job.splitter._name != 'ATLASTier3Splitter':
                raise ApplicationConfigurationError(None,'Splitting with Panda+ATLASTier3Dataset requires ATLASTier3Splitter')
            if job.backend.site == 'AUTO':
                raise ApplicationConfigurationError(None,'Panda+ATLASTier3Dataset requires a specified backend.site')
            job.backend.requirements.cloud = Client.PandaSites[job.backend.site]['cloud']
        elif self.inputdatatype == 'None':
            runPandaBrokerage(job)
            
        if len(job.subjobs) == 0 and job.backend.site == 'AUTO':
            raise ApplicationConfigurationError(None,'Error: backend.site=AUTO after brokerage. Report to DA Help Forum')
        
        # handle the output dataset
        if job.outputdata:
            if job.outputdata._name <> 'DQ2OutputDataset':
                raise ApplicationConfigurationError(None,'Panda backend supports only DQ2OutputDataset')
        else:
            logger.info('Adding missing DQ2OutputDataset')
            job.outputdata = DQ2OutputDataset()

        # get the list of sites that this jobset will run at
        sitesdict = {}
        bjsites = []
        if job.subjobs:
            for sj in job.subjobs:
                sitesdict[sj.backend.site] = 1
            bjsites = sitesdict.keys()
        else:
            bjsites = [job.backend.site]

        # validate the output dataset name (and make it a container)
        job.outputdata.datasetname,outlfn = dq2outputdatasetname(job.outputdata.datasetname, job.id, job.outputdata.isGroupDS, job.outputdata.groupname)
        if not job.outputdata.datasetname.endswith('/'):
            job.outputdata.datasetname+='/'

        # create the container
        Client.createContainer(job.outputdata.datasetname,False)
        logger.info('Created output container %s'%job.outputdata.datasetname)

        # store the lib datasts
        self.libDatasets = {}
        self.libraries = {}
        for site in bjsites:
            self.outDsLocation = Client.PandaSites[site]['ddm']

            tmpDSName = job.outputdata.datasetname[0:-1] + ".%s"%site

            try:
                Client.addDataset(tmpDSName,False,location=self.outDsLocation)
                logger.info('Output dataset %s registered at %s'%(tmpDSName,self.outDsLocation))
                dq2_set_dataset_lifetime(tmpDSName, self.outDsLocation)
                self.indivOutDsList = [tmpDSName]
                # add the DS to the container
                Client.addDatasetsToContainer(job.outputdata.datasetname,[tmpDSName],False)
            except exceptions.SystemExit:
                raise BackendError('Panda','Exception in adding dataset %s: %s %s'%(tmpDSName,sys.exc_info()[0],sys.exc_info()[1]))

            # handle the libds
            if job.backend.libds:
                self.libDatasets[site] = job.backend.libds
                self.fileBO = getLibFileSpecFromLibDS(self.libDatasets[site])
                self.libraries[site] = self.fileBO.lfn
            else:
                self.libDatasets[site]= tmpDSName+'.lib'
                self.libraries[site] = '%s.tgz' % self.libDatasets[site]
                try:
                    Client.addDataset(self.libDatasets[site],False,location=self.outDsLocation)
                    dq2_set_dataset_lifetime(self.libDatasets[site], self.outDsLocation)
                    logger.info('Lib dataset %s registered at %s'%(self.libDatasets[site],self.outDsLocation))
                except exceptions.SystemExit:
                    raise BackendError('Panda','Exception in Client.addDataset %s: %s %s'%(self.libDatasets[site],sys.exc_info()[0],sys.exc_info()[1]))

        # add extOutFiles
        self.extOutFile = []
        for tmpName in job.outputdata.outputdata:
            if tmpName != '':
                self.extOutFile.append(tmpName)
        for tmpName in job.backend.extOutFile:
            if tmpName != '':
                self.extOutFile.append(tmpName)

        # validate dbrelease
        self.dbrFiles,self.dbrDsList = getDBDatasets(self.job_options,'',self.dbrelease)

        # Add inputsandbox to user_area
        if job.inputsandbox:
            logger.warning("Submitting Panda job with inputsandbox. This may slow the submission slightly.")
            inpw = job.getInputWorkspace()
            inputsandbox = inpw.getPath('sources.%s.tar' % commands.getoutput('uuidgen'))

            if app.user_area.name:
                rc, output = commands.getstatusoutput('cp %s %s.gz' % (app.user_area.name, inputsandbox))
                if rc:
                    logger.error('Copying user_area failed with status %d',rc)
                    logger.error(output)
                    raise ApplicationConfigurationError(None,'Packing inputsandbox failed.')
                rc, output = commands.getstatusoutput('gunzip %s.gz' % (inputsandbox))
                if rc:
                    logger.error('Unzipping user_area failed with status %d',rc)
                    logger.error(output)
                    raise ApplicationConfigurationError(None,'Packing inputsandbox failed.')

            for fname in [os.path.abspath(f.name) for f in job.inputsandbox]:
                fname.rstrip(os.sep)
                path = os.path.dirname(fname)
                fn = os.path.basename(fname)
                ua = os.path.abspath(self.userarea)
                if ua in path:
                    fn = fname[len(ua)+1:]
                    path = ua
                rc, output = commands.getstatusoutput('tar rf %s -C %s %s' % (inputsandbox, path, fn))
                if rc:
                    logger.error('Packing inputsandbox failed with status %d',rc)
                    logger.error(output)
                    raise ApplicationConfigurationError(None,'Packing inputsandbox failed.')
            rc, output = commands.getstatusoutput('gzip %s' % (inputsandbox))
            if rc:
                logger.error('Packing inputsandbox failed with status %d',rc)
                logger.error(output)
                raise ApplicationConfigurationError(None,'Packing inputsandbox failed.')
            inputsandbox += ".gz"
        else:
            inputsandbox = app.user_area.name

        # upload sources
        if inputsandbox and not job.backend.libds and not local_libds:
            uploadSources(os.path.dirname(inputsandbox),os.path.basename(inputsandbox))

        # create build job for each needed site
        logger.info("Creating a build job for %s"%','.join(bjsites))
        bjspecs=[]
        for bjsite in bjsites:
            tmpLibDS = job.outputdata.datasetname+'.lib'
            jspec = JobSpec()
            jspec.jobDefinitionID   = job.id
            jspec.jobName           = commands.getoutput('uuidgen')
            jspec.AtlasRelease      = 'Atlas-%s' % app.atlas_release
            jspec.homepackage       = 'AnalysisTransforms'+self.cacheVer#+nightVer
            if job.backend.bexec != '':
                jspec.transformation    = '%s/buildGen-00-00-01' % Client.baseURLSUB
            else:
                jspec.transformation    = '%s/buildJob-00-00-03' % Client.baseURLSUB
            if Client.isDQ2free(bjsite) and not local_libds:
                jspec.destinationDBlock = '%s/%s' % (job.outputdata.datasetname,self.libDatasets[bjsite])
                jspec.destinationSE     = 'local'
            else:
                jspec.destinationDBlock = self.libDatasets[bjsite]
                jspec.destinationSE     = bjsite
            jspec.prodSourceLabel   = configPanda['prodSourceLabelBuild']
            jspec.processingType    = configPanda['processingType']
            jspec.assignedPriority  = configPanda['assignedPriorityBuild']
            jspec.computingSite     = bjsite
            jspec.cloud             = Client.PandaSites[bjsite]['cloud']
            jspec.jobParameters     = '-o %s' % (self.libraries[bjsite])
            if inputsandbox:
                jspec.jobParameters     += ' -i %s' % (os.path.basename(inputsandbox))
            matchURL = re.search('(http.*://[^/]+)/',Client.baseURLSSL)
            if matchURL:
                jspec.jobParameters += ' --sourceURL %s' % matchURL.group(1)
            jspec.cmtConfig         = AthenaUtils.getCmtConfig(athenaVer=app.atlas_release)
            if job.backend.bexec != '':
                jspec.jobParameters += ' --bexec "%s" ' % urllib.quote(job.backend.bexec)
                jspec.jobParameters += ' -r %s ' % '.'

            fout = FileSpec()
            fout.lfn  = self.libraries[bjsite]
            fout.type = 'output'
            fout.dataset = self.libDatasets[bjsite]
            fout.destinationDBlock = self.libDatasets[bjsite]
            jspec.addFile(fout)

            flog = FileSpec()
            flog.lfn = '%s.log.tgz' % self.libDatasets[bjsite]
            flog.type = 'log'
            flog.dataset = self.libDatasets[bjsite]
            flog.destinationDBlock = self.libDatasets[bjsite]
            if configPanda['chirpconfig']:
                flog.dispatchDBlockToken = configPanda['chirpconfig']
            jspec.addFile(flog)
            
            bjspecs.append(jspec)
        
        if local_libds:
            self.make_local_libds(jspec, inputsandbox, local_libds_nobuild)
            job.backend.libds = self.libDataset
            jspec = None 
            self.fileBO = getLibFileSpecFromLibDS(self.libDataset)

        return bjspecs

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        '''prepare the subjob specific configuration'''

        from pandatools import Client
        from pandatools import AthenaUtils
        from taskbuffer.JobSpec import JobSpec
        from taskbuffer.FileSpec import FileSpec

 
        job = app._getParent()
        masterjob = job._getRoot()

        logger.debug('AthenaPandaRTHandler prepare called for %s', job.getFQID('.'))

#       in case of a simple job get the dataset content, otherwise subjobs are filled by the splitter
        if job.inputdata and self.inputdatatype=='DQ2' and not masterjob.subjobs:
            if not job.inputdata.names:
                for guid, lfn in job.inputdata.get_contents():
                    job.inputdata.guids.append(guid)
                    job.inputdata.names.append(lfn)

        job.backend.actualCE = job.backend.site
        job.backend.requirements.cloud = Client.PandaSites[job.backend.site]['cloud']

#       if no outputdata are given
        if not job.outputdata:
            job.outputdata = DQ2OutputDataset()
        job.outputdata.datasetname = masterjob.outputdata.datasetname[0:-1]+'.%s'%job.backend.site 

        if job.inputdata and self.inputdatatype=='DQ2':
            if len(job.inputdata.dataset) > 1:
                raise ApplicationConfigurationError(None,'Multiple input datasets per subjob not supported. Use a container dataset?')

        jspec = JobSpec()
        jspec.jobDefinitionID   = masterjob.id
        jspec.jobName           = commands.getoutput('uuidgen')  
        jspec.AtlasRelease      = 'Atlas-%s' % app.atlas_release
        jspec.homepackage       = 'AnalysisTransforms'+self.cacheVer#+nightVer
        if app.atlas_exetype in ['PYARA','ARES','ROOT','EXE']:
            jspec.transformation    = '%s/runGen-00-00-02' % Client.baseURLSUB
        else:
            jspec.transformation    = '%s/runAthena-00-00-11' % Client.baseURLSUB
        if job.inputdata and self.inputdatatype=='DQ2' and not job.inputdata.tag_info:
            jspec.prodDBlock    = job.inputdata.dataset[0]
        else:
            jspec.prodDBlock    = 'NULL'
        jspec.destinationDBlock = job.outputdata.datasetname
        if Client.isDQ2free(job.backend.site):
            jspec.destinationSE = 'local'
        elif job.outputdata.location:# and (not job._getRoot().subjobs or job.id == 0):
#            logger.warning('User defined output locations not supported. Use DaTRI: https://twiki.cern.ch/twiki/bin/view/Atlas/DataTransferRequestInterface')
            jspec.destinationSE = job.outputdata.location
        else:
            jspec.destinationSE = job.backend.site
        jspec.prodSourceLabel   = configPanda['prodSourceLabelRun']
        jspec.processingType    = configPanda['processingType']
        jspec.assignedPriority  = configPanda['assignedPriorityRun']
        jspec.cloud             = job.backend.requirements.cloud
        jspec.computingSite     = job.backend.site
        if job.backend.requirements.memory != -1:
            jspec.minRamCount = job.backend.requirements.memory
        if job.backend.requirements.cputime != -1:
            jspec.maxCpuCount = job.backend.requirements.cputime
        jspec.cmtConfig         = AthenaUtils.getCmtConfig(athenaVer=app.atlas_release)

#       library (source files)
        if not job.backend.libds:
            flib = FileSpec()
            flib.lfn            = self.libraries[job.backend.site]
            flib.type           = 'input'
            flib.dataset        = self.libDatasets[job.backend.site]
            flib.dispatchDBlock = self.libDatasets[job.backend.site]
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
        if job.inputdata and self.inputdatatype=='DQ2' and not job.inputdata.tag_info:
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
        AthenaUtils.convertConfToOutput(self.runConfig,jspec,outMap,job.backend.individualOutDS,self.extOutFile)
        for file in jspec.Files:
            if file.type in ['output', 'log'] and configPanda['chirpconfig']:
                file.dispatchDBlockToken = configPanda['chirpconfig']
                logger.debug('chirp file %s',file)
       
        if job.backend.individualOutDS:
            for f in jspec.Files:
                if f.type in ['output','log']:
                    if not f.dataset in self.indivOutDsList:
                        try:
                            logger.info('Creating individualOutDS %s'%f.dataset)
                            Client.addDataset(f.dataset,False,location=self.outDsLocation)
                            dq2_set_dataset_lifetime(f.dataset, self.outDsLocation)
                            self.indivOutDsList.append(f.dataset)
                        except exceptions.SystemExit:
                            raise BackendError('Panda','Exception in Client.addDataset %s: %s %s'%(f.dataset,sys.exc_info()[0],sys.exc_info()[1]))

#       job parameters
        param = ''
        # FIXME if not options.nobuild:
        param =  '-l %s ' % self.libraries[job.backend.site]
        param += '-r %s ' % self.rundirectory
        # set jobO parameter
        if app.atlas_exetype in ['PYARA','ARES','ROOT','EXE']:
            param += '-j "" -p "%s" ' % self.job_options
        else:
            param += '-j "%s" ' % urllib.quote(self.job_options)
        if app.atlas_exetype == 'ARES':
            param += '--useAthenaPackages '
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
            # check for ELSSI files
            input_files = job.inputdata.names
            if self.inputdatatype == 'DQ2' and job.inputdata.tag_info:
                tag_file = job.inputdata.tag_info.keys()[0]
                if job.inputdata.tag_info[tag_file]['path'] != '' and job.inputdata.tag_info[tag_file]['dataset'] == '':
                    
                    # set ship input
                    param += '--shipInput '

                    # set the coll name
                    if self.runConfig.input.collRefName:
                        param += '--collRefName %s ' % self.runConfig.input.collRefName
                        
                    # sort out the input ELSSI file from the tag_info
                    input_files = ['.'.join( tag_file.split(".")[:len(tag_file.split("."))-2] )]

                    # get the GUID boundaries
                    guid_boundaries = []
                    for tag_file2 in job.inputdata.tag_info:
                        guid_boundaries.append(job.inputdata.tag_info[tag_file2]['refs'][0][2])

                    param += '--guidBoundary "%s" ' % guid_boundaries
                    
            param += '-i "%s" ' % input_files
        else:
            param += '-i "[]" '
        #param += '-m "[]" ' #%minList FIXME
        #param += '-n "[]" ' #%cavList FIXME
        #FIXME
        #if bhaloList != []:
        #    param += '--beamHalo "%s" ' % bhaloList
        #if bgasList != []:
        #    param += '--beamGas "%s" ' % bgasList
        if app.atlas_exetype in ['PYARA','ARES','ROOT','EXE']:
            outMapNew={}
            for x in outMap.values():
                outMapNew.update(dict(x))
            param += '-o "%s" ' % outMapNew
        else:
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
        nEventsToSkip = app.skip_events
        if app.atlas_exetype == 'ATHENA' and app.max_events > 0:
            param += '-f "theApp.EvtMax=%d;EventSelector.SkipEvents=%s" ' % (app.max_events,nEventsToSkip)

        #event picking 
        if len(app.run_event) >= 1:
            param += '--eventPickTxt=%s ' % app.run_event_file.split('/')[-1]

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
#        if app.atlas_exetype in ['PYARA','ARES','ROOT']:
#            param += '--trf '
#            param += '--ara '
        if job.backend.accessmode == 'FILE_STAGER':
            param += '--accessmode=filestager '
        elif job.backend.accessmode == 'DIRECT':
            param += '--accessmode=direct '
        if self.inputdatatype == 'Tier3':
            param += '--givenPFN '
 
        jspec.jobParameters = param
        
        return jspec

from Ganga.GPIDev.Credentials import GridProxy
gridProxy = GridProxy()

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

for app in ['Athena', 'ExecutableDQ2', 'RootDQ2']:
    allHandlers.add(app,'Panda',AthenaPandaRTHandler)

from Ganga.Utility.Config import getConfig, ConfigError
configDQ2 = getConfig('DQ2')
configPanda = getConfig('Panda')

from Ganga.Utility.logging import getLogger
logger = getLogger()
