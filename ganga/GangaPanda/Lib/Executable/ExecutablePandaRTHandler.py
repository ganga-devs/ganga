###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ExecutablePandaRTHandler.py,v 1.4 2009/04/22 07:43:44 dvanders Exp $
###############################################################################
# Athena LCG Runtime Handler
#
# ATLAS/ARDA

import os, sys, pwd, commands, re, shutil, urllib, time, string, exceptions, time

from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.Core.exceptions import BackendError
from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Schema import *
from GangaCore.GPIDev.Lib.File import *

from GangaAtlas.Lib.ATLASDataset import DQ2Dataset, DQ2OutputDataset
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2outputdatasetname
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_set_dataset_lifetime

from GangaPanda.Lib.Panda.Panda import setChirpVariables, uploadSources

class ExecutablePandaRTHandler(IRuntimeHandler):
    '''Executable Panda Runtime Handler'''

    def master_prepare(self,app,appconfig):
        '''Prepare the master job'''

        from pandatools import Client
        from taskbuffer.JobSpec import JobSpec
        from taskbuffer.FileSpec import FileSpec

        job = app._getParent()
        logger.debug('ExecutablePandaRTHandler master_prepare called for %s', job.getFQID('.')) 

        # set chirp variables
        if configPanda['chirpconfig'] or configPanda['chirpserver']:
            setChirpVariables()

#       Pack inputsandbox
        inputsandbox = 'sources.%s.tar' % commands.getoutput('uuidgen 2> /dev/null') 
        inpw = job.getInputWorkspace()
        # add user script to inputsandbox
        if hasattr(job.application.exe, "name"):
            if not job.application.exe in job.inputsandbox:
                job.inputsandbox.append(job.application.exe)

        for fname in [f.name for f in job.inputsandbox]:
            fname.rstrip(os.sep)
            path = fname[:fname.rfind(os.sep)]
            f = fname[fname.rfind(os.sep)+1:]
            rc, output = commands.getstatusoutput('tar rf %s -C %s %s' % (inpw.getPath(inputsandbox), path, f))
            if rc:
                logger.error('Packing inputsandbox failed with status %d',rc)
                logger.error(output)
                raise ApplicationConfigurationError('Packing inputsandbox failed.')
        if len(job.inputsandbox) > 0:
            rc, output = commands.getstatusoutput('gzip %s' % (inpw.getPath(inputsandbox)))
            if rc:
                logger.error('Packing inputsandbox failed with status %d',rc)
                logger.error(output)
                raise ApplicationConfigurationError('Packing inputsandbox failed.')
            inputsandbox += ".gz"
        else:
            inputsandbox = None

#       Upload Inputsandbox
        if inputsandbox:
            logger.debug('Uploading source tarball ...')
            uploadSources(inpw.getPath(),os.path.basename(inputsandbox))
            self.inputsandbox = inputsandbox
        else:
            self.inputsandbox = None

#       input dataset
        if job.inputdata:
            if job.inputdata._name != 'DQ2Dataset':
                raise ApplicationConfigurationError('PANDA application supports only DQ2Datasets')

        # run brokerage here if not splitting
        if not job.splitter:
            from GangaPanda.Lib.Panda.Panda import runPandaBrokerage
            runPandaBrokerage(job)
        elif job.splitter._name not in ['DQ2JobSplitter', 'ArgSplitter', 'ArgSplitterTask']:
            raise ApplicationConfigurationError('Panda splitter must be DQ2JobSplitter or ArgSplitter')
        
        if job.backend.site == 'AUTO':
            raise ApplicationConfigurationError('site is still AUTO after brokerage!')

#       output dataset
        if job.outputdata:
            if job.outputdata._name != 'DQ2OutputDataset':
                raise ApplicationConfigurationError('Panda backend supports only DQ2OutputDataset')
        else:
            logger.info('Adding missing DQ2OutputDataset')
            job.outputdata = DQ2OutputDataset()

        job.outputdata.datasetname,outlfn = dq2outputdatasetname(job.outputdata.datasetname, job.id, job.outputdata.isGroupDS, job.outputdata.groupname)

        self.outDsLocation = Client.PandaSites[job.backend.site]['ddm']

        try:
            Client.addDataset(job.outputdata.datasetname,False,location=self.outDsLocation)
            logger.info('Output dataset %s registered at %s'%(job.outputdata.datasetname,self.outDsLocation))
            dq2_set_dataset_lifetime(job.outputdata.datasetname, location=self.outDsLocation)
        except exceptions.SystemExit:
            raise BackendError('Panda','Exception in Client.addDataset %s: %s %s'%(job.outputdata.datasetname,sys.exc_info()[0],sys.exc_info()[1]))

        # handle the libds
        if job.backend.libds:
            self.libDataset = job.backend.libds
            self.fileBO = getLibFileSpecFromLibDS(self.libDataset)
            self.library = self.fileBO.lfn
        elif job.backend.bexec:
            self.libDataset = job.outputdata.datasetname+'.lib'
            self.library = '%s.tgz' % self.libDataset
            try:
                Client.addDataset(self.libDataset,False,location=self.outDsLocation)
                dq2_set_dataset_lifetime(self.libDataset, location=self.outDsLocation)
                logger.info('Lib dataset %s registered at %s'%(self.libDataset,self.outDsLocation))
            except exceptions.SystemExit:
                raise BackendError('Panda','Exception in Client.addDataset %s: %s %s'%(self.libDataset,sys.exc_info()[0],sys.exc_info()[1]))

        # collect extOutFiles
        self.extOutFile = []
        for tmpName in job.outputdata.outputdata:
            if tmpName != '':
                self.extOutFile.append(tmpName)

        for tmpName in job.outputsandbox:
            if tmpName != '':
                self.extOutFile.append(tmpName)

        for tmpName in job.backend.extOutFile:
            if tmpName != '':
                self.extOutFile.append(tmpName)

        # create build job
        if job.backend.bexec != '':
            jspec = JobSpec()
            jspec.jobDefinitionID   = job.id
            jspec.jobName           = commands.getoutput('uuidgen 2> /dev/null')
            jspec.transformation    = '%s/buildGen-00-00-01' % Client.baseURLSUB
            if Client.isDQ2free(job.backend.site):
                jspec.destinationDBlock = '%s/%s' % (job.outputdata.datasetname,self.libDataset)
                jspec.destinationSE     = 'local'
            else:
                jspec.destinationDBlock = self.libDataset
                jspec.destinationSE     = job.backend.site
            jspec.prodSourceLabel   = configPanda['prodSourceLabelBuild']
            jspec.processingType    = configPanda['processingType']
            jspec.assignedPriority  = configPanda['assignedPriorityBuild']
            jspec.computingSite     = job.backend.site
            jspec.cloud             = job.backend.requirements.cloud
            jspec.jobParameters     = '-o %s' % (self.library)
            if self.inputsandbox:
                jspec.jobParameters     += ' -i %s' % (self.inputsandbox)
            else:
                raise ApplicationConfigurationError('Executable on Panda with build job defined, but inputsandbox is emtpy !')
            matchURL = re.search('(http.*://[^/]+)/',Client.baseURLCSRVSSL)
            if matchURL:
                jspec.jobParameters += ' --sourceURL %s ' % matchURL.group(1)
            if job.backend.bexec != '':
                jspec.jobParameters += ' --bexec "%s" ' % urllib.quote(job.backend.bexec)
                jspec.jobParameters += ' -r %s ' % '.'
                

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
            return jspec
        else:
            return None

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
                
                contents = job.inputdata.get_contents(overlap=False, size=True)

                for ds in contents.keys():

                    for f in contents[ds]:
                        job.inputdata.guids.append( f[0] )
                        job.inputdata.names.append( f[1][0] )
                        job.inputdata.sizes.append( f[1][1] )
                        job.inputdata.checksums.append( f[1][2] )
                        job.inputdata.scopes.append( f[1][3] )


        site = job._getRoot().backend.site
        job.backend.site = site
        job.backend.actualCE = site
        cloud = job._getRoot().backend.requirements.cloud
        job.backend.requirements.cloud = cloud

#       if no outputdata are given
        if not job.outputdata:
            job.outputdata = DQ2OutputDataset()
            job.outputdata.datasetname = job._getRoot().outputdata.datasetname
        #if not job.outputdata.datasetname:
        else:
            job.outputdata.datasetname = job._getRoot().outputdata.datasetname

        if not job.outputdata.datasetname:
            raise ApplicationConfigurationError('DQ2OutputDataset has no datasetname')

        jspec = JobSpec()
        jspec.jobDefinitionID   = job._getRoot().id
        jspec.jobName           = commands.getoutput('uuidgen 2> /dev/null')
        jspec.transformation    = '%s/runGen-00-00-02' % Client.baseURLSUB
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
        # memory
        if job.backend.requirements.memory != -1:
            jspec.minRamCount = job.backend.requirements.memory
        # cputime     
        if job.backend.requirements.cputime != -1:
            jspec.maxCpuCount = job.backend.requirements.cputime
        jspec.computingSite     = site

#       library (source files)
        if job.backend.libds:
            flib = FileSpec()
            flib.lfn            = self.fileBO.lfn
            flib.GUID           = self.fileBO.GUID
            flib.type           = 'input'
            flib.status         = self.fileBO.status
            flib.dataset        = self.fileBO.destinationDBlock
            flib.dispatchDBlock = self.fileBO.destinationDBlock
            jspec.addFile(flib)
        elif job.backend.bexec:
            flib = FileSpec()
            flib.lfn            = self.library
            flib.type           = 'input'
            flib.dataset        = self.libDataset
            flib.dispatchDBlock = self.libDataset
            jspec.addFile(flib)

#       input files FIXME: many more input types
        if job.inputdata:            
            for guid, lfn, size, checksum, scope in zip(job.inputdata.guids,job.inputdata.names,job.inputdata.sizes, job.inputdata.checksums, job.inputdata.scopes):
                finp = FileSpec()
                finp.lfn            = lfn
                finp.GUID           = guid
                finp.scope          = scope
                
#            finp.fsize =
#            finp.md5sum =
                finp.dataset        = job.inputdata.dataset[0]
                finp.prodDBlock     = job.inputdata.dataset[0]
                finp.dispatchDBlock = job.inputdata.dataset[0]
                finp.type           = 'input'
                finp.status         = 'ready'
                jspec.addFile(finp)

#       output files
#        outMap = {}
        
        #FIXME: if options.outMeta != []:
        self.rundirectory = "."

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

        # source URL
        matchURL = re.search("(http.*://[^/]+)/",Client.baseURLCSRVSSL)
        srcURL = ""
        if matchURL is not None:
            srcURL = matchURL.group(1)
            param += " --sourceURL %s " % srcURL

        param += '-r "%s" ' % self.rundirectory

        exe_name = job.application.exe
        if job.backend.bexec == '':
            if hasattr(job.application.exe, "name"):
                exe_name = os.path.basename(job.application.exe.name)

            # set jobO parameter
            if job.application.args:
                param += ' -j "" -p "%s %s" '%(exe_name,urllib.quote(" ".join(job.application.args)))
            else:
                param += ' -j "" -p "%s" '%exe_name
            if self.inputsandbox:
                param += ' -a %s '%self.inputsandbox

        else:
            param += '-l %s ' % self.library
            param += '-j "" -p "%s %s" ' % ( exe_name,urllib.quote(" ".join(job.application.args)))

        if job.inputdata:
            param += '-i "%s" ' % job.inputdata.names

        # fill outfiles
        outfiles = {}
        for f in self.extOutFile:
            tarnum = 1
            if f.find('*') != -1:
            # archive *
                outfiles[f] = "outputbox%i.%s.%s.tar.gz" % (tarnum, job.getFQID('.'), time.strftime("%Y%m%d%H%M%S") )
                tarnum += 1
            else:
                outfiles[f] = "%s.%s.%s" %(f, job.getFQID('.'), time.strftime("%Y%m%d%H%M%S"))

            fout = FileSpec()
            fout.lfn = outfiles[f]
            fout.type = 'output'
            fout.dataset           = job.outputdata.datasetname
            fout.destinationDBlock = job.outputdata.datasetname
            fout.destinationSE     = job.backend.site
            jspec.addFile(fout)

        param += '-o "%s" ' % (outfiles) # must be double quotes, because python prints strings in 'single quotes' 

        for file in jspec.Files:
            if file.type in [ 'output', 'log'] and configPanda['chirpconfig']:
                file.dispatchDBlockToken = configPanda['chirpconfig']
                logger.debug('chirp file %s',file)

        jspec.jobParameters = param
        
        return jspec

from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('Executable','Panda',ExecutablePandaRTHandler)

from GangaCore.Utility.Config import getConfig, ConfigError
config = getConfig('Athena')
configDQ2 = getConfig('DQ2')
configPanda = getConfig('Panda')

from GangaCore.Utility.logging import getLogger
logger = getLogger()
