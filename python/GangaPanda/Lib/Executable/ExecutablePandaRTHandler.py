###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ExecutablePandaRTHandler.py,v 1.4 2009/04/22 07:43:44 dvanders Exp $
###############################################################################
# Athena LCG Runtime Handler
#
# ATLAS/ARDA

import os, sys, pwd, commands, re, shutil, urllib, time, string, exceptions, time

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.Core import BackendError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from GangaAtlas.Lib.ATLASDataset import DQ2Dataset, DQ2OutputDataset
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2outputdatasetname

class ExecutablePandaRTHandler(IRuntimeHandler):
    '''Executable Panda Runtime Handler'''

    def master_prepare(self,app,appconfig):
        '''Prepare the master job'''

        from pandatools import Client

        job = app._getParent()
        logger.debug('ExecutablePandaRTHandler master_prepare called for %s', job.getFQID('.')) 

#       Pack inputsandbox
        inputsandbox = 'sources.%s.tar' % commands.getoutput('uuidgen') 
        inpw = job.getInputWorkspace()
        for fname in [f.name for f in job.inputsandbox]:
            fname.rstrip(os.sep)
            path = fname[:fname.rfind(os.sep)]
            f = fname[fname.rfind(os.sep)+1:]
            rc, output = commands.getstatusoutput('tar rf %s -C %s %s' % (inpw.getPath(inputsandbox), path, f))
            if rc:
                logger.error('Packing inputsandbox failed with status %d',rc)
                logger.error(output)
                raise ApplicationConfigurationError(None,'Packing inputsandbox failed.')
        if len(job.inputsandbox) > 0:
            rc, output = commands.getstatusoutput('gzip %s' % (inpw.getPath(inputsandbox)))
            if rc:
                logger.error('Packing inputsandbox failed with status %d',rc)
                logger.error(output)
                raise ApplicationConfigurationError(None,'Packing inputsandbox failed.')
            inputsandbox += ".gz"
        else:
            inputsandbox = None

#       Upload Inputsandbox
        if inputsandbox:
            logger.debug('Uploading source tarball ...')
            try:
                cwd = os.getcwd()
                os.chdir(inpw.getPath())
                rc, output = Client.putFile(inputsandbox)
                if output != 'True':
                    logger.error('Uploading inputsandbox %s failed. Status = %d', inputsandbox, rc)
                    logger.error(output)
                    raise ApplicationConfigurationError(None,'Uploading inputsandbox failed')
                self.inputsandbox = inputsandbox
            finally:
                os.chdir(cwd)
        else:
            self.inputsandbox = None

#       input dataset
        if job.inputdata:
            if job.inputdata._name <> 'DQ2Dataset':
                raise ApplicationConfigurationError(None,'PANDA application supports only DQ2Datasets')

#       output dataset
        if job.outputdata:
            if job.outputdata._name <> 'DQ2OutputDataset':
                raise ApplicationConfigurationError(None,'Panda backend supports only DQ2OutputDataset')
        else:
            logger.info('Adding missing DQ2OutputDataset')
            job.outputdata = DQ2OutputDataset()

        job.outputdata.datasetname,outlfn = dq2outputdatasetname(job.outputdata.datasetname, job.id, job.outputdata.isGroupDS, job.outputdata.groupname)

        logger.info('Output dataset %s',job.outputdata.datasetname)
        try:
            Client.addDataset(job.outputdata.datasetname,False)
        except exceptions.SystemExit:
            raise BackendError('Panda','Exception in Client.addDataset %s: %s %s'%(job.outputdata.datasetname,sys.exc_info()[0],sys.exc_info()[1]))

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

        # run brokerage here if not splitting
        if not job.splitter:
            from GangaPanda.Lib.Panda.Panda import runPandaBrokerage
            runPandaBrokerage(job)
        elif job.splitter._name not in ['DQ2JobSplitter', 'ArgSplitter', 'ArgSplitterTask']:
            raise ApplicationConfigurationError(None,'Panda splitter must be DQ2JobSplitter or ArgSplitter')
        
        if job.backend.site == 'AUTO':
            raise ApplicationConfigurationError(None,'site is still AUTO after brokerage!')

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
        #if not job.outputdata.datasetname:
        else:
            job.outputdata.datasetname = job._getRoot().outputdata.datasetname

        if not job.outputdata.datasetname:
            raise ApplicationConfigurationError(None,'DQ2OutputDataset has no datasetname')

        jspec = JobSpec()
        jspec.jobDefinitionID   = job._getRoot().id
        jspec.jobName           = commands.getoutput('uuidgen')
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
        jspec.prodSourceLabel   = 'user'
        jspec.assignedPriority  = 1000
        jspec.cloud             = cloud
        # memory
        if job.backend.requirements.memory != -1:
            jspec.minRamCount = job.backend.requirements.memory
        # cputime     
        if job.backend.requirements.cputime != -1:
            jspec.maxCpuCount = job.backend.requirements.cputime
        jspec.computingSite     = site

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
        matchURL = re.search("(http.*://[^/]+)/",Client.baseURLSSL)
        srcURL = ""
        if matchURL != None:
            srcURL = matchURL.group(1)
            param += " --sourceURL %s " % srcURL

        # FIXME if not options.nobuild:
        # set jobO parameter
        #param += '-j "%s" ' % urllib.quote(self.job_options)
        param += '-r "%s" ' % self.rundirectory
        #param += '-j "%s" ' % job.application.exe
        if self.inputsandbox:
            param += '-j "(wget %s/cache/%s || wget --no-check-certificate %s/cache/%s) && tar xzvf %s && { if [ -e %s ]; then chmod +x %s; fi; echo === executing user script ===; PATH=$PATH:. %s %s; }" ' % (srcURL, self.inputsandbox, srcURL, self.inputsandbox, self.inputsandbox, job.application.exe, job.application.exe, job.application.exe, " ".join(job.application.args))
        else:
            param += '-j "{ if [ -e %s ]; then chmod +x %s; fi; echo === executing user script ===; PATH=$PATH:. %s %s; }" ' % (job.application.exe, job.application.exe, job.application.exe, " ".join(job.application.args))
        param += '-p "" '
        #param += '-p "%s" ' % (" ".join(job.application.args))

        if job.inputdata:
            param += '-i "%s" ' % job.inputdata.names

        # source URL
        matchURL = re.search("(http.*://[^/]+)/",Client.baseURLSSL)
        srcURL = ""
        if matchURL != None:
            srcURL = matchURL.group(1)
            param += " --sourceURL %s " % srcURL

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


        # Hack runGen to download...
        #hs = "myRun-00-00-01"
        #hack = 'os.system("wget http://www.in4matiker.de/sirius/%s")\\nasys = " ".join(sys.argv[1:]).split("\\\\n")\\nos.system("chmod +x %s; ./%s %%s" %% (asys[0]+" "+asys[-1]))\\nsys.exit(0)\\n' % (hs, hs, hs)
        #hack = 'os.system("wget %s/cache/%s || wget --no-check-certificate %s/cache/%s")\\nos.system("tar xzvf %s")' % (srcURL, self.inputsandbox, srcURL, self.inputsandbox, self.inputsandbox)
        #param += "-o $'%s\\n%s' " % (outfiles, hack)
        #param += "-o '%s' " % (outfiles)

        jspec.jobParameters = param
        
        return jspec

from Ganga.GPIDev.Credentials import GridProxy
gridProxy = GridProxy()

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('Executable','Panda',ExecutablePandaRTHandler)

from Ganga.Utility.Config import getConfig, ConfigError
config = getConfig('Athena')
configDQ2 = getConfig('DQ2')

from Ganga.Utility.logging import getLogger
logger = getLogger()
