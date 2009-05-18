###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaMCLCGRTHandler.py,v 1.10.2.3 2006/11/22 14:20:53 elmsheus Exp 
###############################################################################
# AthenaMC LCG Runtime Handler
#

import os, string, commands, re, pwd, time, shutil

import imp,xml.dom.minidom,urllib,random

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.Core import FileWorkspace
from Ganga.Core.exceptions import ApplicationConfigurationError

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

from Ganga.Lib.LCG import LCGRequirements, LCGJobConfig
from GangaAtlas.Lib.AtlasLCGRequirements import AtlasLCGRequirements
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset import DQ2OutputDataset
from GangaAtlas.Lib.AthenaMC.AthenaMCDatasets import extractFileNumber, matchFile


from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

# PandaTools
from pandatools import Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
#import AthenaUtils


# the config file may have a section
# aboout monitoring

##mc = getConfig('MonitoringServices')

### None by default

##mc.addOption('AthenaMC', None, 'FIXME')
##mc.addOption('AthenaMC/LCG', None, 'FIXME')
outsite=""

class AthenaMCPandaRTHandler(IRuntimeHandler):
    """Athena MC Panda Runtime Handler"""
    # only filling a list of preformatted job specs. 

#    dsetmap,sitemap={},{}
#    userprefix=""

    
    def master_prepare(self,app,appmasterconfig):

        job = app._getParent()
        logger.debug('AthenaMCPandaRTHandler master_prepare called for %s', job.getFQID('.')) 
        usertag = configDQ2['usertag']
        #usertag='user09'
        self.libDataset = '%s.%s.ganga.%s_%d.lib._%06d' % (usertag,gridProxy.identity(),commands.getoutput('hostname').split('.')[0],int(time.time()),job.id)
#        self.userprefix='%s.%s.ganga' % (usertag,gridProxy.identity())
        sources = 'sources.%s.tar.gz' % commands.getoutput('uuidgen') 
        self.library = '%s.lib.tgz' % self.libDataset

        # check DBRelease
        # if job.backend.dbRelease != '' and job.backend.dbRelease.find(':') == -1:
         #   raise ApplicationConfigurationError(None,"ERROR : invalid argument for backend.dbRelease. Must be 'DatasetName:FileName'")

#       unpack library
        logger.debug('Creating source tarball ...')        
        tmpdir = '/tmp/%s' % commands.getoutput('uuidgen')
        os.mkdir(tmpdir)

        inputbox=[]
        if os.path.exists(app.transform_archive):
            # must add a condition on size.
            inputbox += [ File(app.transform_archive) ]
        if app.evgen_job_option:
            self.evgen_job_option=app.evgen_job_option
            if os.path.exists(app.evgen_job_option):
                # locally modified job option file to add to the input sand box
                inputbox += [ File(app.evgen_job_option) ]
                self.evgen_job_option=app.evgen_job_option.split("/")[-1]

         
#       add input sandbox files
        if (job.inputsandbox):
            for file in job.inputsandbox:
                inputbox += [ file ]
#        add option files
        for extFile in job.backend.extOutFile:
            try:
                shutil.copy(extFile,tmpdir)
            except IOError:
                os.makedirs(tmpdir)
                shutil.copy(extFile,tmpdir)
#       fill the archive
        for opt_file in inputbox:
            try:
                shutil.copy(opt_file.name,tmpdir)
            except IOError:
                os.makedirs(tmpdir)
                shutil.copy(opt_file.name,tmpdir)
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
                    
        # Use Panda's brokerage

        from GangaPanda.Lib.Panda.Panda import runPandaBrokerage
        runPandaBrokerage(job)
        if job.backend.site == 'AUTO':
            raise ApplicationConfigurationError(None,'site is still AUTO after brokerage!')
        
        cacheVer = "-AtlasProduction_" + str(app.prod_release)
            
        logger.debug("master job submit?")
        self.outsite=job.backend.site
        if app.se_name and app.se_name != "none" and not self.outsite:
            self.outsite=app.se_name

        
        #       create build job
        jspec = JobSpec()
        jspec.jobDefinitionID   = job.id
        jspec.jobName           = commands.getoutput('uuidgen')
        jspec.AtlasRelease      = 'Atlas-%s' % app.atlas_rel
        jspec.homepackage       = 'AnalysisTransforms'+cacheVer#+nightVer
        jspec.transformation    = '%s/buildJob-00-00-03' % Client.baseURLSUB # common base to Athena and AthenaMC jobs: buildJob is a pilot job which takes care of all inputs for the real jobs (in prepare()
        jspec.destinationDBlock = self.libDataset
        jspec.destinationSE     = job.backend.site
        jspec.prodSourceLabel   = 'panda'
        jspec.assignedPriority  = 2000
        jspec.computingSite     = job.backend.site
        jspec.cloud             = job.backend.requirements.cloud
#        jspec.jobParameters     = self.args not known yet
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
        #print "MASTER JOB DETAILS:",jspec.jobParameters

        return jspec
    
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        '''prepare the subjob specific configuration'''
 
        job = app._getParent()
        logger.debug('AthenaMCPandaRTHandler prepare called for %s', job.getFQID('.'))

        try:
            assert self.outsite
        except:
            logger.error("outsite not set. Aborting")
            raise Exception()
        
        job.backend.site = self.outsite
        job.backend.actualCE = self.outsite
        cloud = job._getRoot().backend.requirements.cloud
        job.backend.requirements.cloud = cloud
     

        # now just filling the job from AthenaMC data
        
        jspec = JobSpec()
        jspec.jobDefinitionID   = job._getRoot().id
        jspec.jobName           = commands.getoutput('uuidgen')  
        jspec.AtlasRelease      = 'Atlas-%s' % app.atlas_rel
        
        if app.transform_archive:
            jspec.homepackage       = 'AnalysisTransforms'+app.transform_archive
        elif app.prod_release:
            jspec.homepackage       = 'AnalysisTransforms-AtlasProduction_'+str(app.prod_release)
        jspec.transformation    = '%s/runAthena-00-00-11' % Client.baseURLSUB
            
        #---->????  prodDBlock and destinationDBlock when facing several input / output datasets?

        jspec.prodDBlock    = 'NULL'
        if job.inputdata and len(app.inputfiles)>0 and app.inputfiles[0] in app.dsetmap:
            jspec.prodDBlock    = app.dsetmap[app.inputfiles[0]]

        # How to specify jspec.destinationDBlock  when more than one type of output is available? Panda prod jobs seem to specify only the last output dataset
        outdset=""
        for type in ["EVNT","RDO","AOD"]:
            if type in app.outputpaths.keys():
                outdset=string.replace(app.outputpaths[type],"/",".")
                outdset=outdset[1:-1]
                break
        
        jspec.destinationDBlock = outdset
        jspec.destinationSE = self.outsite
        jspec.prodSourceLabel   = 'user'
        jspec.assignedPriority  = 1000
        jspec.cloud             = cloud
        # memory
        if job.backend.requirements.memory != -1:
            jspec.minRamCount = job.backend.requirements.memory
        jspec.computingSite     = self.outsite

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
        for lfn in app.inputfiles:
            useguid=app.turls[lfn].replace("guid:","")
            finp = FileSpec()
            finp.lfn            = lfn
            finp.GUID           = useguid
            finp.dataset        = app.dsetmap[lfn]
            finp.prodDBlock     = app.dsetmap[lfn]
            finp.dispatchDBlock = app.dsetmap[lfn]
            finp.type           = 'input'
            finp.status         = 'ready'
            jspec.addFile(finp)
        # add dbfiles if any:
        for lfn in app.dbfiles:
            useguid=app.dbturls[lfn].replace("guid:","")
            finp = FileSpec()
            finp.lfn            = lfn
            finp.GUID           = useguid
            finp.dataset        = app.dsetmap[lfn]
            finp.prodDBlock     = app.dsetmap[lfn]
            finp.dispatchDBlock = app.dsetmap[lfn]
            finp.type           = 'input'
            finp.status         = 'ready'
            jspec.addFile(finp)
        

#       output files( this includes the logfiles)
        # Output files
        jidtag=""
        job = app._getParent() # Returns job or subjob object
        if job._getRoot().subjobs:
            jidtag = job._getRoot().id
        else:
            jidtag = "%d" % job.id       
        outfiles=app.subjobsOutfiles[job.id]
        pandaOutfiles={}
        for type in outfiles.keys():
            pandaOutfiles[type]=outfiles[type]+"."+str(jidtag)
        #print pandaOutfiles

        for outtype in pandaOutfiles.keys():
            fout = FileSpec()
            dset=string.replace(app.outputpaths[outtype],"/",".")
            dset=dset[1:-1]
            fout.dataset=dset
            fout.lfn=pandaOutfiles[outtype]
            fout.type              = 'output'
            #            fout.destinationDBlock = jspec.destinationDBlock
            fout.destinationDBlock = fout.dataset # let's be crazy and see how it goes...
            fout.destinationSE    = jspec.destinationSE
            if outtype=='LOG':
                fout.type='log'
                fout.destinationDBlock = fout.dataset
                fout.destinationSE     = job.backend.site
            jspec.addFile(fout)


        #       job parameters
        param =  '-l %s ' % self.library # user tarball.
        # use corruption checker
        if job.backend.requirements.corCheck:
            param += '--corCheck '
        # disable to skip missing files
        if job.backend.requirements.notSkipMissing:
            param += '--notSkipMissing '
        
        # transform parameters
        # need to update arglist with final output file name...
        newArgs=[]
        if app.mode != "template":
            app.args[3]=app.args[3]+" -t "
            if app.verbosity:
                app.args[3]=app.args[3]+" -l %s " % app.verbosity

        for arg in app.args[3:]:
            for type in outfiles.keys():
                if arg.find(outfiles[type])>-1:
                    arg=arg.replace(outfiles[type],pandaOutfiles[type])

            newArgs.append(arg)
        arglist=string.join(newArgs," ")
#        print "Arglist:",arglist

        param += ' -r ./ '
        param += ' -j "%s"' % urllib.quote(arglist)

        allinfiles=app.inputfiles+app.dbfiles
        # Input files.
        param += ' -i "%s" ' % allinfiles
        if len(app.mbfiles)>0:
            param+= ' -m "%s" ' % app.mbfiles
        if len(app.cavernfiles)>0:
            param+= ' -n "%s" ' % app.cavernfiles
        #        param += '-m "[]" ' #%minList FIXME
        #        param += '-n "[]" ' #%cavList FIXME

        del pandaOutfiles["LOG"] # logfiles do not appear in IROOT block, and this one is not needed anymore...
        param += ' -o "{\'IROOT\':%s }"' % str(pandaOutfiles.items())

        # source URL        
        matchURL = re.search("(http.*://[^/]+)/",Client.baseURLSSL)
        if matchURL != None:
            param += " --sourceURL %s " % matchURL.group(1)
        param += " --trf"


        jspec.jobParameters = param
        jspec.metadata="--trf \"%s\"" % arglist

        #print "SUBJOB DETAILS:",jspec.values()

        return jspec


from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('AthenaMC','Panda',AthenaMCPandaRTHandler)

from Ganga.GPIDev.Credentials import GridProxy
gridProxy = GridProxy()

from Ganga.Utility.Config import getConfig, ConfigError
config = getConfig('AthenaMC')
configDQ2 = getConfig('DQ2')

from Ganga.Utility.logging import getLogger
logger = getLogger()
