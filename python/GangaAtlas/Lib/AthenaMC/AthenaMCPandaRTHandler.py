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
import Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
#import AthenaUtils


# the config file may have a section
# aboout monitoring

##mc = getConfig('MonitoringServices')

### None by default

##mc.addOption('AthenaMC', None, 'FIXME')
##mc.addOption('AthenaMC/LCG', None, 'FIXME')


class AthenaMCPandaRTHandler(IRuntimeHandler):
    """Athena MC Panda Runtime Handler"""
    # only filling a list of preformatted job specs. 

    turls,cavern_turls,minbias_turls,dbturls={},{},{},{}
    dsetmap,sitemap={},{}

    lfcs,cavern_lfcs,minbias_lfcs,dblfcs={},{},{},{}
    sites,cavern_sites,minbias_sites,dbsites=[],[],[],[]
    outputlocation,lfchosts,lfcstrings={},{},{}
    outsite,outlfc,outlfc2="","",""
    outputpaths,fileprefixes={},{}
    evgen_job_option=""
    prod_release=""
    atlas_rel=""
    userprefix=""
    
    def master_prepare(self,app,appmasterconfig):

        job = app._getParent()
        logger.debug('AthenaMCPandaRTHandler master_prepare called for %s', job.getFQID('.')) 
        usertag = configDQ2['usertag']
        #usertag='user09'
        self.libDataset = '%s.%s.ganga.%s_%d.lib._%06d' % (usertag,gridProxy.identity(),commands.getoutput('hostname').split('.')[0],int(time.time()),job.id)
        self.userprefix='%s.%s.ganga' % (usertag,gridProxy.identity())
        sources = 'sources.%s.tar.gz' % commands.getoutput('uuidgen') 
        self.library = '%s.lib.tgz' % self.libDataset

        # validate parameters
        if string.count(app.atlas_release,".")==3:
            self.prod_release=app.atlas_release
            imax=string.rfind(app.atlas_release,".")
            self.atlas_rel=app.atlas_release[:imax]
        else:
            self.atlas_rel=app.atlas_release
        # check DBRelease
        if job.backend.dbRelease != '' and job.backend.dbRelease.find(':') == -1:
            raise ApplicationConfigurationError(None,"ERROR : invalid argument for backend.dbRelease. Must be 'DatasetName:FileName'")

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
        extFile2=os.environ["PANDA_SYS"]+"/etc/panda/share/extPoolRefs.C"
        #        if os.path.exists(extFile2):
        # locally modified job option file to add to the input sand box
        #            inputbox += [ File(extFile2) ]
         
#       add input sandbox files
        if (job.inputsandbox):
            for file in job.inputsandbox:
                inputbox += [ file ]
#        add option files
        for extFile in job.backend.extFile:
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

        
        if string.count(app.atlas_release,".")==3:
            self.prod_release=app.atlas_release
            imax=string.rfind(app.atlas_release,".")
            self.atlas_rel=app.atlas_release[:imax]
        else:
            self.atlas_rel=app.atlas_release
            
        # note: either self.prod_release or app.transform_archive must be set! Otherwise, you won't be able to get any transform!
        try:
            assert self.prod_release or app.transform_archive
        except:
            logger.error("A reference to the Production archive to be used must be set, either through the declaration of the archive itself in application.transform_archive or by putting the 4-digit production cache release number in application.atlas_release. Neither are set. Aborting.")
            raise
        job = app._getParent()

        # checking se-name: must not write to MC/DATA/PRODDISK space tokens.
        if not app.se_name:
            app.se_name='none'
            # important to avoid having null string in app.se_name as it would ruin the argument list of the wrapper script!
            
        if app.se_name:
            forbidden_spacetokens=["MCDISK","DATADISK","MCTAPE","DATATAPE","PRODDISK","PRODTAPE"]
            for token in forbidden_spacetokens:
                try:
                    assert token not in app.se_name
                except:
                    logger.error("You are not allowed to write output data in any production space token: %s. Please select a site with ATLASUSERDISK or ATLASLOCALGROUPDISK space token" % app.se_name)
                    raise

        # input data
        
        if not app.dryrun and job.inputdata and job.inputdata._name == 'AthenaMCInputDatasets':
            # The input dataset was already read in in AthenaMC.master_configure
            self.turls=app.turls
            self.lfcs=app.lfcs
            self.sites=app.sites
            logger.debug("turls: %s " % str(self.turls))
            logger.debug("lfcs: %s " % str(self.lfcs))
            logger.debug("sites: %s " % str(self.sites))
                    
            # handling cavern and minbias data now.
            if job.inputdata.cavern:
                inputdata=job.inputdata.get_cavern_dataset(app)
                if len(inputdata)!= 3:
                    raise  ApplicationConfigurationError(None,"Error, wrong format for inputdata %d, %s. Input files not found" % (len(inputdata),inputdata))
                self.cavern_turls=inputdata[0]
                self.cavern_lfcs=inputdata[1]
                self.cavern_sites=inputdata[2]
            if job.inputdata.minbias:
                inputdata=job.inputdata.get_minbias_dataset(app)
                if len(inputdata)!= 3:
                    raise  ApplicationConfigurationError(None,"Error, wrong format for inputdata %d, %s. Input files not found" % (len(inputdata),inputdata))
                self.minbias_turls=inputdata[0]
                self.minbias_lfcs=inputdata[1]
                self.minbias_sites=inputdata[2]

        # Add db release to input data if relevant
        dbrelease=""
        if app.extraArgs:    
            arglist=string.split(app.extraArgs)
            for arg in arglist:
                key,val=string.split(arg,"=")
                digval=string.replace(val,".","0")
                if key=="DBRelease" and digval.isdigit():
                    dbrelease=val
                    break
        if dbrelease:
            logger.debug("Detected numeric value for DBRelease. Looking for match in DQ2 database")
            if not job.inputdata:
                job.inputdata=AthenaMCInputDatasets()
            inputdata=job.inputdata.get_DBRelease(app,dbrelease)
            if len(inputdata)!= 3:
                raise  ApplicationConfigurationError(None,"Error, wrong format for inputdata %d, %s. Input files not found" % (len(inputdata),inputdata))
            self.dbturls=inputdata[0]
            self.dblfcs=inputdata[1]
            self.dbsites=inputdata[2]

        # doing output data now
        self.fileprefixes,self.outputpaths=job.outputdata.prep_data(app)
        expected_datasets=""
        for filetype in self.outputpaths.keys():
            dataset=string.replace(self.outputpaths[filetype],"/",".")
            imax=string.find(self.outputpaths[filetype],".jid")
            if imax>-1:
                self.outputpaths[filetype]=self.outputpaths[filetype][:imax]
            if dataset[0]==".": dataset=dataset[1:]
            if dataset[-1]==".": dataset=dataset[:-1]
            expected_datasets+=dataset+","
            #        if not job.outputdata.output_dataset:
        if not job.outputdata.output_dataset or string.find(job.outputdata.output_dataset,",") > 0 : #update only if output_dataset is not used to force the output dataset names.
            job.outputdata.output_dataset=expected_datasets[:-1] # removing final coma.
        
        # Use Panda's brokerage

        from GangaPanda.Lib.Panda.Panda import runPandaBrokerage
        runPandaBrokerage(job)
        if job.backend.site == 'AUTO':
            raise ApplicationConfigurationError(None,'site is still AUTO after brokerage!')
        
        if job.backend.site:
            self.outsite=job.backend.site
            job.application.se_name=job.backend.site
        elif job.application.se_name:
            self.outsite=job.application.se_name

        
        cacheVer = "-AtlasProduction_" + str(self.prod_release)
            
        logger.debug("master job submit?")


        
        #       create build job
        jspec = JobSpec()
        jspec.jobDefinitionID   = job.id
        jspec.jobName           = commands.getoutput('uuidgen')
        jspec.AtlasRelease      = 'Atlas-%s' % self.atlas_rel
        jspec.homepackage       = 'AnalysisTransforms'+cacheVer#+nightVer
#        jspec.homepackage       = 'AnalysisTransforms'
#        if self.prod_release:
#            jspec.homepackage       = 'UserProduction-%s' % self.prod_release
#        else:
#            jspec.homepackage       = app.transform_archive
        jspec.transformation    = '%s/buildJob-00-00-03' % Client.baseURLSUB # common base to Athena and AthenaMC jobs: buildJob is a pilot job which takes care of all inputs for the real jobs (in prepare()
        jspec.destinationDBlock = self.libDataset
        jspec.destinationSE     = job.backend.site
        jspec.prodSourceLabel   = 'panda'
        jspec.assignedPriority  = 2000
        jspec.computingSite     = job.backend.site
        jspec.cloud             = job.backend.cloud
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
    

    ##### methods for prepare() (individual jobs and subjobs) #####
    def getEvgenArgs(self,app):
        """prepare args vector for evgen mode"""

        if not app.transform_script:
            app.transform_script="csc_evgen_trf.py"
##        args =  [ self.atlas_rel,
##                  app.se_name,
##                  self.outputfiles["LOG"],
        args = [ app.transform_script,
                 "runNumber=%s" % str(app.run_number),
                 "firstEvent=%s" % str(self.firstevent),
                 "maxEvents=%s" % str(self.number_events_job),
                 "randomSeed=%s" % str(self.randomseed),
                 "jobConfig=%s" % self.evgen_job_option,
                 "outputEvgenFile=%s" % self.outputfiles["EVNT"]
                 ]


        if "HIST" in self.outputfiles:
            args.append("histogramFile=%s" % self.outputfiles["HIST"]) # validation histos on request only for csc_evgen_trf.py
        if "NTUP" in self.outputfiles:
            args.append("ntupleFile=%s" % self.outputfiles["NTUP"])
        if self.inputfile:
            args.append("inputGeneratorFile=%s" % self.inputfile)


        return args
    
    def getSimulArgs(self,app):
        """prepare args vector for simul-digit mode"""
        skip=str(self.firstevent-1)
        if not app.transform_script:
            app.transform_script="csc_simul_trf.py"

            #        args = [ self.atlas_rel,
            #                 app.se_name,
            #                 self.outputfiles["LOG"],
        args = [ app.transform_script,
                 "inputEvgenFile=%s" % self.inputfile, # already quoted by construction
                 "outputHitsFile=%s" % self.outputfiles["HITS"],
                 "outputRDOFile=%s" % self.outputfiles["RDO"],
                 "maxEvents=%s" % str(self.number_events_job),
                 "skipEvents=%s" % str(skip),
                 "randomSeed=%s" % str(self.randomseed),
                 "geometryVersion=%s" % app.geometryTag
                 ]
        if self.atlas_rel >="12.0.5" :
            args.append("triggerConfig=%s" % app.triggerConfig)
        if self.atlas_rel >="13" and not "digiSeedOffset" in app.extraArgs:
            random.seed(int(self.randomseed))
            app.extraArgs += ' digiSeedOffset1=%s digiSeedOffset2=%s ' % (random.randint(1,2**15),random.randint(1,2**15))
        
        return args

        
    def getReconArgs(self,app):
        """prepare args vector for recon mode"""
        skip=str(self.firstevent-1)
        if not app.transform_script:
            app.transform_script="csc_reco_trf.py"
            #        args = [ self.atlas_rel,
            #                 app.se_name,
            #                 self.outputfiles["LOG"],
        args = [ app.transform_script,
                 "inputRDOFile=%s" % self.inputfile,
                 "maxEvents=%s" % str(self.number_events_job),
                 "skipEvents=%s" % str(skip),
                 "geometryVersion=%s" % app.geometryTag
                 ]
        if "ESD" in self.outputfiles and self.outputfiles["ESD"].upper() != "NONE":
            args.append("outputESDFile=%s" % self.outputfiles["ESD"])
        if "AOD" in self.outputfiles and self.outputfiles["AOD"].upper() != "NONE":
            args.append("outputAODFile=%s" % self.outputfiles["AOD"])

        if self.atlas_rel >="12.0.5" :
            args.append("ntupleFile=%s" %  self.outputfiles["NTUP"])
            args.append("triggerConfig=%s" % app.triggerConfig)

        return args

    def getTemplateArgs(self,app):
        """prepare args vector for template mode"""
        try:
            assert app.transform_script
        except AssertionError:
            logger.error("template mode requires the name of the transformation you want to use")
            raise
        
        logger.warning("Using the new template mode. Please use application.extraArgs and application.extraIncArgs for the transformation parameters")

##        try:
##            assert "LOG" in self.outputfiles
##        except AssertionError:
##            logger.error("template mode requires a logfile, set by job.application.outputdata.logfile")
##            raise

##        args =  [ self.atlas_rel,
##                 app.se_name,
##                 self.outputfiles["LOG"],
##                 app.transform_script
##                 ]
        args = [ app.transform_script ]
        if "EVNT" in self.outputfiles and self.outputfiles["EVNT"].upper() != "NONE":
            args.append("outputEvgenFile=%s" % self.outputfiles["HIST"]) 
        if "HIST" in self.outputfiles and self.outputfiles["HIST"].upper() != "NONE":
            args.append("histogramFile=%s" % self.outputfiles["HIST"]) 
        if "HITS" in self.outputfiles and self.outputfiles["HITS"].upper() != "NONE":
            args.append("outputHitsFile=%s" % self.outputfiles["HITS"]) 
        if "RDO" in self.outputfiles and self.outputfiles["RDO"].upper() != "NONE":
            args.append("outputRDOFile=%s" % self.outputfiles["RDO"]) 
        if "ESD" in self.outputfiles and self.outputfiles["ESD"].upper() != "NONE":
            args.append("outputESDFile=%s" % self.outputfiles["ESD"])
        if "AOD" in self.outputfiles and self.outputfiles["AOD"].upper() != "NONE":
            args.append("outputAODFile=%s" % self.outputfiles["AOD"])
        if "NTUP" in self.outputfiles and self.outputfiles["NTUP"].upper() != "NONE":
            args.append("ntupleFile=%s" % self.outputfiles["NTUP"])
        return args
        
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
        cloud = job._getRoot().backend.cloud
        job.backend.cloud = cloud
     
        partition = app.getPartitionList()[0][0] # This function either throws an exception or returns at least one element
        
        job = app._getParent() # Returns job or subjob object

        self.randomseed = app.random_seed

        (self.firstevent, self.number_events_job) = app.getFirstEvent(partition, job.inputdata)
        logger.debug("partition %i, first event is %i, processing %i events" % (partition,self.firstevent, self.number_events_job))
        
        inputnumbers = app.getInputsForPartitions([partition], job._getRoot().inputdata) # getInputsForPartitions get the subset of inputfiles needed by partition i. So far so good. 
        if inputnumbers:
            matchrange = (job._getRoot().inputdata.numbersToMatcharray(inputnumbers), False)
        else:
            matchrange = ([],False)
        logger.debug("partition %i using input partitions: %s as files: %s" % (partition, inputnumbers, matchrange[0]))
        
        inputfiles = [fn for fn in self.turls.keys() if matchFile(matchrange, fn)]
        inputfiles.sort()
        
        # Strict matching must be discarded if neither splitter.input_partitions nor inputdata.redefine_partitions are used.
        
        if job._getRoot().inputdata and job._getRoot().inputdata.redefine_partitions == "" and job._getRoot().splitter and job._getRoot().splitter.input_partitions == "":
            inputfiles=[]
            inlfns=self.turls.keys()
            inlfns.sort()
            for i in inputnumbers:
                try:
                    assert len(inlfns)>= i
                except:
                    logger.error("Not enough input files, got %i expected %i" % (len(inlfns),i))
                    raise Exception()

                inputfiles.append(inlfns[i-1])

        
        if not app.dryrun and len(inputfiles) < len(inputnumbers):
            if len(inputfiles) > 0:
               missing = []
               for fn in matchrange[0]:
                   found = False
                   for infile in inputfiles:
                       if fn in infile: 
                           found = True
                           break
                   if not found:
                       missing.append(fn)
               logger.warning("Not all input files for partition %i found! Missing files: %s" % (partition, missing))
            else:
               raise ApplicationConfigurationError(None,"No input files for partition %i found ! Files expected: %s" % (partition, matchrange[0]))

        self.inputfile,self.cavernfile,self.minbiasfile="","",""

        
        for infile in inputfiles:
            self.dsetmap[infile]=self.lfcs.keys()[0]
            #            sitemap[infile]=string.join(self.sites," ")
            self.sitemap[infile]=self.sites[0]
        self.inputfile=",".join(inputfiles)
        # adding cavern/minbias/dbrelease to the mapping
        cavernfiles= self.cavern_turls.keys()
        for infile in cavernfiles:
            self.dsetmap[infile]=self.cavern_lfcs.keys()[0]
            #            sitemap[infile]=string.join(self.cavern_sites," ")
            self.sitemap[infile]=self.cavern_sites[0]
        mbfiles= self.minbias_turls.keys()
        for infile in mbfiles:
            self.dsetmap[infile]=self.minbias_lfcs.keys()[0]
            #           sitemap[infile]=string.join(self.minbias_sites," ")
            self.sitemap[infile]=self.minbias_sites[0]
        dbfiles=self.dbturls.keys()
        for infile in dbfiles:
            self.dsetmap[infile]=self.dblfcs.keys()[0]
            #            sitemap[infile]=string.join(self.dbsites," ")
            self.sitemap[infile]=self.dbsites[0]
        random.shuffle(cavernfiles)
        if job.inputdata and len(cavernfiles) >0 and job.inputdata.n_cavern_files_job:
            imax=job.inputdata.n_cavern_files_job
            try:
                assert len(cavernfiles)>= imax
            except:
                raise ApplicationConfigurationError(None,"Not enough cavern input files to sustend a single job (expected %d got %d). Aborting" %(imax,len(cavernfiles)))
            self.cavernfile=",".join([cavernfiles[i] for i in range(imax)])
            cavernfiles=cavernfiles[:imax]

        random.shuffle(mbfiles)
        if job.inputdata and len(mbfiles) >0 and job.inputdata.n_minbias_files_job:
            imax=job.inputdata.n_minbias_files_job
            try:
                assert len(mbfiles)>= imax
            except:
                raise ApplicationConfigurationError(None,"Not enough minbias input files to sustend a single job (expected %d got %d). Aborting" %(imax,len(mbfiles)))
            self.minbiasfile=",".join([mbfiles[i] for i in range(imax)])
            mbfiles=mbfiles[:imax]

        alllfns=inputfiles+cavernfiles+mbfiles+dbfiles
        infilenr=0

# now doing output files....
        self.outputfiles={}
        outpartition = partition + job._getRoot().outputdata.output_firstfile - 1
        jobid=str(job.id)
        if job.master:
            jobid=str(job.master.id)
        for filetype in self.fileprefixes.keys():
            if filetype=="LOG":
                self.outputfiles["LOG"]=self.userprefix+"."+self.fileprefixes["LOG"]+"._%5.5d.job.log.%s" % (outpartition,jobid) 
            elif  filetype=="HIST":
                self.outputfiles["HIST"]=self.userprefix+"."+self.fileprefixes["HIST"]+"._%5.5d.hist.root.%s" % (outpartition,jobid)
            elif  filetype=="NTUP":
                self.outputfiles["NTUP"]=self.userprefix+"."+self.fileprefixes["NTUP"]+"._%5.5d.root.%s" % (outpartition,jobid)
            else:
                self.outputfiles[filetype]=self.userprefix+"."+self.fileprefixes[filetype]+"._%5.5d.pool.root.%s" % (outpartition,jobid)
            # add the final lfn to the expected output list
            if self.outputfiles[filetype].upper() != "NONE":
                logger.debug("adding %s to list of expected output" % self.outputfiles[filetype])
                job.outputdata.expected_output.append(self.outputfiles[filetype])

        expected_datasets=""
        for filetype in self.outputpaths.keys():
            dataset=string.replace(self.outputpaths[filetype],"/",".")
            if dataset[0]==".": dataset=dataset[1:]
            if dataset[-1]==".": dataset=dataset[:-1]
            expected_datasets+=dataset+","
        if not job.outputdata.output_dataset or string.find(job.outputdata.output_dataset,",") > 0 :
            # if not job.outputdata.output_dataset:
            job.outputdata.output_dataset=expected_datasets[:-1] # removing final coma.
        args=[]
        # Fill arg list and output data vars depending on the prod mode 
        if app.mode=='evgen':
            args=self.getEvgenArgs(app)
        elif app.mode=='simul':
            args=self.getSimulArgs(app)
        elif app.mode=='recon':
            args=self.getReconArgs(app)
        elif app.mode=='template':
            args=self.getTemplateArgs(app)

        if app.extraArgs:    
            #            args.append(app.extraArgs)
            #        need to scan for $entries...
            arglist=string.split(app.extraArgs)
            NewArgstring=""
            for arg in arglist:
                key,val=string.split(arg,"=")
                digval=string.replace(val,".","0")
                if key=="DBRelease" and digval.isdigit():
                    dbfile="DBRelease-%s.tar.gz" % val
                    NewArgstring=NewArgstring+"DBRelease=%s " % dbfile
                    continue
                imin=string.find(val,"$")
                imin2=string.find(val,"$out")
                newval=""
                if imin>-1:
                    if string.find(val[imin+1:],"inputfile")>-1:
                        newval=self.inputfile
                    if string.find(val[imin+1:],"cavern")>-1:
                        newval=self.cavernfile
                    if string.find(val[imin+1:],"minbias")>-1:
                        newval=self.minbiasfile
                    if string.find(val[imin+1:],"first")>-1:
                        newval=str(self.firstevent)
                    if string.find(val[imin+1:],"skip")>-1:
                        skip=str(self.firstevent-1)
                        newval=str(skip)
                    if string.find(val[imin+1:],"number_events_job")>-1:
                        newval=str(self.number_events_job)
                    if imin2 > -1 and val[imin2+4:] in self.outputfiles:
                        newval=self.outputfiles[ val[imin2+4:]]
                    try:
                        assert newval
                    except AssertionError:
                        logger.error("Error while parsing arguments: %s %d %d" % (val, imin, imin2))
                        raise
                    newarg="%s=%s" % (key,newval)
                else:
                    newarg=arg
                NewArgstring=NewArgstring+newarg+" "
            args.append(NewArgstring)
               
        if app.extraIncArgs:
            # incremental arguments: need to add the subjob number.
            arglist=string.split(app.extraIncArgs)
            NewArgstring=""
            for arg in arglist:
                key,val=string.split(arg,"=")
                ival=partition
                if not val.isdigit():
                    logger.warning("Non digit value entered for extraIncArgs: %s. Using %i as default value" % (str(val),ival))
                else:
                    ival+=string.atoi(val)
                newarg="%s=%i" %(key,ival)
                NewArgstring=NewArgstring+newarg+" "
            args.append(NewArgstring)

        try:
            assert len(args)>0
        except AssertionError:
            logger.error("Transformation with no arguments. Please check your inputs!")
            raise


        outfilelist=""

        for type in self.outputfiles.keys():
            if self.outputpaths[type][-1]!="/":
                self.outputpaths[type]=self.outputpaths[type]+"/"
            outfilelist+=self.outputpaths[type]+self.outputfiles[type]+" "
                

##        cacheVer = ''
##        if app.atlas_project and app.atlas_production:
##            cacheVer = "-" + app.atlas_project + "_" + app.atlas_production
        
        jspec = JobSpec()
        jspec.jobDefinitionID   = job._getRoot().id
        jspec.jobName           = commands.getoutput('uuidgen')  
        jspec.AtlasRelease      = 'Atlas-%s' % self.atlas_rel
        
        if app.transform_archive:
            jspec.homepackage       = 'AnalysisTransforms'+app.transform_archive
        elif self.prod_release:
            jspec.homepackage       = 'AnalysisTransforms-AtlasProduction_'+str(self.prod_release)
        jspec.transformation    = '%s/runAthena-00-00-11' % Client.baseURLSUB
            
##        if app.transform_archive:
##            jspec.homepackage       = app.transform_archive
##        elif self.prod_release:
##            jspec.homepackage       = 'AtlasProduction/'+str(self.prod_release)
##        jspec.transformation    = app.transform_script
        
##        jspec.homepackage       = 'AnalysisTransforms'+cacheVer#+nightVer



        #---->????  prodDBlock and destinationDBlock when facing several input / output datasets?

        jspec.prodDBlock    = 'NULL'
        if job.inputdata and len(inputfiles)>0 and inputfiles[0] in self.dsetmap:
            jspec.prodDBlock    = self.dsetmap[inputfiles[0]]

        # How to specify jspec.destinationDBlock  when more than one type of output is available? Panda prod jobs seem to specify only the last output dataset
        outdset=""
        for type in ["EVNT","RDO","AOD"]:
            if type in self.outputfiles.keys():
                outdset=string.replace(self.outputpaths[type],"/",".")
                outdset=outdset[1:-1]
                break
        
        jspec.destinationDBlock = outdset
        jspec.destinationSE = self.outsite
        jspec.prodSourceLabel   = 'user'
        jspec.assignedPriority  = 1000
        jspec.cloud             = cloud
        # memory
        if job.backend.memory != -1:
            jspec.minRamCount = job.backend.memory
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
        for lfn in inputfiles:
            useguid=self.turls[lfn].replace("guid:","")
            finp = FileSpec()
            finp.lfn            = lfn
            finp.GUID           = useguid
            finp.dataset        = self.dsetmap[lfn]
            finp.prodDBlock     = self.dsetmap[lfn]
            finp.dispatchDBlock = self.dsetmap[lfn]
            finp.type           = 'input'
            finp.status         = 'ready'
            jspec.addFile(finp)
        # add dbfiles if any:
        for lfn in dbfiles:
            useguid=self.dbturls[lfn].replace("guid:","")
            finp = FileSpec()
            finp.lfn            = lfn
            finp.GUID           = useguid
            finp.dataset        = self.dsetmap[lfn]
            finp.prodDBlock     = self.dsetmap[lfn]
            finp.dispatchDBlock = self.dsetmap[lfn]
            finp.type           = 'input'
            finp.status         = 'ready'
            jspec.addFile(finp)
        

#       output files( this includes the logfiles)
        for outtype in self.outputfiles.keys():
            fout = FileSpec()
            dset=string.replace(self.outputpaths[outtype],"/",".")
            dset=dset[1:-1]
            fout.dataset=dset
            fout.lfn=self.outputfiles[outtype]
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
        if job.backend.corCheck:
            param += '--corCheck '
        # disable to skip missing files
        if job.backend.notSkipMissing:
            param += '--notSkipMissing '
##        # db release files
##        if len(dbfiles)==1:
##            param += ' --dbrFile "%s"' % dbfiles[0]
        # transform parameters
        arglist=string.join(args," ")
        arglist2=string.join(args[1:]," ")
        #print "Arglist:",arglist2
        param += ' -r ./ '
        param += ' -j "%s"' % urllib.quote(arglist)

        allinfiles=inputfiles+dbfiles
        # Input files.
        param += ' -i "%s" ' % allinfiles
        if len(mbfiles)>0:
            param+= ' -m "%s" ' % mbfiles
        if len(cavernfiles)>0:
            param+= ' -n "%s" ' % cavernfiles
        #        param += '-m "[]" ' #%minList FIXME
        #        param += '-n "[]" ' #%cavList FIXME

        
        # Output files
        
        pandaOutfiles=self.outputfiles
        del pandaOutfiles["LOG"] # logfiles are handled separately in Panda.
##        pandaOutfiles={}
##        for filetype in self.outputfiles.keys():
##            if filetype=="LOG":
##                continue
##            newkey=filetype+".pool.root"
##            pandaOutfiles[newkey]=self.outputfiles[filetype]
        
        param += ' -o "{\'IROOT\':%s }"' % str(pandaOutfiles.items())

        # source URL        
        matchURL = re.search("(http.*://[^/]+)/",Client.baseURLSSL)
        if matchURL != None:
            param += " --sourceURL %s " % matchURL.group(1)
        param += " --trf"


        jspec.jobParameters = param
        jspec.metadata="--trf \"%s\"" % arglist
        #jspec.jobParameters = arglist2
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
