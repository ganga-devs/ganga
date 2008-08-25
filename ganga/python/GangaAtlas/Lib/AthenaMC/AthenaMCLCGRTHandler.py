
###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaMCLCGRTHandler.py,v 1.10.2.3 2006/11/22 14:20:53 elmsheus Exp 
###############################################################################
# AthenaMC LCG Runtime Handler
#

import os, string, commands, re, pwd

import imp,xml.dom.minidom,urllib,random

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.Core import FileWorkspace

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

from Ganga.Lib.LCG import LCGRequirements, LCGJobConfig
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset import DQ2OutputDataset

from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from Ganga.GPIDev.Credentials import GridProxy

# the config file may have a section
# aboout monitoring

mc = getConfig('MonitoringServices')

# None by default

mc.addOption('AthenaMC', None, 'FIXME')
mc.addOption('AthenaMC/LCG', None, 'FIXME')


class AthenaMCLCGRTHandler(IRuntimeHandler):
    """Athena MC LCG Runtime Handler"""

    # Counter for number of subjobs
    ijob = 0
    turls,cavern_turls,minbias_turls={},{},{}
    lfcs,cavern_lfcs,minbias_lfcs={},{},{}
    sites=[]
    maxinfiles=0
    outputlocation,lfchosts,lfcstrings={},{},{}
    outsite,outlocation,outlfc="","",""
    outputpaths,fileprefixes={},{}
    username=""
    joboffset=0
    number_input_jobs=1
    nsubjob_infile=1
    inputdataset=""
    outputdirectory=""
    evgen_job_option=""
    prod_release=""
    atlas_rel=""
    
    def master_prepare(self,app,appmasterconfig):
        
        # Extract username from certificate
        proxy = GridProxy()
        self.username = proxy.identity()

        if app.siteroot:
            os.environ["SITEROOT"]=app.siteroot
        os.environ["CMTSITE"]=app.cmtsite
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
        if job.backend._name in ["Local","PBS"]:
            try:
                assert "SITEROOT" in os.environ
            except:
                logger.error("Error, ATLAS environment not defined")
                raise
            try:
                assert "CMTSITE" in os.environ
            except:
                logger.error("cmt not setup properly. Please check your ATLAS setup or run on the grid")
                raise
            if os.environ["CMTSITE"]=="CERN" and "AtlasVersion" in os.environ:
                logger.debug("Checking AtlasVersion: %s and selected atlas release %s" % (os.environ["AtlasVersion"],self.atlas_rel))
                try:
                    assert self.atlas_rel==os.environ["AtlasVersion"]
                except:
                    logger.error("Mismatching atlas release. Local setup is %s, resetting requested atlas release to local value." % os.environ["AtlasVersion"])
                    app.atlas_release=os.environ["AtlasVersion"]
                    self.atlas_rel==os.environ["AtlasVersion"]
            elif "ATLAS_RELEASE" in os.environ:
                logger.debug("Checking ATLAS_RELEASE: %s and selected atlas release %s" % (os.environ["ATLAS_RELEASE"],self.atlas_rel))
                try:
                    assert self.atlas_rel==os.environ["ATLAS_RELASE"]
                except:
                    logger.error("Mismatching atlas release. Local setup is %s, resetting requested atlas release to local value." % os.environ["ATLAS_RELEASE"])
                    self.atlas_rel=os.environ["ATLAS_RELEASE"]
            else:
                logger.warning("Could not compare requested release and local setup. Hope you are doing something sensible...")

                
        if job.backend._name=="LSF":
            try:
                assert "CMTSITE" in os.environ and os.environ["CMTSITE"]=="CERN"
            except:
                logger.error("Error, CERN ATLAS AFS environment not defined. Needed by LSF backend")
                raise
            
        if app.mode !="evgen" and app.mode !="template":
            try:
                assert job.inputdata and job.inputdata._name == 'AthenaMCInputDatasets'
            except :
                logger.error("job.inputdata must be used and set to 'AthenaMCInputDatasets'")
                raise
        if job.inputdata and app.mode =="template":
            try:
                assert job.inputdata._name == 'AthenaMCInputDatasets'
            except :
                logger.error("job.inputdata must be set to 'AthenaMCInputDatasets'")
                raise
        # checking se-name: must not write to MC/DATA/PRODDISK space tokens.

        if app.se_name:
            forbidden_spacetokens=["MCDISK","DATADISK","MCTAPE","DATATAPE","PRODDISK","PRODTAPE"]
            for token in forbidden_spacetokens:
                try:
                    assert token not in app.se_name
                except:
                    logger.error("You are not allowed to write output data in any production space token: %s. Please select a site with ATLASUSERDISK space token or a srmv1 endpoint" % app.se_name)
                    raise

        if job.inputdata and job.inputdata._name == 'AthenaMCInputDatasets':
            inputdata=job.inputdata.get_dataset(app,self.username)
            if len(inputdata)!= 3:
                logger.error("Error, wrong format for inputdata %d, %s" % (len(inputdata),inputdata))
                raise "Input file not found"
            self.turls=inputdata[0]
            self.lfcs=inputdata[1]
            self.sites=inputdata[2]
            inputfiles=self.turls.keys()
            logger.debug("inputfiles: %s " % str(inputfiles))
            logger.debug("turls: %s " % str(self.turls))
            logger.debug("lfcs: %s " % str(self.lfcs))
            logger.debug("sites: %s " % str(self.sites))
            
            if job.inputdata.number_inputfiles>0 and job.inputdata.number_inputfiles<len(inputfiles) :
                self.maxinfiles=job.inputdata.number_inputfiles
            else:
                self.maxinfiles=len(inputfiles)
                
            # job splitting and input file pre allocation must take place HERE!
            # compute nsubjobs_infile, ninfile_subjobs, etc... , compare with numsubjobs and maxinfiles, do the whole splitting business here!
            numsubjobs=0
            njob_infile=1
            if app._getRoot().splitter:
                numsubjobs= app._getRoot().splitter.numsubjobs
                njob_infile=  app._getRoot().splitter.nsubjobs_inputfile
            # now checking maxinfiles
            n_infiles_job=job.inputdata.n_infiles_job
            if numsubjobs>0:
                totalinfile=int((numsubjobs-1)*n_infiles_job/njob_infile)+1
                if n_infiles_job >= njob_infile:
                    totalinfile=int(numsubjobs*n_infiles_job/njob_infile)
                    
                if totalinfile>self.maxinfiles:
                    logger.debug("Not enough input files, need %s got %s" % (totalinfile, self.maxinfiles))
                    try:
                        assert self.maxinfiles>=totalinfile-n_infiles_job
                    except AssertionError:
                        numsubjobs=int((self.maxinfiles-1)*njob_infile/n_infiles_job)+1
                        app._getRoot().splitter.numsubjobs=numsubjobs
                        logger.error("Too many subjobs. Must abort to prevent crashes. Please resubmit with splitter.numsubjobs=%d." % numsubjobs)
                        raise 

                else:
                    logger.debug("Too many input files, selecting only the %d first ones" % totalinfile)
                    self.maxinfiles=totalinfile
                    
            # handling cavern and minbias data now.
            if job.inputdata.cavern:
                inputdata=job.inputdata.get_cavern_dataset(app)
                if len(inputdata)!= 3:
                    logger.error("Error, wrong format for inputdata %d, %s" % (len(inputdata),inputdata))
                    raise "Input file not found"
                self.cavern_turls=inputdata[0]
                self.cavern_lfcs=inputdata[1]
            if job.inputdata.minbias:
                inputdata=job.inputdata.get_minbias_dataset(app)
                if len(inputdata)!= 3:
                    logger.error("Error, wrong format for inputdata %d, %s" % (len(inputdata),inputdata))
                    raise "Input file not found"
                self.minbias_turls=inputdata[0]
                self.minbias_lfcs=inputdata[1]
            
        # doing output data now
        self.fileprefixes,self.outputpaths=job.outputdata.prep_data(app,self.username)
        expected_datasets=""
        for filetype in self.outputpaths.keys():
            dataset=string.replace(self.outputpaths[filetype],"/",".")
            if dataset[0]==".": dataset=dataset[1:]
            if dataset[-1]==".": dataset=dataset[:-1]
            expected_datasets+=dataset+","
        if not job.outputdata.output_dataset:
            job.outputdata.output_dataset=expected_datasets[:-1] # removing final coma.

        # All the following must move to master_prepare, as well as the translation of outlfc -> potential submission SEs...(dedicated method of outputdata?)
        outsite,backup,outputlocation,backuplocation="","","",""

        logger.debug("self.sites: %s %d" % (str(self.sites),len(self.sites)))

        if  app.se_name == "none" and len(self.sites)>0:
            [outlfc,outsite,outputlocation]=job.outputdata.getDQ2Locations(self.sites[0])
            if len(self.sites)>1:
                [outlfc,backup,backuplocation]=job.outputdata.getDQ2Locations(self.sites[1])
                
            
        outloc="CERNCAF"
        if app.se_name != "none":
            outloc=app.se_name
        if outsite=="" :
            [outlfc,outsite,outputlocation]=job.outputdata.getDQ2Locations(outloc)

       
            
        environment={'T_LCG_GFAL_INFOSYS' :'atlas-bdii.cern.ch:2170'}
        environment["OUTLFC"]=outlfc
        environment["OUTSITE"]=outsite
        environment["OUTPUT_LOCATION"]=outputlocation
        environment["OUTSITE2"]=backup
        environment["OUTPUT_LOCATION2"]=backuplocation

        environment["PROD_RELEASE"]=self.prod_release


        # setting environment["BACKEND"]
        # Local, Condor become "batch". LSF becomes "batch" unless the inputdata is on castor (in this case, it becomes "castor")
        environment["BACKEND"]=job.backend._name
        if job.backend._name=="LSF" and len(self.turls.values())>0:
            turl=self.turls.values()[0]
            if string.find(turl,"castor")>-1:
                environment["BACKEND"]="castor"
            else:
                environment["BACKEND"]="batch"
        if job.backend._name in ["Local","Condor","PBS"]:
            environment["BACKEND"]="batch"
            environment["SITEROOT"]=os.environ["SITEROOT"]
            environment["CMTSITE"]=os.environ["CMTSITE"]

#       finalise environment

        environment["VERBOSITY"]= "%s" % app.verbosity
        # preparing input sandbox, output sandbox , environment vars and job requirements
        
        inputbox = [ 
            File(os.path.join(os.path.dirname(__file__),'setup-release.sh')),
            File(os.path.join(os.path.dirname(__file__),'stage-in.sh')),
            File(os.path.join(os.path.dirname(__file__),'stage-out.sh'))
        ]

        if os.path.exists(app.transform_archive):
            # must add a condition on size.
            inputbox += [ File(app.transform_archive) ]
        else:
            # tarball in local or remote web area.
            if string.find(app.transform_archive,"http")>=0:
                environment['TRANSFORM_ARCHIVE'] = "%s" % (app.transform_archive)
            else:
                myfile=os.path.basename(app.transform_archive)
                myfile="http://cern.ch/atlas-computing/links/kitsDirectory/Production/kits/"+myfile
                environment['TRANSFORM_ARCHIVE'] = "%s" % (myfile)
                
        
        if app.evgen_job_option and os.path.exists(app.evgen_job_option):
            # locally modified job option file to add to the input sand box
            inputbox += [ File(app.evgen_job_option) ]
            # need to strip the path away.
            self.evgen_job_option = app.evgen_job_option.split("/")[-1]
            environment['CUSTOM_JOB_OPTION'] = "%s" % (self.evgen_job_option)
        elif app.evgen_job_option:
            self.evgen_job_option = app.evgen_job_option
            
        if (job.inputsandbox):
            for file in job.inputsandbox:
                inputbox += [ file ]


        outputbox = [ ]
        outputGUIDs='output_guids'
        outputLOCATION='output_location'
        outputbox.append( outputGUIDs )
        outputbox.append( outputLOCATION )
        outputbox.append( 'output_data' )
        if (job.outputsandbox):
            for file in job.outputsandbox:
                outputbox += [ file ]

        

        
        # switch JobTransforms/AtlasProduction package.
        self.isJT=string.find(app.transform_archive,"JobTransform")
        if self.isJT>-1 and app.mode=="evgen":
            environment['T_CONTEXT'] = app.number_events_job # needed to avoid prodsys failure mechanism based on a hardcoded minimum number of event of 5000 per job

            
#       prepare job requirements

        requirements = LCGRequirements()
        requirements.other.append('other.GlueCEStateStatus=="Production"') # missing production
        imax=string.rfind(self.atlas_rel,".")
        rel=string.atof(self.atlas_rel[:imax]) # to deal with string comparisons: [2-9].0.0 > 11.0.0. 
        if self.atlas_rel <= "11.4.0" or rel <=11.4:
            requirements.software=['VO-atlas-release-%s' % self.atlas_rel ]
        elif self.atlas_rel < "12.0.3":
            requirements.software=['VO-atlas-offline-%s' % self.atlas_rel ]
        elif self.atlas_rel >= "14.0.0":
            requirements.software=['VO-atlas-offline-%s-i686-slc4-gcc34-opt' % self.atlas_rel ]
        else:
            requirements.software=['VO-atlas-production-%s' % self.atlas_rel ]
        # case of prod_release set
        if self.prod_release:
            # no prod release tag before 13.0.X
            if self.atlas_rel < "14.0.0" and self.atlas_rel > "13.0.0":
                requirements.software=['VO-atlas-production-%s' % self.prod_release]
            elif self.atlas_rel>= "14.0.0" :
                requirements.software=['VO-atlas-production-%s-i686-slc4-gcc34-opt' % self.prod_release]
        
        # job to data: if inputdata (len(self.sites)>0) then build the string using outlfc to retrieve the cloud and the SEs matching the cloud (dedicated method of outputdata)
        # if no inputdata, use outlfc anyway (or app.se_name if set to anything which is not a DQ2 site) to force the job submission to the matching site.
        # Exception: app.se_name not set and no inputdata -> no constraint.
        targetCloud=""
        if app.se_name!="none"  or job.inputdata:
            targetCloud=job.outputdata.makeLCGmatch(outlfc,app.se_name)
            if targetCloud:
                requirements.other.append(targetCloud)
            logger.debug("targetCloud result:%s" % targetCloud)
            ##if string.find(targetCloud,"VO-atlas-cloud")>-1 and not string.find(targetCloud,"VO-atlas-cloud-T0")>-1:
            ##    requirements.other.append('(Member("VO-atlas-tier-T2",other.GlueHostApplicationSoftwareRunTimeEnvironment) || Member("VO-atlas-tier-T3",other.GlueHostApplicationSoftwareRunTimeEnvironment))')
            #### Applying computing model: Tier 0 and Tier 1s should be reserved to official production. Users jobs must use Tier2s or Tier 3s. Not for Ganga 4, perhaps Ganga 5???
            
        logger.debug("master job submit?")
        if job.backend._name=="LCG" or job.backend._name=="Cronus" or job.backend._name=="Condor" or job.backend._name=="NG":
            return LCGJobConfig("",inputbox,[],outputbox,environment,[],requirements)
        else :
        
            return StandardJobConfig("",inputbox,[],outputbox,environment)

    ##### methods for prepare() (individual jobs and subjobs) #####

    def getEvgenArgs(self,app):
        """prepare args vector for evgen mode"""
        args=[]
        if not app.transform_script:
            app.transform_script="csc_evgen_trf.py"
            if self.isJT>-1:
                app.transform_script="csc.evgen.trf"
            
        if self.isJT>-1:
            args = [ self.atlas_rel,
                     app.se_name,
                     self.outputfiles["logfile"],
                     app.transform_script,
                     app.run_number,
                     self.outputfiles["rootfile"],
                     self.outputfiles["ntuplefile"],
                     self.firstevent,
                     app.number_events_job,
                     self.randomseed,
                     self.evgen_job_option,
                     self.outputfiles["histfile"]
                     ]
        else: # AtlasProduction archive in use.
            args =  [ self.atlas_rel,
                      app.se_name,
                      self.outputfiles["logfile"],
                      app.transform_script,
                      "runNumber=%s" % str(app.run_number),
                      "firstEvent=%s" % str(self.firstevent),
                      "maxEvents=%s" % str(app.number_events_job),
                      "randomSeed=%s" % str(self.randomseed),
                      "jobConfig=%s" % self.evgen_job_option,
                      "outputEvgenFile=%s" % self.outputfiles["rootfile"]
                      ]

            if "histfile" in self.outputfiles:
                args.append("histogramFile=%s" % self.outputfiles["histfile"]) # validation histos on request only for csc_evgen_trf.py

            if "ntuplefile" in self.outputfiles:
                args.append("ntupleFile=%s" % self.outputfiles["ntuplefile"])
            if self.inputfile:
                args.append("inputGeneratorFile=%s" % self.inputfile)


        return args
    
    def getSimulArgs(self,app):
        """prepare args vector for simul-digit mode"""
        args=[]
        skip=str(string.atoi(self.firstevent)-1)
        
        if not app.transform_script:
            app.transform_script="csc_simul_trf.py"
            if self.isJT>-1:
                app.transform_script="csc.simul.trf"
                

        if self.isJT>-1:    
            args = [ self.atlas_rel,
                     app.se_name,
                     self.outputfiles["logfile"],
                     app.transform_script,
                     self.inputfile,  # set up earlier on in master_prepare
                     self.outputfiles["rootfile"],
                     self.outputfiles["rdofile"],
                     app.number_events_job,
                     skip,
                     self.randomseed
                 ]
        else:
            args = [ self.atlas_rel,
                     app.se_name,
                     self.outputfiles["logfile"],
                     app.transform_script,
                     "inputEvgenFile=%s" %self.inputfile,# already quoted by construction
                     "outputHitsFile=%s" % self.outputfiles["rootfile"],
                     "outputRDOFile=%s" % self.outputfiles["rdofile"],
                     "maxEvents=%s" % str(app.number_events_job),
                     "skipEvents=%s" % str(skip),
                     "randomSeed=%s" % str(self.randomseed),
                     "geometryVersion=%s" % app.geometryTag
                     ]
                
        
        return args

        
    def getReconArgs(self,app):
        """prepare args vector for recon mode"""
        args=[]
        skip=str(string.atoi(self.firstevent)-1)
            
        if not app.transform_script:
            app.transform_script="csc_reco_trf.py"
            if self.isJT>-1:
                app.transform_script="csc.reco.trf"

        if self.isJT>-1:    
            args = [ self.atlas_rel,
                     app.se_name,
                     self.outputfiles["logfile"],
                     app.transform_script,
                     self.inputfile,
                     self.outputfiles["esdfile"],
                     self.outputfiles["aodfile"],
                     self.outputfiles["ntuplefile"],
                     app.number_events_job,
                     skip
                     ]
        else:
            args = [ self.atlas_rel,
                     app.se_name,
                     self.outputfiles["logfile"],
                     app.transform_script,
                     "inputRDOFile=%s" % self.inputfile,
                     "maxEvents=%s" % str(app.number_events_job),
                     "skipEvents=%s" % str(skip),
                     "geometryVersion=%s" % app.geometryTag
                     ]
            if app.transform_script=="csc_reco_trf.py" or app.transform_script=="csc_recoESD.trf":
                args.append("outputESDFile=%s" % self.outputfiles["esdfile"])
            if app.transform_script=="csc_reco_trf.py" or app.transform_script=="csc_recoAOD.trf":
                args.append("outputAODFile=%s" % self.outputfiles["aodfile"])

            if self.atlas_rel >="12.0.5" :
                if "ntuplefile" in self.outputfiles :
                    args.append("ntupleFile=%s" %  self.outputfiles["ntuplefile"])
                else:
                    args.append("ntupleFile=NONE")
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

        try:
            assert "logfile" in self.outputfiles
        except AssertionError:
            logger.error("template mode requires a logfile, set by job.application.outputdata.logfile")
            raise
        args =  [ self.atlas_rel,
                  app.se_name,
                  self.outputfiles["logfile"],
                  app.transform_script]
        return args
        
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        """Prepare the job"""

        inputbox=[ ]
        #       prepare environment
        environment={}
        environment=jobmasterconfig.env.copy()
       
        jobnum = self.ijob+1  #current subjob number
        
        job = app._getParent() # Returns job or subjob object
        if job.outputdata.output_firstfile>0:
            jobnum += int(job.outputdata.output_firstfile)-1

        logger.debug("job number is %s" % app.partition_number)
        # should change to :
        if app.partition_number:
            jobnum=string.atoi(app.partition_number)

        logger.debug("job number is now %i" % jobnum)

        self.randomseed = app.random_seed


        self.firstevent = "%i" % (int(app.number_events_job)*(jobnum-1)+1) # automatic computation: takes into account output job offset to compute firstevent. E.g: let assume that the first job processes file number 55, each input file has 100 events so the actual first event is 5401.
        if app.firstevent:
            self.firstevent= "%i" % (int(app.firstevent)+int(app.number_events_job)*self.ijob )# user based numeration. Overrides job offset, but not subjob offset. First event of first job will be app.firstevent, first event of second subjob will be app.firstevent+app.number_events_job, and so on...
            
        # Divide the input files over the subjobs and adjust first events
        njob_infile=1
        if app._getRoot().splitter and app._getRoot().splitter.nsubjobs_inputfile:
            njob_infile=app._getRoot().splitter.nsubjobs_inputfile
            
        if app.mode!="evgen":
            
            # for simul and others, firstevent is the actual offset (skip+1) and should not be bigger than the total number of events in the input file.
            self.firstevent = "%i" % (int(app.firstevent)+ int(app.number_events_job)*(self.ijob%njob_infile))
        logger.debug("job number is %i, first event is %i" % (int(self.ijob),int(self.firstevent)))

        # If using subjobs, take subjob random seed, modify first event
        k=0
        for i in app._getRoot().subjobs:
            if self.ijob==k:
                self.randomseed = i.application.random_seed
            k=k+1   


##        filenrs = ["_"+string.zfill(i,5) for i in inputjobnums]
##        #print "Filenumbers: ", filenrs
##        #print "InLFNs: ", self.inlfns
##        have_input = False
##        if len(filenrs) > 0:
##            have_input = True

##        for filenr in filenrs:
##            if not (filenr in self.turls and filenr in self.lfcs and filenr in self.inlfns):
##                logger.warning("File number %s not found! File stagein disabled!" %filenr)
##                have_input = False
##                #raise Exception()
        inputfiles=self.turls.keys()
        inputfiles.sort()
        outsite=""
        environment["INPUTTURLS"]=""
        environment["INPUTLFCS"]=""
        environment["INPUTFILES"]=""
        self.inputfile,self.cavernfile,self.minbiasfile="","",""
        if len(inputfiles)>0:
            # need to allocate N input turls, lfcs, files to environment vars.
            # getting N
            ninfiles_subjob=job.inputdata.n_infiles_job
            
            imin=int(self.ijob*ninfiles_subjob/njob_infile)
            imax=imin+int((ninfiles_subjob-1)/njob_infile)+1
            if imin>=len(inputfiles):
                logger.error("subjob has no input data. Aborting")
                return 
            if imax>=self.maxinfiles:
                logger.debug("Reached maximum number of inputfiles provided in job.inputdata.number_inputfiles. This is the last subjob")
                imax=self.maxinfiles
            
            self.inputfile=",".join([inputfiles[i] for i in range(imin,imax)])
            if app._getRoot().splitter:
                logger.debug("Splitting data: %d %d %d %d %d " % (self.maxinfiles , app._getRoot().splitter.numsubjobs,ninfiles_subjob,imin,imax))
            j=0 # have to reset j to 0 for each subjob (see wrapper.sh for the reason why)
            for i in range(imin,imax):
                if i > len(inputfiles):
                    logger.error("Requesting more inputfiles than available")
                    break
                turl=""
                if inputfiles[i] in self.turls:
                    turl=self.turls[inputfiles[i]]
                environment["INPUTTURLS"]+="turl[%d]='%s';" % (j,turl.strip())
                lfc=""
                for lfcentry in self.lfcs.values():
                    lfc+=lfcentry+" "
                environment["INPUTLFCS"]+="lfc[%d]='%s';" % (j,lfc.strip())
                environment["INPUTFILES"]+="lfn[%d]='%s';" %(j,inputfiles[i].strip())
                j=j+1
                if len(job.inputdata.inputfiles)==0:
                    job.inputdata.inputfiles.append(inputfiles[i].strip())
            logger.debug("%s %s %s" % (str(environment["INPUTTURLS"]),str(environment["INPUTLFCS"]),str(environment["INPUTFILES"])))

        # now handling cavern/minbias input datasets:
        inputfiles=self.cavern_turls.keys()
        if len(inputfiles)>0:
            try:
                assert len(inputfiles)>= job.inputdata.n_cavern_files_job
            except:
                logger.error("Not enough input files to sustend a single job (expected %d got %d). Aborting" %(job.inputdata.n_cavern_files_job,len(inputfiles)))
                raise

            random.shuffle(inputfiles) # shuffle cavern files to randomize noise distributions between subjobs
            imax=job.inputdata.n_cavern_files_job 
            self.cavernfile=",".join([inputfiles[i] for i in range(imax)])
            for i in range(imax):
                turl=""
                if inputfiles[i] in self.cavern_turls:
                    turl=self.cavern_turls[inputfiles[i]]
                environment["INPUTTURLS"]+="turl[%d]='%s';" % (j,turl.strip())
                lfc=""
                for lfcentry in self.cavern_lfcs.values():
                    lfc+=lfcentry+" "
                environment["INPUTLFCS"]+="lfc[%d]='%s';" % (j,lfc.strip())
                environment["INPUTFILES"]+="lfn[%d]='%s';" %(j,inputfiles[i].strip())
                j=j+1
            logger.debug("%s %s %s" % (str(environment["INPUTTURLS"]),str(environment["INPUTLFCS"]),str(environment["INPUTFILES"])))

        inputfiles=self.minbias_turls.keys()
        if len(inputfiles)>0:
            try:
                assert len(inputfiles)>= job.inputdata.n_minbias_files_job
            except:
                logger.error("Not enough minbias input files to sustend a single job (expected %d got %d). Aborting" %(job.inputdata.n_minbias_files_job,len(inputfiles)))
                raise

            random.shuffle(inputfiles) # shuffle cavern files to randomize noise distributions between subjobs
            imax=job.inputdata.n_minbias_files_job 
            self.minbiasfile=",".join([inputfiles[i] for i in range(imax)])
            for i in range(imax):
                turl=""
                if inputfiles[i] in self.minbias_turls:
                    turl=self.minbias_turls[inputfiles[i]]
                environment["INPUTTURLS"]+="turl[%d]='%s';" % (j,turl.strip())
                lfc=""
                for lfcentry in self.minbias_lfcs.values():
                    lfc+=lfcentry+" "
                environment["INPUTLFCS"]+="lfc[%d]='%s';" % (j,lfc.strip())
                environment["INPUTFILES"]+="lfn[%d]='%s';" %(j,inputfiles[i].strip())
                j=j+1
            logger.debug("%s %s %s" % (str(environment["INPUTTURLS"]),str(environment["INPUTLFCS"]),str(environment["INPUTFILES"])))
        if environment["INPUTTURLS"] :
            # Work around for glite WMS spaced environement variable problem
            inputbox += [ FileBuffer('inputturls.conf',environment['INPUTTURLS']+'\n') ]
        if environment["INPUTLFCS"] :
            # Work around for glite WMS spaced environement variable problem
            inputbox += [ FileBuffer('inputlfcs.conf',environment['INPUTLFCS']+'\n') ]
        if environment["INPUTFILES"] :
            # Work around for glite WMS spaced environement variable problem
            inputbox += [ FileBuffer('inputfiles.conf',environment['INPUTFILES']+'\n') ]



# now doing output files....
        self.outputfiles={}
        for filetype in self.fileprefixes.keys():
            if filetype=="logfile":
                self.outputfiles["logfile"]=self.fileprefixes["logfile"]+"._%5.5d.job.log" % jobnum
            elif  filetype=="histfile":
                self.outputfiles["histfile"]=self.fileprefixes["histfile"]+"._%5.5d.hist.root" % jobnum
            elif  filetype=="ntuplefile":
                self.outputfiles["ntuplefile"]=self.fileprefixes["ntuplefile"]+"._%5.5d.root" % jobnum   
            else:
                self.outputfiles[filetype]=self.fileprefixes[filetype]+"._%5.5d.pool.root" % jobnum
            # add the final lfn to the expected output list
            logger.debug("adding %s to list of expected output" % self.outputfiles[filetype])
            job.outputdata.expected_output.append(self.outputfiles[filetype])

        expected_datasets=""
        for filetype in self.outputpaths.keys():
            dataset=string.replace(self.outputpaths[filetype],"/",".")
            if dataset[0]==".": dataset=dataset[1:]
            if dataset[-1]==".": dataset=dataset[:-1]
            expected_datasets+=dataset+","
        if not job.outputdata.output_dataset:
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
                        skip=str(string.atoi(self.firstevent)-1)
                        newval=str(skip)
                    if string.find(val[imin+1:],"number_events_job")>-1:
                        newval=str(app.number_events_job)
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
                ival=self.ijob
                if not val.isdigit():
                    logger.warning("Non digit value entered for extraIncArgs: %s. Using %i as default value" % (str(val),self.ijob))

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
        
        # Save variables in subjobs for later reuse
        k=0
        for i in app._getRoot().subjobs:
            if self.ijob==k:
                logger.debug("partition number was %s" % i.application.partition_number)
                if not i.application.partition_number:
                    i.application.partition_number=str(jobnum)
                logger.debug("partition number is now %s" % i.application.partition_number)
                if self.firstevent:
                    i.application.firstevent = self.firstevent
##                if len(inputjobnums) > 0:
##                    i.application.input_firstfile = inputjobnums[0]
##                if app.number_inputfiles:
##                    i.application.number_inputfiles=app.number_inputfiles
##                if app.output_firstfile:
##                    i.application.output_firstfile= app.output_firstfile
                if app.transform_script:
                    i.application.transform_script =  app.transform_script
## must change to job.outputdata and job.inputdata members
##                if app.datasets.outdirectory:
##                    i.application.datasets.outdirectory = app.datasets.outdirectory
##                if app.datasets.logfile:
##                    i.application.datasets.logfile = app.datasets.logfile
##                if app.datasets.outrootfile:
##                    i.application.datasets.outrootfile = app.datasets.outrootfile
##                if app.datasets.outntuplefile:
##                    i.application.datasets.outntuplefile = app.datasets.outntuplefile
##                if app.datasets.outhistfile:
##                    i.application.datasets.outhistfile = app.datasets.outhistfile
##                if app.datasets.indirectory:
##                    i.application.datasets.indirectory = app.datasets.indirectory
##                if app.datasets.inputfile:
##                    i.application.datasets.inputfile = app.datasets.inputfile
##                if app.datasets.outrdofile:
##                    i.application.datasets.outrdofile = app.datasets.outrdofile
##                if app.datasets.outesdfile:
##                    i.application.datasets.outesdfile = app.datasets.outesdfile
##                if app.datasets.outaodfile:
##                    i.application.datasets.outaodfile = app.datasets.outaodfile
##                if app.datasets.outcbntfile:
##                    i.application.datasets.outcbntfile = app.datasets.outcbntfile
            k=k+1   


# now filling up environment variables for output data in jdl file...
        outfilelist=""

        for type in self.outputfiles.keys():
            if self.outputpaths[type][-1]!="/":
                self.outputpaths[type]=self.outputpaths[type]+"/"
            outfilelist+=self.outputpaths[type]+self.outputfiles[type]+" "
                
        environment["OUTPUTFILES"]=outfilelist
        # Work around for glite WMS spaced environement variable problem
        inputbox += [ FileBuffer('outputfiles.conf',environment['OUTPUTFILES']+'\n') ]

        jid=""
        if job._getRoot().subjobs:
            jid = job._getRoot().id
        else:
            jid = "%d" % job.id
        
        environment["OUTPUT_JOBID"]=str(jid) # used for versionning
        
        inputdata = []

        filename="wrapper.sh"
        exe = os.path.join(os.path.dirname(__file__),filename)


#       output sandbox
        outputbox =jobmasterconfig.outputbox

        #Increase subjobs prepare counter
        self.ijob=self.ijob+1

        if job.backend._name=="LCG" or job.backend._name=="Cronus" or job.backend._name=="Condor" or job.backend._name=="NG":
            logger.debug("submission to %s" % job.backend._name)
            #       prepare job requirements
            requirements = jobmasterconfig.requirements
            if "INPUTTURLS" in environment:
                logger.debug(environment["INPUTTURLS"])
                if string.find(environment["INPUTTURLS"],"file:")>=0:
                    logger.error("Input file was found to be local, and LCG backend does not support replication of local files to the GRID yet. Please register your input dataset in DQ2 before resubmitting this job. Aborting")
                    raise "Submission cancelled"
            if string.lower(app.se_name)=="local":
                logger.error("Output file cannot be committed to local filesystem on a grid job. Please change se_name")
                raise "Submission cancelled"

            lcg_job_config = LCGJobConfig(File(exe),inputbox,args,outputbox,environment,inputdata,requirements) 
            lcg_job_config.monitoring_svc = mc['AthenaMC/LCG']
            return lcg_job_config
        else:
            logger.debug("Backend %s not fully supported , will try our best anyway..." % job.backend._name)
            # if there are input data files and they are on the grid, prestage them on local area (use either app.datasets.input_dataset or /tmp/$login/data (and update environment["INPUTFILE"] accordingly inf the later is used...)
            # later development....

            return StandardJobConfig(File(exe),inputbox,args,outputbox,environment) 



allHandlers.add('AthenaMC','LCG',AthenaMCLCGRTHandler)
allHandlers.add('AthenaMC','Local',AthenaMCLCGRTHandler)
allHandlers.add('AthenaMC','LSF',AthenaMCLCGRTHandler)
allHandlers.add('AthenaMC','Condor',AthenaMCLCGRTHandler)
allHandlers.add('AthenaMC','Cronus',AthenaMCLCGRTHandler)
allHandlers.add('AthenaMC','NG',AthenaMCLCGRTHandler)
allHandlers.add('AthenaMC','PBS',AthenaMCLCGRTHandler)

config = getConfig('AthenaMC')
logger = getLogger('AthenaMC')
