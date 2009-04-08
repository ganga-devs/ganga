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

# the config file may have a section
# aboout monitoring

mc = getConfig('MonitoringServices')

# None by default

mc.addOption('AthenaMC', None, 'FIXME')
mc.addOption('AthenaMC/LCG', None, 'FIXME')


class AthenaMCLCGRTHandler(IRuntimeHandler):
    """Athena MC LCG Runtime Handler"""

    turls,cavern_turls,minbias_turls,dbturls={},{},{},{}
    lfcs,cavern_lfcs,minbias_lfcs,dblfcs={},{},{},{}
    sites,cavern_sites,minbias_sites,dbsites=[],[],[],[]
    outputlocation,lfchosts,lfcstrings={},{},{}
    outsite,outlfc,outlfc2="","",""
    outputpaths,fileprefixes={},{}
    evgen_job_option=""
    prod_release=""
    atlas_rel=""
    
    
    def master_prepare(self,app,appmasterconfig):
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
            raise ApplicationConfigurationError(None,"A reference to the Production archive to be used must be set, either through the declaration of the archive itself in application.transform_archive or by putting the 4-digit production cache release number in application.atlas_release. Neither are set. Aborting.")
        job = app._getParent()
        if job.backend._name in ["Local","PBS"]:
            if app.dryrun:
                os.environ["SITEROOT"]  = "NONE"
                os.environ["CMTSITE"]  = "NONE"
            try:
                assert "SITEROOT" in os.environ
            except:
                raise ApplicationConfigurationError(None," ATLAS environment not defined")
                
            try:
                assert "CMTSITE" in os.environ
            except:
                raise ApplicationConfigurationError(None,"cmt not setup properly. Please check your ATLAS setup or run on the grid")
            
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
                raise ApplicationConfigurationError(None,"Error, CERN ATLAS AFS environment not defined. Needed by LSF backend")
                
            
        if app.mode !="evgen" and app.mode !="template":
            try:
                assert job.inputdata and job.inputdata._name == 'AthenaMCInputDatasets'
            except :
                raise ApplicationConfigurationError(None,"job.inputdata must be used and set to 'AthenaMCInputDatasets'")
            
        if job.inputdata:
            try:
                assert job.inputdata._name == 'AthenaMCInputDatasets'
            except :
                raise ApplicationConfigurationError(None,"job.inputdata must be set to 'AthenaMCInputDatasets'")
                
        # checking se-name: must not write to MC/DATA/PRODDISK space tokens.
        if not app.se_name:
            app.se_name='none'
            # important to avoid having null string in app.se_name as it would ruin the argument list of the wrapper script!

        outSE=app.se_name
        if hasattr(job.backend,'requirements') and hasattr(job.backend.requirements,'sites') and len(job.backend.requirements.sites)!=0:
            outSE=str(job.backend.requirements.sites[0])
        
        if outSE:
            forbidden_spacetokens=["MCDISK","DATADISK","MCTAPE","DATATAPE","PRODDISK","PRODTAPE"]
            for token in forbidden_spacetokens:
                try:
                    assert token not in outSE
                except:
                    raise ApplicationConfigurationError(None,"You are not allowed to write output data in any production space token: %s. Please select a site with ATLASUSERDISK or ATLASLOCALGROUPDISK space token" % outSE)                   
        if not app.dryrun and job.inputdata and job.inputdata._name == 'AthenaMCInputDatasets':
            # The input dataset was already read in in AthenaMC.master_configure
            self.turls=app.turls
            self.lfcs=app.lfcs
            self.sites=app.sites
            inputfiles=self.turls.keys()
            logger.debug("inputfiles: %s " % str(inputfiles))
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
            if dataset[0]==".": dataset=dataset[1:]
            if dataset[-1]==".": dataset=dataset[:-1]
            expected_datasets+=dataset+","
            #       if not job.outputdata.output_dataset:
        if not job.outputdata.output_dataset or string.find(job.outputdata.output_dataset,",") > 0 : #update only if output_dataset is not used to force the output dataset names.
            job.outputdata.output_dataset=expected_datasets[:-1] # removing final coma.

        # All the following must move to master_prepare, as well as the translation of outlfc -> potential submission SEs...(dedicated method of outputdata?)
        outsite,backup,outputlocation,backuplocation="","","",""

        logger.debug("self.sites: %s %d" % (str(self.sites),len(self.sites)))

        #        if  app.se_name == "none" and len(self.sites)>0:
        if len(self.sites)>0:
            [outlfc,outsite,outputlocation]=job.outputdata.getDQ2Locations(self.sites[0])
            if len(self.sites)>1:
                [outlfc2,backup,backuplocation]=job.outputdata.getDQ2Locations(self.sites[1])
            
        outloc="CERN-PROD_USERDISK"
        
        if app.se_name != "none":
            # outloc=app.se_name
            outloc=app.se_name
        if hasattr(job.backend,'requirements') and hasattr(job.backend.requirements,'sites') and len(job.backend.requirements.sites)!=0: 
            outloc=str(job.backend.requirements.sites[0])        
        #print outloc
        if outsite=="" :
            [outlfc,outsite,outputlocation]=job.outputdata.getDQ2Locations(outloc)
        try:
            assert outsite
        except:
            raise ApplicationConfigurationError(None,"Could not find suitable location for your output. Please subscribe your input dataset (if any) to a suitable location or change application.se_name to a suitable space token")
        # outlfc is now set. Clearing up all input sites lists accordingly:
        imax=string.find(outsite,"_")
        outsite_short=outsite[:imax] # remove space token for easier match
        if len(self.sites)>0:
            selsite=""
            for site in self.sites:
                if string.find(site,outsite_short)>-1:
                     selsite=site
                     break
            try:
                assert selsite!=""
            except:
                raise ApplicationConfigurationError(None,"Input data not in destination site %s. Please subscribe the input dataset to the destination site or choose another site.Aborting " % outsite)
            #    self.sites=[selsite]
            #    must put selsite as first choice in self.sites
            #print "SELSITE",len(self.sites)
            if len(self.sites)>1:
                if selsite in self.sites:
                    self.sites.remove(selsite)
                self.sites.insert(0,selsite)
            # trimming down self.sites: 
        if len(self.cavern_sites)>0:
            selsite=""
            for site in self.cavern_sites:
                if string.find(site,outsite_short)>-1:
                    selsite=site
                    break
            try:
                assert selsite!=""
            except:
                raise ApplicationConfigurationError(None,"Cavern input data not in destination site %s. Please subscribe the cavern dataset to the destination site or choose another site.Aborting " % outsite)
            self.cavern_sites=[selsite]
        if len(self.minbias_sites)>0:
            selsite=""
            for site in self.minbias_sites:
                if string.find(site,outsite_short)>-1:
                    selsite=site
                    break
            try:
                assert selsite!=""
            except:
                raise ApplicationConfigurationError(None,"Minbias input data not in destination site %s. Please subscribe the minbias dataset to the destination site or choose another site.Aborting " % outsite)
            self.minbias_sites=[selsite]
        if len(self.dbsites)>0:
            selsite=""
            for site in self.dbsites:
                if string.find(site,outsite_short)>-1:
                     selsite=site
                     break
            try:
                assert selsite!=""
            except:
                raise ApplicationConfigurationError(None,"DBRelease dataset not in destination site %s. Please subscribe the cavern dataset to the destination site or choose another site.Aborting " % outsite)
            self.dbsites=[selsite]
             
            

        # srmv2 sites special treatment: the space token has been prefixed to the outputlocation and must be removed now:
        imin=string.find(outputlocation,"token:")
        imax=string.find(outputlocation,"srm:")
        spacetoken=""
        if imin>-1 and imax>-1:
            spacetoken=outputlocation[imin+6:imax-1]
            outputlocation=outputlocation[imax:]
        # same treatment for backup location if any
        imin=string.find(backuplocation,"token:")
        imax=string.find(backuplocation,"srm:")
        bst=""
        if imin>-1 and imax>-1:
            bst=backuplocation[imin+6:imax-1]
            backuplocation=backuplocation[imax:]
       
        environment={'T_LCG_GFAL_INFOSYS' :'atlas-bdii.cern.ch:2170'}
        if app.mode !="template":
            trflags="/Ft"
            if app.verbosity:
                trflags+="/W/Fl/W%s" % app.verbosity
            environment["TRFLAGS"]=trflags

#        if app.mode=="template":

            
        environment["OUTLFC"]=outlfc
        environment["OUTSITE"]=outsite
        environment["OUTPUT_LOCATION"]=outputlocation
        if spacetoken:
            environment["SPACETOKEN"]=spacetoken
        if backup:
            environment["OUTLFC2"]=outlfc2
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


        # preparing input sandbox, output sandbox , environment vars and job requirements
        
        inputbox = [ 
            File(os.path.join(os.path.dirname(__file__),'setup-release.sh')),
            File(os.path.join(os.path.dirname(__file__),'stage-in.sh')),
            File(os.path.join(os.path.dirname(__file__),'stage-out.sh')),
            File(os.path.join(os.path.dirname(__file__),'adler32.py'))
        ]

        if os.path.exists(app.transform_archive):
            # must add a condition on size.
            inputbox += [ File(app.transform_archive) ]
        elif app.transform_archive:
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
            environment['T_CONTEXT'] = str(self.number_events_job) # needed to avoid prodsys failure mechanism based on a hardcoded minimum number of event of 5000 per job

            
        #       prepare job requirements
            
        if hasattr(job.backend,'requirements') and hasattr(job.backend.requirements,'sites') and hasattr(job.backend.requirements,'software') and hasattr(job.backend.requirements,'other') and hasattr(job.backend.requirements,'dq2client_version'):
            requirements=job.backend.requirements
        else:
            requirements = AtlasLCGRequirements()
        
#        requirements.other.append('other.GlueCEStateStatus=="Production"') # missing production
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

        if app.transform_archive and string.find(app.transform_archive,"AtlasTier0")>-1:
            requirements.software=['VO-atlas-tier0-%s' % self.prod_release]
        extraConfig=getConfig('defaults_AtlasLCGRequirements')
        dq2client_version = extraConfig['dq2client_version']

        if job.backend.requirements.dq2client_version:
            dq2client_version = job.backend.requirements.dq2client_version
        try:
            assert dq2client_version!=""
        except:
            raise 
        requirements.software += ['VO-atlas-dq2clients-%s' % dq2client_version]

        # job to data, strict: target outsite and nothing else.
        requirements.sites=outsite

##        # job to data: if inputdata (len(self.sites)>0) then build the string using outlfc to retrieve the cloud and the SEs matching the cloud (dedicated method of outputdata)
##        # if no inputdata, use outlfc anyway (or app.se_name if set to anything which is not a DQ2 site) to force the job submission to the matching site.
##        # Exception: app.se_name not set and no inputdata -> no constraint.
##        targetCloud=""
##        if app.se_name!="none"  or job.inputdata:
##            targetCloud=job.outputdata.makeLCGmatch(outlfc,app.se_name)
##            if targetCloud:
##                requirements.other.append(targetCloud)
##            logger.debug("targetCloud result:%s" % targetCloud)
##            ##if string.find(targetCloud,"VO-atlas-cloud")>-1 and not string.find(targetCloud,"VO-atlas-cloud-T0")>-1:
##            #requirements.other.append('( ! Member("VO-atlas-tier-T0",other.GlueHostApplicationSoftwareRunTimeEnvironment) && ! Member("VO-atlas-tier-T1",other.GlueHostApplicationSoftwareRunTimeEnvironment))')
##            requirements.other.append('( ! Member("VO-atlas-tier-T1",other.GlueHostApplicationSoftwareRunTimeEnvironment))')
##            #### Applying computing model: Tier 0 and Tier 1s should be reserved to official production. Users jobs must use other sites.
##            #### Allowing T0 as CERN T2 is not properly tagged .
            
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
                     self.outputfiles["LOG"],
                     app.transform_script,
                     app.run_number,
                     self.outputfiles["EVNT"],
                     self.outputfiles["NTUP"],
                     str(self.firstevent),
                     str(self.number_events_job),
                     self.randomseed,
                     self.evgen_job_option,
                     ]
            if "HIST" in self.outputfiles:
                args.append(self.outputfiles["HIST"])
        else: # AtlasProduction archive in use.
            args =  [ self.atlas_rel,
                      app.se_name,
                      self.outputfiles["LOG"],
                      app.transform_script,
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
        args=[]
        skip=str(self.firstevent-1)
        
        if not app.transform_script:
            app.transform_script="csc_simul_trf.py"
            if self.isJT>-1:
                app.transform_script="csc.simul.trf"

        if self.isJT>-1:    
            args = [ self.atlas_rel,
                     app.se_name,
                     self.outputfiles["LOG"],
                     app.transform_script,
                     self.inputfile,  # set up earlier on in master_prepare
                     self.outputfiles["HITS"],
                     self.outputfiles["RDO"],
                     str(self.number_events_job),
                     skip,
                     self.randomseed
                 ]
        else:
            args = [ self.atlas_rel,
                     app.se_name,
                     self.outputfiles["LOG"],
                     app.transform_script,
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
        args=[]
        skip=str(self.firstevent-1)
            
        if not app.transform_script:
            app.transform_script="csc_reco_trf.py"
            if self.isJT>-1:
                app.transform_script="csc.reco.trf"

        if self.isJT>-1:    
            args = [ self.atlas_rel,
                     app.se_name,
                     self.outputfiles["LOG"],
                     app.transform_script,
                     self.inputfile,
                     self.outputfiles["ESD"],
                     self.outputfiles["AOD"],
                     self.outputfiles["NTUP"],
                     str(self.number_events_job),
                     skip
                     ]
        else:
            args = [ self.atlas_rel,
                     app.se_name,
                     self.outputfiles["LOG"],
                     app.transform_script,
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
            raise ApplicationConfigurationError(None,"template mode requires the name of the transformation you want to use")
        
        logger.warning("Using the new template mode. Please use application.extraArgs for the transformation parameters")

        try:
            assert "LOG" in self.outputfiles
        except AssertionError:
            raise ApplicationConfigurationError(None,"template mode requires a logfile, set by job.application.outputdata.logfile")

        args =  [ self.atlas_rel,
                 app.se_name,
                 self.outputfiles["LOG"],
                 app.transform_script
                 ]
        if "EVNT" in self.outputfiles and self.outputfiles["EVNT"].upper() != "NONE":
            args.append("outputEvgenFile=%s" % self.outputfiles["EVNT"]) 
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
        """Prepare the job"""

        inputbox=[ ]
        #       prepare environment
        environment={}
        environment=jobmasterconfig.env.copy()
       
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
        
        # Strict matching must be discarded if inputdata.redefine_partitions is not used.

        if (job._getRoot().inputdata and job._getRoot().inputdata.redefine_partitions == ""):
            inputfiles=[]
            inlfns=self.turls.keys()
            inlfns.sort()
            for i in inputnumbers:
                try:
                    assert len(inlfns)>= i
                except:
                    raise ApplicationConfigurationError(None,"Not enough input files, got %i expected %i" % (len(inlfns),i))

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

        outsite=""
        # migration to using dq2-get in stage-in: environment["INPUTTURLS"] is deprecated, as well as environment["INPUTLFCS"]. They are replaced by environment["INPUTDATASETS"] and environment["INPUTSITES"]
       
        environment["INPUTDATASETS"]=""
        environment["INPUTSITES"]=""
        environment["INPUTFILES"]=""
        self.inputfile,self.cavernfile,self.minbiasfile="","",""

        dsetmap,sitemap={},{}
        
        for infile in inputfiles:
            dsetmap[infile]=self.lfcs.keys()[0]
            sitemap[infile]=string.join(self.sites," ") # only for signal input datasets
            #sitemap[infile]=self.sites[0]
        self.inputfile=",".join(inputfiles)
        # adding cavern/minbias/dbrelease to the mapping
        cavernfiles= self.cavern_turls.keys()
        for infile in cavernfiles:
            dsetmap[infile]=self.cavern_lfcs.keys()[0]
            #            sitemap[infile]=string.join(self.cavern_sites," ")
            sitemap[infile]=self.cavern_sites[0]
        mbfiles= self.minbias_turls.keys()
        for infile in mbfiles:
            dsetmap[infile]=self.minbias_lfcs.keys()[0]
            #           sitemap[infile]=string.join(self.minbias_sites," ")
            sitemap[infile]=self.minbias_sites[0]
        dbfiles=self.dbturls.keys()
        for infile in dbfiles:
            dsetmap[infile]=self.dblfcs.keys()[0]
            #            sitemap[infile]=string.join(self.dbsites," ")
            sitemap[infile]=self.dbsites[0]
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
        for infile in alllfns:
            environment["INPUTFILES"]+="lfn[%d]='%s';" %(infilenr,infile)
            environment["INPUTDATASETS"]+="dset[%d]='%s';"%(infilenr,dsetmap[infile])
            environment["INPUTSITES"]+="site[%d]='%s';"%(infilenr,sitemap[infile])
            infilenr += 1


                
        logger.debug("%s %s %s" % (str(environment["INPUTDATASETS"]),str(environment["INPUTSITES"]),str(environment["INPUTFILES"])))
        
        if environment["INPUTDATASETS"] :
            # Work around for glite WMS spaced environement variable problem
            inputbox += [ FileBuffer('inputturls.conf',environment['INPUTDATASETS']+'\n') ]
        if environment["INPUTSITES"] :
            # Work around for glite WMS spaced environement variable problem
            inputbox += [ FileBuffer('inputlfcs.conf',environment['INPUTSITES']+'\n') ]
        if environment["INPUTFILES"] :
            # Work around for glite WMS spaced environement variable problem
            inputbox += [ FileBuffer('inputfiles.conf',environment['INPUTFILES']+'\n') ]



# now doing output files....
        self.outputfiles={}
        outpartition = partition + job._getRoot().outputdata.output_firstfile - 1
        for filetype in self.fileprefixes.keys():
            if filetype=="LOG":
                self.outputfiles["LOG"]=self.fileprefixes["LOG"]+"._%5.5d.job.log" % outpartition 
            elif  filetype=="HIST":
                self.outputfiles["HIST"]=self.fileprefixes["HIST"]+"._%5.5d.hist.root" % outpartition
            elif  filetype=="NTUP":
                self.outputfiles["NTUP"]=self.fileprefixes["NTUP"]+"._%5.5d.root" % outpartition
            else:
                self.outputfiles[filetype]=self.fileprefixes[filetype]+"._%5.5d.pool.root" % outpartition
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
                    environment["ATLASDBREL"]=val
                    NewArgstring=NewArgstring+"DBRelease=%s " % dbfile
                    continue
                imin=string.find(val,"$")
                imin2=string.find(val,"$out")
                newval=""
                if imin>-1:
                    if string.find(val[imin+1:],"J")>-1:
                        nval=val.replace("$J",str(partition))
                        try:
                            newval=eval(nval)
                            assert newval
                        except AssertionError:
                            raise ApplicationConfigurationError(None,"error while parsing arguments: %s %d %d" % (val, imin, imin2))
                    
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
                    #if imin2 > -1:
                     #   print self.outputfiles.keys()
                    if imin2 > -1 and val[imin2+4:] in self.outputfiles:
                        newval=self.outputfiles[ val[imin2+4:]]
                    try:
                        assert newval
                    except AssertionError:
                        raise ApplicationConfigurationError(None,"Error while parsing arguments: %s %d %d" % (val, imin, imin2))
                        
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
            raise ApplicationConfigurationError(None,"Transformation with no arguments. Please check your inputs!")

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
        if app.dryrun:
            environment["DRYRUN"] = "TRUE"
        
        inputdata = []

        filename="wrapper.sh"
        exe = os.path.join(os.path.dirname(__file__),filename)


#       output sandbox
        outputbox =jobmasterconfig.outputbox

        if job.backend._name=="LCG" or job.backend._name=="Cronus" or job.backend._name=="Condor" or job.backend._name=="NG":
            logger.debug("submission to %s" % job.backend._name)
            #       prepare job requirements
            requirements = jobmasterconfig.requirements
            if "INPUTTURLS" in environment:
                logger.debug(environment["INPUTTURLS"])
                if string.find(environment["INPUTTURLS"],"file:")>=0:
                    raise ApplicationConfigurationError(None,"Input file was found to be local, and LCG backend does not support replication of local files to the GRID yet. Please register your input dataset in DQ2 before resubmitting this job. Aborting")
            if string.lower(app.se_name)=="local":
                raise ApplicationConfigurationError(None,"Output file cannot be committed to local filesystem on a grid job. Please change se_name")

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
