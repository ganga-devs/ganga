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

_defaultSite='CERN-PROD_SCRATCHDISK'

class AthenaMCLCGRTHandler(IRuntimeHandler):
    """Athena MC LCG Runtime Handler"""

    def sortSites(self,insites,outsite):
        inlist=insites.split(" ")
        imax=outsite.find("_")
        outsite=outsite[:imax]
        newlist=[]
        for site in inlist:
            if site.find(outsite)>-1:
                newlist.insert(0,site)
            else:
                newlist.append(site)
        return string.join(newlist," ")
            
    def master_prepare(self,app,appmasterconfig):
        if app.siteroot: 
            os.environ["SITEROOT"]=app.siteroot
        os.environ["CMTSITE"]=app.cmtsite

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
                logger.debug("Checking AtlasVersion: %s and selected atlas release %s" % (os.environ["AtlasVersion"],app.atlas_rel))
                try:
                    assert app.atlas_rel==os.environ["AtlasVersion"]
                except:
                    logger.error("Mismatching atlas release. Local setup is %s, resetting requested atlas release to local value." % os.environ["AtlasVersion"])
                    app.atlas_release=os.environ["AtlasVersion"]
                    app.atlas_rel==os.environ["AtlasVersion"]
            elif "ATLAS_RELEASE" in os.environ:
                logger.debug("Checking ATLAS_RELEASE: %s and selected atlas release %s" % (os.environ["ATLAS_RELEASE"],app.atlas_rel))
                try:
                    assert app.atlas_rel==os.environ["ATLAS_RELASE"]
                except:
                    logger.error("Mismatching atlas release. Local setup is %s, resetting requested atlas release to local value." % os.environ["ATLAS_RELEASE"])
                    app.atlas_rel=os.environ["ATLAS_RELEASE"]
            else:
                logger.warning("Could not compare requested release and local setup. Hope you are doing something sensible...")

                
        if job.backend._name=="LSF":
            try:
                assert "CMTSITE" in os.environ and os.environ["CMTSITE"]=="CERN"
            except:
                raise ApplicationConfigurationError(None,"Error, CERN ATLAS AFS environment not defined. Needed by LSF backend")

       
        environment={'T_LCG_GFAL_INFOSYS' :'atlas-bdii.cern.ch:2170'}

        trfopts=app.transflags
        # need to parse them to be able to pass them in an environment variable
        trfopts=trfopts.replace(" ","/W")
        trfopts=trfopts.replace("-","/F")
        
        trflags=trfopts
        if app.mode =="evgen":
            trflags="/Ft"
            if app.verbosity:
                trflags+="/W/Fl/W%s" % app.verbosity
        
        if trflags:
            environment["TRFLAGS"]=trflags

        # setting output site from input data if any.
        outsite,backup,outputlocation,backuplocation="","","",""
        logger.info("checking sites from input data: %s" % str(app.sites))

        # must distinguish running site (backend.requirements.sites) and output storage site (app.se_name)
        
        # matching with user's wishes (app.se_name or backend.requirements.sites)

        usersites=[]
        if len(job.backend.requirements.sites)>0:
            usersites=job.backend.requirements.sites
##        elif job.application.se_name and job.application.se_name != "none":
##            usersites=job.application.se_name.split(" ")
        logger.info("user selection: %s" % str(usersites))
            
        # select sites which are matching user's wishes, if any.
        selectedSites=app.sites
        if len(selectedSites)==0:
            selectedSites=usersites
        if len(usersites)>0 and len(app.sites)>0:
            selectedSites=job.inputdata.trimSites(usersites,app.sites)
        # evgen case (no input data-> app.sites=[])
        if len(app.sites)==0 and app.se_name and app.se_name != "none":
            selectedSites=app.se_name.split(" ")

        # This comes last: using surviving sites from matching process.
        if len(selectedSites)==0:
            try:
                assert len(usersites)==0
            except:
                raise ApplicationConfigurationError(None,"Could not find a match between input dataset locations: %s and your requested sites: %s. Please use a space token compatible with one of the input dataset locations (replace _XXXDISK or _XXXTAPE by _LOCALGROUPDISK or _SCRATCHDISK if necessary)" % (str(app.sites),str(usersites)))
            logger.warning("Failed to obtain processing site from input data, will use default value: CERN-PROD_SCRATCHDISK and submit production to CERN")
            selectedSites.append(_defaultSite)


        [outlfc,outsite,outputlocation]=job.outputdata.getDQ2Locations(selectedSites[0])
        if len(selectedSites)>1:
            [outlfc2,backup,backuplocation]=job.outputdata.getDQ2Locations(selectedSites[1])

        # app.se_name set: users wishes to get the output data written to another site than the one hosting the input.
        # One needs to ensure that this location is at least in the same cloud as the targetted processing site. This is done by insuring that the lfcs are the same.
        userSEs=[]
        outse=""
        if job.application.se_name and job.application.se_name != "none":
            userSEs=job.application.se_name.split(" ")
            # loop through userSEs until up to 2 valid sites are found...
            outse=""
            for SE in userSEs:
                [lfc,se,location]=job.outputdata.getDQ2Locations(SE)
                if lfc==outlfc:
                    if not outse:
                        outse=se # important to use outse and not outsite here, as outsite is used for selection of processing site.
                        # userSEs overrides outlfc,outputlocation, but not outsite as outsite is unfortunately used for choice of the processing site.
                        outputlocation=location
                    else:
                        outlfc2=lfc
                        backup=se
                        backuplocation=location
                        break
        # finally: if no backup location is defined at this point, enforce CERN-PROD_SCRATCHDISK as backup location
        if backup=="":
             [outlfc2,backup,backuplocation]=job.outputdata.getDQ2Locations(_defaultSite)
        
        logger.info("Final selection of output sites: %s , backup: %s" % (outsite,backup))
        try:
            assert outsite
        except:
            raise ApplicationConfigurationError(None,"Could not find suitable location for your output. Please subscribe your input dataset (if any) to a suitable location or change application.se_name to a suitable space token")


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

        environment["OUTLFC"]=outlfc
        environment["OUTSITE"]=outsite
        if outse:
           environment["OUTSITE"]=outse # user's choice for output storage location overriding AthenaMC's.
           
        environment["OUTPUT_LOCATION"]=outputlocation
        if spacetoken:
            environment["SPACETOKEN"]=spacetoken
        if backup:
            environment["OUTLFC2"]=outlfc2
            environment["OUTSITE2"]=backup
            environment["OUTPUT_LOCATION2"]=backuplocation

        environment["PROD_RELEASE"]=app.prod_release

        # setting environment["BACKEND"]
        # Local, Condor become "batch". LSF becomes "batch" unless the inputdata is on castor (in this case, it becomes "castor")
        environment["BACKEND"]=job.backend._name
        environment["BACKEND_DATA"]=app.backend_inputdata
        if job.backend._name=="LSF" and len(app.turls.values())>0:
            turl=app.turls.values()[0]
            if string.find(turl,"castor")>-1:
                environment["BACKEND_DATA"]="castor"
            else:
                environment["BACKEND_DATA"]="batch"
        if job.backend._name in ["Local","Condor","PBS"]:
            environment["BACKEND_DATA"]="batch"
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
            
        # user area:
        if app.userarea : 
            inputbox.append(File(app.userarea))
            environment['USER_AREA']=os.path.basename(app.userarea)


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
            
        if hasattr(job.backend,'requirements') and hasattr(job.backend.requirements,'sites') and hasattr(job.backend.requirements,'software') and hasattr(job.backend.requirements,'other') :
            requirements=job.backend.requirements
        else:
            requirements = AtlasLCGRequirements()
        
#        requirements.other.append('other.GlueCEStateStatus=="Production"') # missing production
        imax=string.rfind(app.atlas_rel,".")
        rel=string.atof(app.atlas_rel[:imax]) # to deal with string comparisons: [2-9].0.0 > 11.0.0. 
        if app.atlas_rel <= "11.4.0" or rel <=11.4:
            requirements.software=['VO-atlas-release-%s' % app.atlas_rel ]
        elif app.atlas_rel < "12.0.3":
            requirements.software=['VO-atlas-offline-%s' % app.atlas_rel ]
        elif app.atlas_rel >= "14.0.0" and app.atlas_rel<= "15.6.1":
            requirements.software=['VO-atlas-offline-%s-i686-slc4-gcc34-opt' % app.atlas_rel ]
        elif app.atlas_rel> "15.6.1":
            requirements.software=['VO-atlas-offline-%s-i686-slc5-gcc43-opt' % app.atlas_rel ]
        else:
            requirements.software=['VO-atlas-production-%s' % app.atlas_rel ]
        # case of prod_release set
        if app.prod_release:
            # no prod release tag before 13.0.X
            if app.atlas_rel < "14.0.0" and app.atlas_rel > "13.0.0":
                requirements.software=['VO-atlas-production-%s' % app.prod_release]
            elif app.atlas_rel>= "14.0.0" and app.atlas_rel<= "15.6.1":
                requirements.software=['VO-atlas-production-%s-i686-slc4-gcc34-opt' % app.prod_release]
            elif app.atlas_rel> "15.6.1":
                requirements.software=['VO-atlas-production-%s-i686-slc5-gcc43-opt' % app.prod_release]

        if app.transform_archive and string.find(app.transform_archive,"AtlasTier0")>-1:
            requirements.software=['VO-atlas-tier0-%s' % app.prod_release]
##        extraConfig=getConfig('defaults_AtlasLCGRequirements')
##        if  'dq2client_version' in extraConfig:
##            dq2client_version = extraConfig['dq2client_version']

##        if job.backend.requirements.dq2client_version:
##            dq2client_version = job.backend.requirements.dq2client_version
##        try:
##            assert dq2client_version!=""
##        except:
##            raise  ApplicationConfigurationError(None,"Please give a value to dq2client_version in job.backend.requirements.")
        
#        requirements.software += ['VO-atlas-dq2clients-%s' % dq2client_version]
#        requirements.other+=['RegExp("VO-atlas-dq2clients",other.GlueHostApplicationSoftwareRunTimeEnvironment)']

        # controlled relaxation for simple cases: one single input dataset, less than 200 subjobs. In this case, the subjobs can be submitted to the whole cloud.
        loosematch="true"
        if job.splitter and job.splitter.numsubjobs>200:
            loosematch="false"
        if job.inputdata and (job.inputdata.cavern or job.inputdata.minbias):
            loosematch="false"
# commented the nex block out as stage-in.sh can now ensure that the local copy is downloaded in the first attempt. However, as a safety net, we maintain the veto on complex jobs with pileup and or minbias, because they are heavy weight anyway and should not be run everywhere.
#        if app.dbrelease: 
#            loosematch="false"
        if len(job.backend.requirements.sites)>0:
            loosematch="false" # specified sites take precedence over cloud.
            
        userCloud=job.backend.requirements.cloud
        if userCloud=='ALL':
            userCloud='' # not supporting the AthenaLCGRequirements catch-all
        # By default: job to data, strict: target outsite and nothing else.
        requirements.sites=outsite
        
        if loosematch=="true" and userCloud :
            logger.debug("Your job qualifies for controlled relaxation of the current job-to-data policy. Now checking that requested cloud matches with input data")
            
            from dq2.info.TiersOfATLAS import whichCloud,ToACache
            targetSites=whichCloud(outsite)
            cloud=""
            for cloudID,sites in ToACache.dbcloud.iteritems():
                if sites==targetSites:
                    cloud=cloudID
            try:
                assert cloud==userCloud
            except:
                raise ApplicationConfigurationError(None,"Requested cloud: %s did not match selected processing cloud: %s. Reverting to submission to site %s" % (userCloud,cloud,outsite))

            requirements.cloud=cloud
            # looks like cloud has to be converted in a list of sites anyway, and this is not done in AtlasLCGRequirements.convert()... 
            allsites=requirements.list_sites_cloud()
            try:
                assert len(allsites)>0
            except:
                raise ApplicationConfigurationError(None,"Could not get any sites from the specified cloud: %s. You will have to specify a target site in job.backend.requirements.sites" % cloud)
            # need to weed out unwanted sites from excluded list
            excludedSites=requirements.excluded_sites
            goodsites=[]
            for checksite in allsites:
                selsite=True
                for site in excludedSites:
                    imax=site.find("_")
                    shortSite=site[:imax]
                    if shortSite in checksite:
                      #  print "site is excluded, skipping ", checksite
                        selsite=False
                        break
                if selsite and checksite not in goodsites:
                    goodsites.append(checksite)
#            print len(allsites),len(goodsites)
            if len(goodsites)>0:
                allsites=goodsites
            job.backend.requirements.sites=allsites
            logger.debug("Relaxing job to data policy to job to cloud. Selected cloud is %s" % cloud)

        logger.debug("master job submit?")
        
        if job.backend._name=="LCG" or job.backend._name=="Cronus" or job.backend._name=="Condor" or job.backend._name=="NG":
            return LCGJobConfig("",inputbox,[],outputbox,environment,[],requirements)
        else :
            return StandardJobConfig("",inputbox,[],outputbox,environment)



        
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        """Prepare the job"""

        inputbox=[ ]

        #       prepare environment
        environment={}
        environment=jobmasterconfig.env.copy()
        environment["INPUTDATASETS"]=""
        environment["INPUTFILES"]=""
        environment["INPUTTURLS"]=""

        alllfns=app.inputfiles+app.cavernfiles+app.mbfiles+app.dbfiles
        guids=app.turls
        guids.update(app.cavern_turls)
        guids.update(app.minbias_turls)
        guids.update(app.dbturls)
        
        infilenr=0
        for infile in alllfns:
            environment["INPUTFILES"]+="lfn[%d]='%s';" %(infilenr,infile)
            environment["INPUTDATASETS"]+="dset[%d]='%s';"%(infilenr,app.dsetmap[infile])
##            insites=app.sitemap[infile]
##            # compare with environment["OUTSITE"] and reorder if needed.
##            newinsites=self.sortSites(insites,environment["OUTSITE"])
##            environment["INPUTSITES"]+="site[%d]='%s';"%(infilenr,newinsites)
            environment["INPUTTURLS"]+="turl[%d]='%s';"%(infilenr,guids[infile])
            
            infilenr += 1


        logger.debug("%s %s %s" % (str(environment["INPUTDATASETS"]),str(environment["INPUTTURLS"]),str(environment["INPUTFILES"])))
        
        if environment["INPUTDATASETS"] :
            # Work around for glite WMS spaced environement variable problem
            inputbox += [ FileBuffer('inputdsets.conf',environment['INPUTDATASETS']+'\n') ]
        if environment["INPUTTURLS"] :
            # Work around for glite WMS spaced environement variable problem
            inputbox += [ FileBuffer('inputturls.conf',environment['INPUTTURLS']+'\n') ]
        if environment["INPUTFILES"] :
            # Work around for glite WMS spaced environement variable problem
            inputbox += [ FileBuffer('inputfiles.conf',environment['INPUTFILES']+'\n') ]

# now doing output files....
        job = app._getParent() # Returns job or subjob object

        outfilelist=""
        for type in app.outputpaths.keys():
            if type=="LOG" and "LOG" not in job.outputdata.outrootfiles:
                # logfiles are no longer saved in DQ2 datasets unless they are explicitly named in the outrootfiles dictionnary
                continue
            outfilelist+=app.outputpaths[type]+app.subjobsOutfiles[job.id][type]+" "

        environment["OUTPUTFILES"]=outfilelist
        # Work around for glite WMS spaced environement variable problem
        inputbox += [ FileBuffer('outputfiles.conf',environment['OUTPUTFILES']+'\n') ]        

 # setting up job wrapper arguments.       
        args=app.args
        trfargs=' '.join(app.args[4:])
        inputbox += [ FileBuffer('trfargs.conf',trfargs+'\n') ]
        jid=""
        if job._getRoot().subjobs:
            jid = job._getRoot().id
        else:
            jid = "%d" % job.id
        environment["OUTPUT_JOBID"]=str(jid) # used for versionning
        if app.dryrun:
            environment["DRYRUN"] = "TRUE"
        if app.dbrelease:
            environment["ATLASDBREL"]=app.dbrelease
        inputdata = []

        filename="wrapper.sh"
        exe = os.path.join(os.path.dirname(__file__),filename)

#       output sandbox
        outputbox =jobmasterconfig.outputbox

        if job.backend._name=="LCG" or job.backend._name=="Cronus" or job.backend._name=="Condor" or job.backend._name=="NG" or job.backend._name=="SGE":
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
allHandlers.add('AthenaMC','Condor',AthenaMCLCGRTHandler)
allHandlers.add('AthenaMC','NG',AthenaMCLCGRTHandler)


config = getConfig('AthenaMC')
logger = getLogger('AthenaMC')
