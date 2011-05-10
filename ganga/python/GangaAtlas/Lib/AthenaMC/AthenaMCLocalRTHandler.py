###############################################################################
# Ganga Project. http://cern.ch/ganga
#
###############################################################################
# AthenaMC Local Runtime Handler
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

from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset import DQ2OutputDataset
from GangaAtlas.Lib.AthenaMC.AthenaMCDatasets import extractFileNumber, matchFile


from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

# the config file may have a section
# aboout monitoring

mc = getConfig('MonitoringServices')

# None by default

#mc.addOption('AthenaMC', None, 'FIXME')
mc.addOption('AthenaMC/Local', None, 'FIXME')

_defaultSite='CERN-PROD_SCRATCHDISK'

class AthenaMCLocalRTHandler(IRuntimeHandler):
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
        if app.cmtsite:    
            os.environ["CMTSITE"]=app.cmtsite

        job = app._getParent()
        if app.dryrun:
            os.environ["SITEROOT"]  = "NONE"
            os.environ["CMTSITE"]  = "NONE"
        try:
            assert "SITEROOT" in os.environ
        except:
            raise ApplicationConfigurationError(None," ATLAS environment not defined")
                
        try:
            assert "CMTSITE" in os.environ and os.environ["CMTSITE"]!=""
        except:
            raise ApplicationConfigurationError(None,"cmt not setup properly. Please check your ATLAS setup or run on the grid")
            
        if "AtlasVersion" in os.environ:
            logger.debug("Checking AtlasVersion: %s and selected atlas release %s" % (os.environ["AtlasVersion"],app.atlas_rel))
            try:
                assert app.atlas_release==os.environ["AtlasVersion"]
            except:
                logger.error("Mismatching atlas release. Local setup is %s, resetting requested atlas release to local value." % os.environ["AtlasVersion"])
                app.atlas_release=os.environ["AtlasVersion"]
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
        for tag in os.environ:
            if os.environ[tag]!="":
                environment[tag]=os.environ[tag]
        
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

        # select sites which are matching user's wishes, if any.
        selectedSites=app.sites
        print app.sites
        if job.backend._name!="Local":
            if len(selectedSites)>0:
                [outlfc,outsite,outputlocation]=job.outputdata.getDQ2Locations(selectedSites[0])
            if len(selectedSites)>1:
                [outlfc2,backup,backuplocation]=job.outputdata.getDQ2Locations(selectedSites[1])

        # app.se_name set: users wishes to get the output data written to another site than the one hosting the input.
        # One needs to ensure that this location is at least in the same cloud as the targetted processing site. This is done by insuring that the lfcs are the same.
        userSEs=[]
        outse=""
        if job.application.se_name and job.application.se_name != "none" and job.backend._name!="Local":
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
        if outsite=="":
            [outlfc,outsite,outputlocation]=job.outputdata.getDQ2Locations(_defaultSite)
        if backup=="":
             [outlfc2,backup,backuplocation]=job.outputdata.getDQ2Locations(_defaultSite)
        
        logger.info("Final selection of output sites: %s , backup: %s" % (outsite,backup))



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
        if job.backend._name=="LSF" and len(app.turls.values())>0:
            turl=app.turls.values()[0]
##            if string.find(turl,"castor")>-1:
##                environment["BACKEND"]="castor"
##            else:
##                environment["BACKEND"]="batch"
        if job.backend._name in ["Local","Condor","PBS"]:
            environment["SITEROOT"]=os.environ["SITEROOT"]
            environment["CMTSITE"]=os.environ["CMTSITE"]
            if job.backend._name in ["Condor","PBS"]:
                environment["BACKEND"]="batch"

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


        # backend specifics:
        if job.backend._name=="SGE" and job.backend.extraopts=="":
            job.backend.extraopts="-l h_vmem=5G -l s_vmem=5G -l h_cpu=1:00:00 -l h_fsize=10G" # minimum set up for Atlfast II
            
        logger.debug("master job submit?")
        
        return StandardJobConfig("",inputbox,[],outputbox,environment)



        
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        """Prepare the job"""

        inputbox=[ ]

        #       prepare environment
        environment={}
        environment=jobmasterconfig.env.copy()
        environment["INPUTDATASETS"]=""
        environment["INPUTSITES"]=""
        environment["INPUTFILES"]=""

        alllfns=app.inputfiles+app.cavernfiles+app.mbfiles+app.dbfiles
        infilenr=0
        for infile in alllfns:
            environment["INPUTFILES"]+="lfn[%d]='%s';" %(infilenr,infile)
            environment["INPUTDATASETS"]+="dset[%d]='%s';"%(infilenr,app.dsetmap[infile])
            insites=app.sitemap[infile]
            # compare with environment["OUTSITE"] and reorder if needed.
            newinsites=self.sortSites(insites,environment["OUTSITE"])
            environment["INPUTSITES"]+="site[%d]='%s';"%(infilenr,newinsites)
            infilenr += 1


                
        logger.debug("%s %s %s" % (str(environment["INPUTDATASETS"]),str(environment["INPUTSITES"]),str(environment["INPUTFILES"])))

        job = app._getParent() # Returns job or subjob object
        # if datasetType is DQ2, then one needs to ensure that DQ2 environment is properly set.
        if job.inputdata:
            if job.inputdata.datasetType=="DQ2":
                environment["BACKEND"]="LCG"
                try:
                    assert "DQ2_LOCAL_SITE_ID" in os.environ
                except:
                    logger.error("Error in DQ2 configuration. Please leave ganga, then rerun local DQ2 setup before restarting ganga. Or change inputdata.datasetType to 'local'")
                    raise
            elif job.backend._name=="Local":
                 environment["BACKEND"]="Local"
            else:
                 environment["BACKEND"]="batch"

# now doing output files....

        outfilelist=""
        subjob_outbox=[]
        for type in app.outputpaths.keys():
            outfilelist+=app.outputpaths[type]+app.subjobsOutfiles[job.id][type]+" "
            if job.application.se_name=="ganga":
                outfile1=app.subjobsOutfiles[job.id][type]
                subjob_outbox.append(outfile1)
        environment["OUTPUTFILES"]=outfilelist
        # Work around for glite WMS spaced environement variable problem
        inputbox += [ FileBuffer('outputfiles.conf',environment['OUTPUTFILES']+'\n') ]        


 # setting up job wrapper arguments.       
        args=app.args
        
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
        outputbox.extend(subjob_outbox)

        return StandardJobConfig(File(exe),inputbox,args,outputbox,environment) 



allHandlers.add('AthenaMC','Local',AthenaMCLocalRTHandler)
allHandlers.add('AthenaMC','LSF',AthenaMCLocalRTHandler)
#allHandlers.add('AthenaMC','Condor',AthenaMCLocalRTHandler) # condor has requirements...
allHandlers.add('AthenaMC','PBS',AthenaMCLocalRTHandler)
allHandlers.add('AthenaMC','SGE',AthenaMCLocalRTHandler)


config = getConfig('AthenaMC')
logger = getLogger('AthenaMC')
