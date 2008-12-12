###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaMC.py,v 1.6 2008-12-12 10:17:42 elmsheus Exp $
###############################################################################
# AthenaMC Job Handler
#


import os, re, string, commands

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

from Ganga.Core import FileWorkspace

from Ganga.Utility.Config import makeConfig, ConfigError
from Ganga.Utility.logging import getLogger

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from Ganga.GPIDev.Credentials import GridProxy

from GangaAtlas.Lib.AthenaMC.AthenaMCDatasets import matchFile, expandList



class AthenaMC(IApplication):
    """The main Athena MC Job Handler for JobTransformations"""

    _schema = Schema(Version(2,0), {
        'random_seed'        : SimpleItem(defvalue='1',doc='Random Seed for MC Generator',typelist=["str"]),
        'evgen_job_option'         : SimpleItem(defvalue='',doc='JobOption filename, or path is modified locally',typelist=["str"]),
        'production_name'    : SimpleItem(defvalue='',doc='Name of the MC production',typelist=["str"]),
        'process_name'       : SimpleItem(defvalue='',doc='Name of the generated physics process',typelist=["str"]),
        'run_number'         : SimpleItem(defvalue='',doc='Run number',typelist=["str"]),
        'number_events_job'  : SimpleItem(defvalue=1,doc='Number of events per job',typelist=["int"]),
        'atlas_release'      : SimpleItem(defvalue='',doc='ATLAS Software Release',typelist=["str"]),
        'transform_archive'  : SimpleItem(defvalue='',doc='Name or Web location of a modified ATLAS transform archive.',typelist=["str"]),
        'se_name'            : SimpleItem(defvalue='none',doc='Name of prefered SE or DQ2 site (from TierOfAtlas.py) for output',typelist=["str"]),
        'mode'               : SimpleItem(defvalue='',doc='Step in the generation chain (evgen, simul (is simul+digit), recon, template). template is to use any transformation not coverd by any of the three previous steps.'),
        'transform_script'     : SimpleItem(defvalue='',doc='File name of the transformation script to use',typelist=["str"]),
        
#        'input_firstfile'           : SimpleItem(defvalue=1,doc='simul,recon: lowest partition number to be processed from input dataset'),
#        'number_inputfiles'  : SimpleItem(defvalue=1,sequence=0,doc='Number of inputfiles to process. With input_firstfile, defines a subset of inputfiles to be processed (subset of size number_inputfiles starting with partition number= input_firstfile)'),
#        'output_firstfile'   : SimpleItem(defvalue=1,doc='offset for output file partition numbers. First job will generate the partition number output_firstfile, second will generate output_firstfile+1, and so on...'),
        'firstevent'         : SimpleItem(defvalue=1,doc='evgen: sets first event number to be generated (in first job. The first event number in second job will be firstevent+number_events_job and so on...). simul, recon: decides how many events to be skipped in input files (= skip +1). This is propagated to all subjobs.',typelist=["int"]),
        'extraArgs'          : SimpleItem(defvalue='',doc='Extra arguments for the transformation, fixed value (experts only)',typelist=["str"]),
        'extraIncArgs'       : SimpleItem(defvalue='',doc='Extra integer arguments for the transformation, with value increasing with the subjob number. Please set like this: extraIncArgs="arg1=val1_0 arg2=val2_0" with valX_0 the value taken by the argument at the first subjob. On the second subjob, the arguments will have the value valX_0 + 1 and so on...  (experts only)',typelist=["str"]),
        'geometryTag'        : SimpleItem(defvalue='ATLAS-DC3-05',doc='Geometry tag for simulation and reconstruction',typelist=["str"]),        
        'partition_number'  : SimpleItem(defvalue=None,doc='output partition number',typelist=['type(None)',"int"]),
        'triggerConfig' : SimpleItem(defvalue='NONE',doc='recon, 12.0.5 and beyond: trigger configuration',typelist=["str"]),
        'version' : SimpleItem(defvalue='',doc='version tag to insert in the output dataset and file names',typelist=["str"]),
        'verbosity' : SimpleItem(defvalue='ERROR',doc='Verbosity of transformation for log files',typelist=["str"]),
        'siteroot' : SimpleItem(defvalue='',doc='location of experiment software area for non-grid backends.',typelist=["str"]),
        'cmtsite' : SimpleItem(defvalue='',doc='flag to use kit or cern AFS installation. Set to CERN for the latter, leave unset otherwise.',typelist=["str"])
        })
    
    _category = 'applications'
    _name = 'AthenaMC'
    _exportmethods = ['prepare', 'postprocess']
    _GUIPrefs= [ { 'attribute' : 'mode', 'widget' : 'String_Choice', 'choices' : ['evgen','simul','recon','template']}, { 'attribute' : 'verbosity', 'widget' : 'String_Choice', 'choices' : ['ALL','VERBOSE','DEBUG','INFO','WARNING','ERROR','FATAL']}]
    
     
    def postprocess(self):
       """Determine outputdata and outputsandbox locations of finished jobs
       and fill output variable"""
       from Ganga.GPIDev.Lib.Job import Job
       job = self._getParent()
       if job.outputdata:
          job.outputdata.fill()
              
    def prepare(self):
       """Prepare each job/subjob from the user area"""

    def getPartitionList(self):
        """ Calculates the list of partitions that should be processed by this application. 
            If no splitter is present, the list has always length one.
            Returns the tuple (list of partitions, boolean 'open'), where 'open' is True if the last 
            entry in the list is the beginning of an open range. """

        job = self.getJobObject()
        # If we are in simul/recon mode the partition can be specified using firstevent
        if self.mode != "evgen" and self.partition_number == None:
           firstpartition = 1 + (self.firstevent-1)/self.number_events_job
        else:
           if self.mode != "evgen" and self.firstevent != 1:
              logger.error("Except for evgen jobs, app.firstevent is an alternative to app.partition_number. You can not specify both at the same time!")
              raise Exception()
           firstpartition = self.partition_number
        if not job.splitter:
           return ([firstpartition], False)
        else:
           # First, use the splitter variables
           if (not job.splitter.output_partitions) and (not job.splitter.input_partitions):
              ## process numsubjobs or all partitions
              if job.splitter.numsubjobs:
                 return (range(firstpartition, firstpartition+job.splitter.numsubjobs), False)
              else:
                 return ([firstpartition], True) # open range starting at firstpartition
           elif (not job.splitter.output_partitions) ^ (not job.splitter.input_partitions): # ^ is XOR
              if job.splitter.output_partitions:
                 return expandList(job.splitter.output_partitions)
              else:
                 inputs = expandList(job.splitter.input_partitions)
                 return (self.getPartitionsForInputs(inputs[0], job.inputdata), inputs[1]) # propagate open range
           else:
              logger.error("Either splitter.output_partitions or splitter.input_partitions can be specified, but not both!")
              raise Exception()

    def getInputPartitionInfo(self, ids):
        """ Returns the tuple (jobs_per_input, inputs_per_job, skip_files, skip_jobs). The variables are:
            jobs_per_input: Number of jobs it takes to process a full input file.
            inputs_per_job: Number of input files a job processes
            skip_files: How many input files are skipped
            skip_jobs: How many jobs are skipped in numbering, i.e. in simulation if the first 1000 events 
                 of one large input file are skipped, partition number one would start at event number 1001."""
        if not ids:
            logger.error("No input dataset specified!")
            raise Exception()
        # This strange division rounds correctly, for example -((-10)/10) == 1,
        jobs_per_input = -((-ids.number_events_file)/self.number_events_job)
        inputs_per_job = -((-self.number_events_job)/ids.number_events_file)
        skip_files = ids.skip_files + ids.skip_events/ids.number_events_file
        skip_jobs = (ids.skip_events % ids.number_events_file) / self.number_events_job
        return (jobs_per_input, inputs_per_job, skip_files, skip_jobs)
  
    def getPartitionsForInputs(self,inputs,ids):
        """ Returns a list of partition numbers that process the given input files"""
        (jobs_per_input, inputs_per_job, skip_files, skip_jobs) = self.getInputPartitionInfo(ids)
        partitions = []
        # loop over the actual input partition numbers
        for inputpart in [input - 1 - skip_files for input in inputs]:
            firstjob = jobs_per_input*inputpart/inputs_per_job + 1 - skip_jobs
            partitions.extend([i for i in range(firstjob, firstjob + jobs_per_input) if i > 0])
        partitions = dict([(i,1)for i in partitions]).keys() # make unique
        partitions.sort()
        return partitions

    def getInputsForPartitions(self,partitions,ids):
        """ Returns a list of input files that are needed by the given jobs (given as partition numbers)"""
        if not ids:
            return []
        (jobs_per_input, inputs_per_job, skip_files, skip_jobs) = self.getInputPartitionInfo(ids)
        inputs = []
        # loop over the partition numbers and collect inputs
        for p in partitions:
            firstinput = inputs_per_job * (p - 1 + skip_jobs)/jobs_per_input + skip_files + 1
            inputs.extend(range(firstinput, firstinput+inputs_per_job))
        inputs = dict([(i,1)for i in inputs]).keys() # make unique
        inputs.sort()
        return inputs

    def getFirstEvent(self, partition,ids):
        """ For a given partition, return the first event in the first input file that has to be processed. 
            Returns a tuple (firstevent, numevents) where numevents is the adjusted number of events to be processed."""
        if not ids:
            return (self.firstevent + partition * self.number_events_job, self.number_events_job)
        (jobs_per_input, inputs_per_job, skip_files, skip_jobs) = self.getInputPartitionInfo(ids)
        if partition == 1:
            skip = (ids.skip_events % ids.number_events_file) % self.number_events_job
        else:
            skip = 0
        return (1 + ((partition - 1 + skip_jobs) % jobs_per_input) * self.number_events_job + skip, self.number_events_job - skip)
 
    def configure(self,masterappconfig):
       return (None,None)
    
    def master_configure(self):
       """Prepare the job from the user area"""
       try:
          assert self.mode in [ 'evgen', 'simul' , 'recon' , 'template']
       except AssertionError:
          logger.error('Variable application.mode: must be evgen, simul or recon or template')
          raise

       try:
           assert self.atlas_release
           assert self.number_events_job
       except AssertionError:
           logger.error('Please provide a start value for parameter atlas_release, number_events_job')
           raise
       if (self.mode != 'template'):
           try:
               assert self.production_name
               assert self.process_name
               assert self.run_number
           except AssertionError:
               logger.error('Please provide a start value for parameters production_name, process_name, run_number')
               raise
       
       if self.mode == "evgen":
          try:
             assert self.evgen_job_option
          except AssertionError:
             logger.error('Please provide a start value for parameter evgen_job_option needed for any evgen transformation')

          
       isJT=string.find(self.transform_archive,"JobTransform")
       isAP=string.find(self.transform_archive,"AtlasProduction")
       ##       try:
##          assert (isJT>-1 or isAP>-1)
##       except:
##           logger.error('Transformation archive name must contain either AtlasProduction or JobTransform')
##           raise
       if isJT==-1 and self.mode in [ 'simul' , 'recon' ]:
           try:
               assert self.geometryTag
           except AssertionError:
               logger.error('Variable application.geometryTag: In step simul or recon with AtlasProduction python transforms, please provide detector geometry version tag')
               raise
           
       if self._getRoot().splitter:
           try:
               assert self._getRoot().splitter._name=="AthenaMCSplitterJob"
           except AssertionError:
               logger.error('If you want to use a job splitter with the AthenaMC application, you have to use AthenaMCSplitterJob')
               raise

       # enforce the use of AthenaMCOutputDataset
       try:
          assert (self._getRoot().outputdata and self._getRoot().outputdata._name=="AthenaMCOutputDatasets")
       except AssertionError:
          logger.error('AthenaMC now requires to set outputdata to AthenaMCOutputDatasets')
          raise

       # doing the cross-check of splitter variables.
       if self._getRoot().inputdata:
          try:
             assert self._getRoot().inputdata._name=="AthenaMCInputDatasets"
          except AssertionError:
             logger.error('AthenaMC requires to use AthenaMCInputDatasets for inputdata')
             raise
          inputdata = self._getRoot().inputdata.get_dataset(self, self._getRoot().backend._name)
          if len(inputdata)!= 3:
             logger.error("Error, wrong format for inputdata %d, %s" % (len(inputdata),inputdata))
             raise Exception("Input file not found")
          self.turls=inputdata[0]
          self.lfcs=inputdata[1]
          self.sites=inputdata[2]


        
       return (0,None)

from Ganga.GPIDev.Adapters.ISplitter import ISplitter

class AthenaMCSplitterJob(ISplitter):
    """AthenaMC handler job splitting"""
    
    _name = "AthenaMCSplitterJob"
    _schema = Schema(Version(1,0), {
       'numsubjobs': SimpleItem(defvalue=0,sequence=0, doc='Limit the number of subjobs. If this is left at 0, all partitions will be processed.'),
       'input_partitions' : SimpleItem(defvalue="",doc='List of input file numbers to be processed, either as a string in the format "1,3,5-10,15-" or as a list of integers. Alternative to output_partitions',typelist=["str","list"]),
       'output_partitions' : SimpleItem(defvalue="",doc='List of partition numbers to be processed, either as a string in the format "1,3,5-10,15-" or as a list of integers. Alternative to input_partitions',typelist=["str","list"])
        } )

    ### Splitting based on numsubjobs
    def split(self,job):
        from Ganga.GPIDev.Lib.Job import Job
        subjobs = []

        partitionList = job.application.getPartitionList()
        partitions = partitionList[0]
        openrange = partitionList[1]
        if self.numsubjobs:
            partitions = partitions[:self.numsubjobs]
            if openrange and len(partitions) < self.numsubjobs:
                missing = self.numsubjobs - len(partitions)
                partitions.extend(range(partitions[-1]+1, partitions[-1]+1+missing))
            openrange = False
        elif openrange:

            inputnumbers = job.application.getInputsForPartitions(partitions, job.inputdata)
            if inputnumbers:
                matchrange = (job._getRoot().inputdata.numbersToMatcharray(inputnumbers), openrange)
            else:
                matchrange = ([],False)
            infiles = [fn for fn in job.application.turls.keys() if matchFile(matchrange, fn)]
            innumbers = job._getRoot().inputdata.filesToNumbers(infiles)
            partitions = partitions[:-1] # the partition start of the open range beginning is not mandatory 
            partitions.extend(job.application.getPartitionsForInputs(innumbers, job.inputdata))
            partitions = dict([(i,1)for i in partitions]).keys() # make unique
            logger.warning("Number of subjobs %i determined from input dataset!" % len(partitions))

        try:
            assert partitions
        except AssertionError:
            logger.error('Partition to process could not be determined! Check if inputdata.skip_files or inputdata.skip_events do not skip your specified input partition!')
            raise
        
        for p in partitions:
            rndtemp = int(job.application.random_seed)+p
            j = Job()
            j.application = job.application
            j.application.random_seed = "%s" % rndtemp
            j.backend=job.backend
            j.inputdata=job.inputdata
            j.outputdata=job.outputdata
            j.inputsandbox=job.inputsandbox
            j.outputsandbox=job.outputsandbox
            j.application.partition_number=p
            subjobs.append(j)
        return subjobs

config = makeConfig('AthenaMC', 'AthenaMC configuration options')
logger = getLogger()

# some default values



# $Log: not supported by cvs2svn $
# Revision 1.5  2008/10/21 06:43:09  elmsheus
# Change defvalue of partition_number
#
# Revision 1.4  2008/10/15 13:44:05  fbrochu
# Bug fix for inputdata.inputfiles type, improving timeout evaluation for stage-out, allowing support for stage-in files from SRMV2 space tokens, extending splitter to allow partial resubmission of master jobs (all failed subjobs at once instead of one by one.)
#
# Revision 1.3  2008/09/02 13:32:44  fbrochu
# Gave a type and a default value to app.number_events_job
#
# Revision 1.2  2008/07/30 13:23:55  fbrochu
# AthenaMC.py: added type checks to members. AthenaMCDatasets: bug fix for treatment of DQ2 datasets available only for Panda. AthenaMCLCGRTGandler.py and wrapper.sh: extension of the glite WMS workaround to input data
#
# Revision 1.1  2008/07/17 16:41:19  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.19.2.5  2008/04/22 11:23:22  fbrochu
# bug fix for template mode: taking into account first event/ skip parameters in list of transformation parameters
#
# Revision 1.19.2.4  2008/04/21 15:55:56  fbrochu
# Added new production mode, template, to handle any transformation not covered by the three existing modes. template arguments must be entered using application.extraArgs and extraIncArgs. See internal doc of application.mode for more details. Simplified wrapper.sh to allow support for input sandbox files (input weight files, custom job option fragments). Extended application.atlas_release to support 4-digits production cache releases. Using a 4-digit number in application.atlas_release will force the job to be sent to a site where the requested production cache is already deployed, meaning that application.transform_archive is not needed in this particular case. 3-digit numbers are still supported.
#
# Revision 1.19.2.3  2008/04/16 13:51:19  fbrochu
# cleaning up job scripts wrapper.sh and setup-release.sh to reduce spurrious error messages in stderr. Added new production mode, template, to cover all transformations that cannot be run with the three existing modes. template relies solely on the use of application.extraArgs and application.extraIncArgs to fill the transformation parameters. Input and Output data must be declared in dedicated inputdata and outputdata members, and then the relevant members must be declared in application.extraArgs like this: outputEvgenFile= if the file was declared in outputdata.outrootfile.
#
# Revision 1.19.2.2  2008/02/08 16:53:11  fbrochu
# Added support to PBS backend, new application members siteroot and cmtsite for ATLAS setup for Local and PBS job submissions, bug fixes for input dataset preparation and staging , handling of missing output files in fill()
#
# Revision 1.19.2.1  2008/01/07 13:37:10  elmsheus
# Update to new config schema
#
# Revision 1.19  2007/11/27 11:52:27  fbrochu
# bug fix for output_dataset broken string in AthenaMCDatasets.py and support for extra integer transformation variables incrementing with the subjob number (extraIncArgs)
#
# Revision 1.18  2007/07/30 09:26:04  elmsheus
# Move 4-4-0-dev-branch to head
#
# Revision 1.17.2.2  2007/07/30 09:09:57  fbrochu
# warning suppression
#
# Revision 1.17.2.1  2007/06/19 14:46:50  fbrochu
# initial export for 4.4.0 developments
#
# Revision 1.17  2007/05/29 11:27:54  fbrochu
# cosmetic changes: removal of extra coma at the end of application scheme in AthenaMC.py, downgrading two warnings to info and debug level
#
# Revision 1.16  2007/05/29 10:37:01  fbrochu
# Support for remote transformation archives (from web area repositories only, including official area), embedded all grid transactions of AthenaMCLCGRTHandler.py in gridShell calls and added verbosity member to AthenaMC application to control transformation verbosity
#
# Revision 1.15  2007/05/08 16:19:42  elmsheus
# Several patch provided by Johannes Ebke
#
# Revision 1.14  2007/05/04 08:14:44  fbrochu
# Corrected logic bug in AthenaMCLCGRTHandler.py, preventing processing of user-named datasets. Changed default value of version to the null string
#
# Revision 1.13  2007/05/01 09:42:09  fbrochu
# fixed typos in AthenaMC.py (new member version) and AthenaMCLCGRTHandler.py (support to Condor Backend). Revisited verbosity level of most warnings
#
# Revision 1.12  2007/04/24 15:34:58  fbrochu
# Added support to Nordugrid, Cronus and Condor backends. Revisited file naming conventions, added new application member: version
#
# Revision 1.11  2007/03/15 09:59:36  fbrochu
# corrected username bug in AthenaMC, added automatic registration of input castor files for LCG jobs
#
# Revision 1.10  2007/03/07 09:33:47  elmsheus
# Rename fill to postprocess
#
# Revision 1.9  2007/03/07 08:19:01  elmsheus
# Add fill method in application
#
# Revision 1.8  2007/02/23 13:25:11  fbrochu
# Port 4.2.X bug fixes, implement timestamp in output files, add support for LSF backend and local CASTOR files, redesigned I/O file management
#
# Revision 1.7  2006/11/24 13:32:38  elmsheus
# Merge changes from Ganga-4-2-2-bugfix-branch to the trunk
# Add Frederics changes and improvement for AthenaMC
#
# Revision 1.6  2006/09/26 11:17:09  elmsheus
# Frederic: Merge of AthenaMC and AthenaMCpyJT, DQ2 updates
#
# Revision 1.5  2006/09/05 10:25:49  elmsheus
# Some bugfixes from Frederic
#
# Revision 1.2  2006/05/25 14:24:29  elmsheus
# Fix numsubjobs defvalue, introduce offset_job and T_CONTEXT
#
# Revision 1.1  2006/05/19 09:26:22  elmsheus
# initial import of AthenaMC JobTransform wrapper
#
