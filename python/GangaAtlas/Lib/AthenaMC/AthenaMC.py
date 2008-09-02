###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaMC.py,v 1.3 2008-09-02 13:32:44 fbrochu Exp $
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


class AthenaMC(IApplication):
    """The main Athena MC Job Handler for JobTransformations"""

    _schema = Schema(Version(2,0), {
        'random_seed'        : SimpleItem(defvalue='1',doc='Random Seed for MC Generator',typelist=["str"]),
        'evgen_job_option'         : SimpleItem(defvalue='',doc='JobOption filename, or path is modified locally',typelist=["str"]),
        'production_name'    : SimpleItem(defvalue='',doc='Name of the MC production',typelist=["str"]),
        'process_name'       : SimpleItem(defvalue='',doc='Name of the generated physics process',typelist=["str"]),
        'run_number'         : SimpleItem(defvalue='',doc='Run number',typelist=["str"]),
        'number_events_job'  : SimpleItem(defvalue='1',doc='Number of events per job',typelist=["str"]),
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
        'partition_number'  : SimpleItem(defvalue='',doc='output partition number'),
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
       """Prepare the job from the user area"""
       
       
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
                
        
       return (0,None)

from Ganga.GPIDev.Adapters.ISplitter import ISplitter

class AthenaMCSplitterJob(ISplitter):
    """AthenaMC handler job splitting"""
    
    _name = "AthenaMCSplitterJob"
    _schema = Schema(Version(1,0), {
       'numsubjobs': SimpleItem(defvalue=1,sequence=0, doc='Number of subjobs'),
       'nsubjobs_inputfile'  : SimpleItem(defvalue=1,sequence=0,doc='Number of subjobs processing one inputfile (N to M splitting)')
        } )

    ### Splitting based on numsubjobs
    def split(self,job):
        from Ganga.GPIDev.Lib.Job import Job
        subjobs = []

        try:
            assert self.numsubjobs
        except AssertionError:
            logger.error('numsubjobs: No number of subjobs given')
            raise

        
        offset = 0
        if job.outputdata and job.outputdata.output_firstfile>1:
            offset=job.outputdata.output_firstfile-1
        logger.debug("doing job splitting: %d subjobs" % self.numsubjobs)

        for i in range(self.numsubjobs):
            rndtemp = int(job.application.random_seed)+i+offset
            j = Job()
            j.application = job.application
            j.application.random_seed = "%s" % rndtemp
            j.backend=job.backend
            j.inputdata=job.inputdata
            j.outputdata=job.outputdata
            j.inputsandbox=job.inputsandbox
            j.outputsandbox=job.outputsandbox

            subjobs.append(j)

        return subjobs


config = makeConfig('AthenaMC', 'AthenaMC configuration options')
logger = getLogger()

# some default values



# $Log: not supported by cvs2svn $
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
