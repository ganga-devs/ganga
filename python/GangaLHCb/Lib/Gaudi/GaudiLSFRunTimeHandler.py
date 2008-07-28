#!/usr/bin/env python

'''
Provide a Application runtime handler for Gaudi applications the 
LSF backend.
'''

__author__ = ' Andrew Maier, Greig A Cowan'
__date__ = 'June 2008'
__revision__ = 0.1

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import *

import sys,os,os.path
from Ganga.GPIDev.Lib.File import FileBuffer
from Ganga.GPIDev.Lib.File import File
import Ganga.Utility.Config 
from Ganga.Utility.util import unique

import Ganga.Utility.logging

from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

logger = Ganga.Utility.logging.getLogger()
class GaudiLSFRunTimeHandler(IRuntimeHandler):
  """This is the application runtime handler class for Gaudi applications 
  using the LSF backend."""
  
  def __init__(self):
   config=Ganga.Utility.Config.getConfig('LHCb') 
  
  def prepare(self,app,extra,appmasterconfig,jobmasterconfig):
    
    job= app.getJobObject()
    sandbox = []
    if extra.dataopts:
      #sandbox.append(FileBuffer("dataopts.py",extra.dataopts))
      sandbox.append(FileBuffer("dataopts.opts",extra.dataopts))

    logger.debug("extra dataopts: " + str(extra.dataopts))
    outsb = []
    
    for i in extra._outputfiles:
        outsb.append(i)
    
    for f in job.outputsandbox:
        outsb.append(f)
    
    logger.debug("Input sandbox: %s: ",str(sandbox))
    logger.debug("Output sandbox: %s: ",str(outsb))
    script=self.create_runscript(app,extra)

    return StandardJobConfig(FileBuffer('myscript',script,executable=1),inputbox=sandbox,args=[],outputbox=unique(outsb))
      
  def master_prepare(self,app,extra):
    
    job = app.getJobObject()
    logger.debug("Entering the master_prepare of the GaudiLSF Runtimehandler") 
    userdlls = extra._userdlls
    logger.debug("extra dlls: %s", str(extra._userdlls))
    sandbox=[]
    for f in userdlls:
        sandbox.append( File(f,subdir='lib'))
        
    sandbox += job.inputsandbox
    sandbox.append( FileBuffer('options.pkl', extra.opts_pkl_str))
    
    logger.debug("Input sandbox: %s: ",str(sandbox))

    return StandardJobConfig( '', inputbox = sandbox, args=[])


  def jobid_as_string(self,app):
      import os.path
      job=app.getJobObject()
      jstr=''
      # test is this is a subjobs or not
      if job.master: # it's a subjob
          jstr=str(job.master.id)+os.sep+str(job.id)
      else:
          jstr=str(job.id)
      return jstr



  def create_runscript(self,app,extra):
    
    job = app.getJobObject()
    config = Ganga.Utility.Config.getConfig('LHCb') 
    logger.debug("Entering the master_prepare of the GaudiLSF Runtimehandler") 
    version = app.version
    theApp = app.appname
    package = app.package
    lhcb_release_area = app.lhcb_release_area
    user_release_area = app.user_release_area
    platform = app.platform
    appUpper = theApp.upper()
    algpack = ''
    alg = ''
    algver = ''
    userdlls = extra._userdlls
    site = extra._LocalSite
    protocol = extra._SEProtocol
    jstr = self.jobid_as_string(app)
    copy_cmd = config['cp_cmd']
    mkdir_cmd = config['mkdir_cmd']
    joboutdir = config['DataOutput']
    opts = 'options.pkl'
    
    outputdatastr = ' '.join(extra.outputdata)
    
    if app.masterpackage is None:
      master = ''
    else:
      master = app.masterpackage
      (algpack,alg,algver)=app._parseMasterPackage()
    
    script="""#!/usr/bin/env bash
export CMTPATH=###CMTUSERPATH###
export ###THEAPP###_release_area=###CMTRELEASEAREA###
export DATAOUTPUT='###DATAOUTPUT###'
#DATAOPTS='dataopts.py'
DATAOPTS='dataopts.opts'
OPTS='###OPTS###'
JOBOUTPUTDIR=###JOBOUTDIR###/###JOBID###/outputdata
CP=###COPY###
MKDIR=###MKDIR###

if ! [ -f ${OPTS} ]; then 
   OPTS=notavailable
fi
 
echo "Using the master optionsfile: ${OPTS}"

if [ -f ${LHCBHOME}/scripts/SetupProject.sh ]; then
  . ${LHCBHOME}/scripts/SetupProject.sh  --ignore-missing --use="###ALG### ###ALGVER### ###ALGPACK###" ###THEAPP### ###VERSION###
else
  echo "Could not find the SetupProject.sh script. Your job will probably fail"
fi
# create an xml slice
if [ -f ${DATAOPTS} ]; then
    genCatalog -o ${DATAOPTS} -p myFiles.xml -s ###SITE### -P ###SEPROTOCOL### #ugly and hardcoded , FIXME
    echo >> ${DATAOPTS}
    echo 'FileCatalog.Catalogs += { "xmlcatalog_file:myFiles.xml" };' >> ${DATAOPTS}
fi    
# add the lib subdirectory in case some user supplied shared libs where copied to `pwd`/lib  
export LD_LIBRARY_PATH=".:`pwd`/lib:$LD_LIBRARY_PATH"
if  [ -f ${OPTS} ]; then
        export JOBOPTPATH=${OPTS}
else
        export JOBOPTPATH=$###THEAPP###_release_area/###APPUPPER###/###APPUPPER###_###VERSION###/###PACKAGE###/###THEAPP###/###VERSION###/options/job.opts
fi        

$GAUDIROOT/scripts/gaudirun.py ${OPTS} ${DATAOPTS}

$MKDIR -p $JOBOUTPUTDIR
for f in $DATAOUTPUT; do 
    $CP "$f" "${JOBOUTPUTDIR}/$f"
    echo "Copying $f to $JOBOUTPUTDIR"
    if [ $? -ne 0 ]; then
       echo "WARNING:  Could not copy file $f to $JOBOUTPUTDIR"
       echo "WARNING:  File $f will be lost"
    fi
    rm -f "$f"
done
#    export CASTOR_OUT="$CASTOR_HOME/gangadir/###JOBID###/outputdata/"
#    if [ -n $DATAOUTPUT ]; then
#        nsmkdir -p $CASTOR_OUT
#        for f in $DATAOUTPUT; do
#          rfcp "$f" "${CASTOR_OUT}/$f"
#          echo "Copying $f to ${CASTOR_OUT}"
#          if [ $? -ne 0 ]; then
#            echo "WARNING:  Could not copy file $f to $CASTOR_OUT"
#            echo "WARNING:  File $f will be lost"
#          fi
#          echo "Removing $f..."
#          rm -f $f
#        done
#    fi
#fi
"""
    script=script.replace('###CMTUSERPATH###',user_release_area)
    script=script.replace('###DATAOUTPUT###', outputdatastr)
    script=script.replace('###THEAPP###',theApp) 
    script=script.replace('###CMTRELEASEAREA###',lhcb_release_area)
    script=script.replace('###VERSION###',version)
    script=script.replace('###ALG###',alg)
    script=script.replace('###ALGVER###',algver)
    script=script.replace('###ALGPACK###',algpack)
    script=script.replace('###APPUPPER###',appUpper)
    script=script.replace('###PACKAGE###',package)
    script=script.replace('###PLATFORM###',platform)
    script=script.replace('###MASTER###',master)
    script=script.replace('###JOBID###',jstr)
    script=script.replace('###JOBOUTDIR###',joboutdir)
    script=script.replace('###SITE###',site)
    script=script.replace('###SEPROTOCOL###',protocol)
    script=script.replace('###COPY###',copy_cmd)
    script=script.replace('###MKDIR###',mkdir_cmd)
    script=script.replace('###OPTS###',opts)
    
    return script



#
#
# $Log: not supported by cvs2svn $
# Revision 1.39.12.3.2.5  2008/07/15 17:53:51  gcowan
# Modified PythonOptionsParser to pickle the options file. This is converted to a string and added to the Gaudi.extras. The string can then be converted back to an options.pkl file within the runtime handlers and added to the job sandboxes. This replaces the need to use flat_opts. There is no need to have the format() method in PythonOptionsParser.
#
# Revision 1.39.12.3.2.4  2008/07/14 19:08:38  gcowan
# Major update to PythonOptionsParser which now uses gaudirun.py to perform the complete options file flattening. Output flat_opts.opts file is made available and placed in input sandbox of jobs. LSF and Dirac handlers updated to cope with this new design. extraopts need to be in python. User can specify input .opts file and these will be converted to python in the flattening process.
#
# Revision 1.39.12.3.2.3  2008/07/10 17:36:11  gcowan
# Modified GaudiDirac RT handler to support python options files. Small bug fixed in PythonOptions where a string was returned rather than a list.
#
# Revision 1.39.12.3.2.2  2008/07/09 00:11:49  gcowan
# Modified Gaudi._get_user_dlls() to address Savannah bug #31165. This should allow Ganga to pick up user DLLs from multiple user project areas.
#
# Modified GaudiLSFRunTimeHandler to look for gaudirun.py in the correct location.
#
# Revision 1.39.12.3.2.1  2008/07/03 12:52:07  gcowan
# Can now successfully submit and run Gaudi jobs using python job options to Local() and Condor() backends. Changes in Gaudi.py, GaudiLSFRunTimeHandler.py, PythonOptionsParser.py, Splitters.py and GaudiDiracRunTimeHandler.py. More substantial testing using alternative (and more complex) use cases required.
#
# Revision 1.39.12.3  2008/05/22 20:50:15  uegede
# Updates to Gaudi.py and GaudiLSFRunTimeHandler to deal better with the
# ability to give a list of options files.
# Test cases updated to new exceptions in Ganga-5.
#
# Revision 1.39.12.2  2008/04/04 15:11:38  andrew
# Schema changes:
#   * make optsfile a list
#   * rename cmt_user_path to user_release_area
#   * rename cmt_release_area to lhcb_release_area
#
# Add type info to Gaudi schema
#
# Adapt code for schema changes
#
# Revision 1.39.12.1  2008/03/17 11:08:27  andrew
# Merge from head
#
# Revision 1.42  2008/03/07 15:13:40  andrew
#
# Fixes for:
#
# - [-] Bug fixes
#     - [+] bug #28955: cmt.showuses() broken
#     - [+] bug #33367: Option file format changed for specifying XML
#           slice
#     - [+] bug #29368: Dataoutput variable wrongly flagged as undefined
#     - [+] bug #33720: Multiple inclusion of options file
#
#
# Removes CERN centricity of the Gaudi wrapper script for batch and interactive
#
# Revision 1.41  2008/02/29 15:44:42  andrew
# Fix for bug #28955
#
# Revision 1.40  2008/02/18 12:36:26  andrew
# Fix missing s in FileCataogs.Catalog(s) in _determine_catalog_type()
#
# use _determine_catalog_type in GaudiLSFHandler
#
# Revision 1.39  2007/05/22 08:25:19  andrew
# fix for bug 26547 Empty directories created in Castor
#
# Revision 1.38  2007/03/12 08:48:16  wreece
# Merge of the GangaLHCb-2-40 tag to head.
#
# Revision 1.35.2.11  2007/02/19 15:16:04  andrew
# Fix for missing libraries? I hope so
#
# Revision 1.35.2.10  2007/02/08 15:17:00  andrew
# Changed to a Shell encapsulated version of the CMT and env scripts
#
# Revision 1.35.2.9  2007/02/06 16:21:54  andrew
# changed the cmt  module into a class, using the caches shell mechanism
# changed the env module to use a cached shell
#
# Revision 1.35.2.8  2007/01/10 10:06:40  andrew
# Fix for bug 22770
#
# Revision 1.35.2.7  2007/01/10 09:52:11  andrew
# test for branch
#
# Revision 1.35.2.6  2006/12/13 16:15:46  andrew
# Added fix for OptionsFileSplitter (did not include the subjob options)
#
# Added basic support for Vetra (needs some cleaning)
#
# Added GaussSplitter
#
# Started fixing the bookkeeping db stuff
#
# Revision 1.35.2.5  2006/11/28 16:32:41  andrew
# Yet another typo
#
# Revision 1.35.2.4  2006/11/27 17:31:40  andrew
# fix of the previous fix
#
# Revision 1.35.2.3  2006/11/25 22:21:52  andrew
# Fix for bug #21917
#
# Revision 1.35.2.2  2006/11/23 15:38:47  andrew
# *** empty log message ***
#
# Revision 1.35.2.1  2006/11/09 13:59:41  andrew
# Fixed bug for split jobs in local and lsf, which would overwrite the
# output, or create it in the wrong location
#
# Revision 1.35  2006/10/13 07:50:27  andrew
# Fix for #20659
#
# Revision 1.34  2006/10/05 14:49:02  andrew
# Fixed bugs related to the inner runscript
#
# Revision 1.33  2006/10/05 08:59:13  andrew
# Fixed stupid typo in Dirac (job instead of j)
#
# Fixed bugs related to Gaudi jobs without options file
#
# Revision 1.32  2006/10/02 15:25:47  andrew
# Fixes in case no options file is specified
#
# Revision 1.31  2006/10/02 07:52:32  andrew
# Fixed all of the following:
# Hi Andrew,
#
# I just noticed that you are missing a newline character at the end of
# the last line in the 'data_stupid_limitation_of_dirac'. It seems like
# Dirac doesn't care but it is poor practise not to terminate files with a
# newline and occasionally causes trouble. Please fix.
#
# Thanks,
#      Ulrik.
#
# Revision 1.30  2006/09/28 19:00:39  andrew
# Fixed bugs in shared sandbox mechanism (wrong dataset, wrong file used in GenCatalog)
#
# Revision 1.29  2006/09/28 12:33:46  andrew
# Tried to migrate the Dirac RT handler to shared sandbox
#
# Revision 1.28  2006/09/27 14:37:48  andrew
# Fixed PATH probekm. Shared inputsanbox now owrking for Local and batch
#
# Revision 1.27  2006/09/27 14:21:10  andrew
# Shared sandbox mechanism working for Local and Batch
#
# Revision 1.26  2006/09/26 13:19:44  andrew
# Added the mechanism for shared sanbox files. Needs testing
# The Gaudi option file is split into two parts:
#   If a job is
#     1) created from a dataset or if the job
#     2) is split (which implies 1)) or
#     3) the user has specified extraopts
#
# a second options file is generated. This 'dataopts.opts' file
# is then included vie the #include mechanism of Gaudi options files
# This will allow to use the shared sanbox mechnism (to be implemented)
#
# Revision 1.25  2006/08/14 08:23:07  andrew
# added a check ithe file created by genCatalog actually exists. If yes
# include it in the options
#
# Revision 1.24  2006/08/10 15:34:58  andrew
# Added a genCatalog hack for local/lsf/pbs handlers to allow the submission of
# LFNs in Gaudi jobs. The config options LocalSite and SEProtocol have to be set
# for this to work
#
#
# Added a fix to adjust the CPUtime set in the Dirac handler to match thoses of
# the "allowd" CPUTimes from Dirac
#
# Revision 1.23  2006/07/31 14:26:29  andrew
# added changes form framwork change
#
# Revision 1.22  2006/07/12 14:42:33  uegede
# Reduced verbosity in stderr of GaudiLSFRuntimehandler
#
# Bug 18127 fixed by protecting change of field separator
#
# Bug 18126 fixed by changing identification of line with Ntuple name
#
# Ntuple and histogram file are no longer added to outputsandbox field.
#
# Revision 1.21  2006/06/13 14:54:29  andrew
# Fixes for bugs
#
# bug #17345 - Outputsandbox for submitted Gaudi subjobs is incomplete
#
# bug #16362, libraries are copied twice
#
# Revision 1.20  2006/05/29 12:03:08  andrew
# Fixed typo
# (Closes bug #17097)
#
# Revision 1.19  2006/05/19 15:08:53  andrew
# Typo CASTORHOME instead of CASTOR_HOME
#
# Revision 1.18  2006/05/19 14:15:43  andrew
# Attempt to remove the CERN centricity of thei handler.
# Outputdata will be copyied to the jobs output directory (like the sandbox files)
# unless we are at CERN, in which case the files will be copied to castor.
#
# Revision 1.17  2006/03/08 14:54:30  andrew
# Removed print statment or converted them into logger statments
#
# Revision 1.16  2006/03/06 11:29:16  andrew
# Changed to  Adapter interface
#
# Implemented  optimized splitting (works for LSF, probably not for DIRAC)
#
# Revision 1.15  2006/01/26 14:02:23  andrew
# Fix for [bug #12587] Gaudi handler confused if master package has fewer than 2 slashes
# In addition the master package can now be given in 'cmt' style
#
# Revision 1.14  2006/01/25 15:11:31  andrew
# Fix a bug which sourced the wrong setup file, if the user did not specify
# a masterpackage. Previously the application setup form the release area
# was source, and not the setup checked out (potentialy modified by the user)
#
# Revision 1.13  2005/11/22 15:32:21  andrew
# * Fixed a missing " in the application script
#
# * Moved the default dirac to be imported to /afs/cern.ch/lhcb/software/releases/DIRAC/latest
# (which is currently a link to v2r8b8 in the DEV area)
#
# Revision 1.12  2005/11/17 13:45:03  andrew
# Remove a possible PFN: from the output-data
#
# Revision 1.11  2005/11/17 13:33:10  andrew
# * Added changes for to handle outputdata
#   Outputdata is parsed from the optionsfile. All files created from either
#   GaussTape, DigiWriter, DstWriter or the NTupleSvc are considered
#   outputdata. Files created from the HistogramPersistencySvc are considered
#   Sandboxdata
#
# * The LSF handler has been changed to copy all outputdata to castor.
#   Currently this is CERN centristic because it assumes castor, a more
#   generla scheme needs to be invented.
#
# * Add the application explicitly to the use statement for the fake cmt
#   package. Wenn extracting the the user libs and setting up cmt an explicit
#   "use Gauss|Boole..." statement is added.
#
# * Introduced a 'OutputData' field to the LHCb section of the config file.
#   Although this is not currently used, will allow to solve the CERN
#   centristic solution mentioned above
#
# Revision 1.10  2005/10/17 08:32:16  andrew
# Fixed a typo (discoered by Vladimir) which prevented setting the LD_LIBRARY_PATH
# correctly
#
# Revision 1.9  2005/10/10 15:06:44  andrew
# Dirac.py: removed filter for output data. All files now go to the output
# sandbox
#
# GaudiLSFRunTimeHandler.py: Added output sandbox handling, similar to the way
# the Dirac handler works
#
# Revision 1.8  2005/09/02 13:49:44  andrew
# Changed the way the sandboxinpput is reaed to fit with Dietrichs modifications
#
# Revision 1.7  2005/08/31 07:38:30  andrew
# Made the jobscript executeable in the StandardjobOptions
#
# Revision 1.6  2005/08/30 14:00:32  andrew
# Made the changes corresponding to comply with the new StandardJobConfig
#
# Added a copy of the user shared libs to the job
#
#
#
