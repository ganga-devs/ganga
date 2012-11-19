###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ClassifyLCG.py,v 1.1 2008-10-04 17:42:38 karl Exp $
###############################################################################
# File: ClassifyLCG.py
# Author: K. Harrison
# Created: 070126

"""Module containing class dealing with preparation of jobs to run
   Classify application on LCG backend"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "30 November 2007"
__version__ = "1.1"

from ClassifyLocal import ClassifyLocal
from ClassifyLocal import ptime
from Ganga.GPIDev.Lib.File import  File
from Ganga.Lib.LCG import LCGJobConfig
from Ganga.Lib.LCG import LCGRequirements
from Ganga.Utility import logging
from Ganga.Utility.files import fullpath

import os

logger = logging.getLogger()

class ClassifyLCG( ClassifyLocal ):

   def prepare( self, app, appsubconfig, appmasterconfig, jobmasterconfig ):

      local = ClassifyLocal.prepare\
         ( self, app, appsubconfig, appmasterconfig, jobmasterconfig )
      inbox = []
      inbox.extend( local.inputbox )
      requirements = LCGRequirements()
      tag = "VO-camont-%s" % appsubconfig[ "version" ]
      requirements.software = [ tag ]
      lcg = LCGJobConfig( exe = local.exe, inputbox = inbox,\
         outputbox = local.outputbox, requirements = requirements )
      return lcg

   def tail( self, job ):
      lineList, outbox = ClassifyLocal.tail( self )
     # Remove last lines (completion times) for local jobs
      lineList = lineList[ : -4 ]
     # Remove tarball from outbox, as this is to be uploaded to storage element
      outbox.remove( "images.tar.gz" ) 

      gridUrl = os.path.join( job.outputdata.getGridStorage(), "images.tar.gz" )
      lineList.extend\
         ( [
         "",
         "UPLOAD_START_TIME=\\",
         "%s" % ptime,
         "edg-gridftp-mkdir --parents %s" % os.path.dirname( gridUrl ),
         "globus-url-copy file:${WORKDIR}/images.tar.gz %s" % gridUrl,
         "UPLOAD_END_TIME=\\",
         "%s" % ptime,
         "",
         "echo \"Upload_start: ${UPLOAD_START_TIME}\" >> ${RUN_DATA}",
         "echo \"Upload_end: ${UPLOAD_END_TIME}\" >> ${RUN_DATA}",
         "JOB_END_TIME=\\",
         "%s" % ptime,
         "echo \"Job_end: ${JOB_END_TIME}\" >> ${RUN_DATA}",
         "echo \"End time: $(date)\"",
         ] )
      return ( lineList, outbox )
