###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PDF_LCG.py,v 1.1 2009-05-10 16:42:03 karl Exp $
###############################################################################
# File: PDF_LCG.py
# Author: K. Harrison
# Created: 070126

"""Module containing class dealing with preparation of jobs to run
   PDF application on LCG backend"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "30 November 2007"
__version__ = "1.1"

from PDF_Local import PDF_Local
from PDF_Local import ptime
from Ganga.GPIDev.Lib.File import  File
from Ganga.Lib.LCG import LCGJobConfig
from Ganga.Lib.LCG import LCGRequirements
from Ganga.Utility import logging
from Ganga.Utility.files import fullpath

import os

logger = logging.getLogger()

class PDF_LCG( PDF_Local ):

   def prepare( self, app, appsubconfig, appmasterconfig, jobmasterconfig ):

      local = PDF_Local.prepare\
         ( self, app, appsubconfig, appmasterconfig, jobmasterconfig )
      inbox = []
      inbox.extend( local.inputbox )
      requirements = LCGRequirements()
      tag = "VO-camont-Camtology-%s" % appsubconfig[ "version" ]
      requirements.software = [ tag ]
      lcg = LCGJobConfig( exe = local.exe, inputbox = inbox,\
         outputbox = local.outputbox, requirements = requirements )
      return lcg

   def tail( self, job ):
      lineList, outbox = PDF_Local.tail( self )
     # Remove last lines (completion times) for local jobs
      lineList = lineList[ : -4 ]
     # Remove tarball from outbox, as this is to be uploaded to storage element
      outbox.remove( "media.tar.gz" ) 

      gridUrl = os.path.join( job.outputdata.getGridStorage(), "media.tar.gz" )
      lineList.extend\
         ( [
         "",
         "UPLOAD_START_TIME=\\",
         "%s" % ptime,
         "edg-gridftp-mkdir --parents %s" % os.path.dirname( gridUrl ),
         "globus-url-copy file:${WORKDIR}/media.tar.gz %s" % gridUrl,
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
