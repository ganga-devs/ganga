###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PDF_Local.py,v 1.1 2009-05-10 16:42:03 karl Exp $
###############################################################################
# File: PDF_Local.py
# Author: K. Harrison
# Created: 070123

"""Module containing class dealing with preparation of jobs to run
   application for PDF analysis on Local backend"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "10 May 2009"
__version__ = "1.0"

from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.GPIDev.Lib.File import  File
from Ganga.GPIDev.Lib.File import  FileBuffer
from Ganga.Utility import logging
from Ganga.Utility.files import fullpath

import os
import time

logger = logging.getLogger()
ptime = \
   "\"$(echo \"$(python -c \"import time; print '%.6f' % time.time()\")\")\""

class PDF_Local( IRuntimeHandler ):

   def prepare( self, app, appsubconfig, appmasterconfig, jobmasterconfig ):

      job = app.getJobObject()
      version = appsubconfig[ "version" ]
      outDir = appsubconfig[ "outDir" ]
      docDir = appsubconfig[ "docDir" ]
      elementList = docDir.split( os.sep )
      docList = appsubconfig[ "docList" ]
      softwareDir = appsubconfig[ "softwareDir" ]

      lineList = []
      inbox = []
      outbox = []

      headList, headBox = self.head( job = job, version = version, \
         softwareDir = softwareDir )
      lineList.extend( headList )
      outbox.extend( headBox )

      bodyList, bodyBox = self.body( job = job, docDir = docDir, \
         docList = docList )
      lineList.extend( bodyList )
      inbox.extend( bodyBox )

      tailList, tailBox = self.tail( job = job )
      lineList.extend( tailList )
      outbox.extend( tailBox )

      jobScript = "\n".join( lineList )
      jobWrapper = FileBuffer( "PDF.sh", jobScript, executable = 1 )

      outbox.extend( job.outputsandbox )

      return StandardJobConfig\
         ( exe = jobWrapper, inputbox = inbox, outputbox = outbox )

   def head( self, job = None, version = "", softwareDir = "" ):
      outbox = []
      lineList = \
         [
            "#!/bin/bash",
            "",
            "# Run script for PDF analysis",
            "# Created by Ganga - %s" % ( time.strftime( "%c" ) ),
            "",
            "JOB_START_TIME=\\",
            "%s" % ptime,
            "HOSTNAME=$(hostname -f)",
            "echo \"Job running on ${HOSTNAME}\"",
            "echo \"Processor architecture: $(arch)\"",
            "echo \"Start time: $(date)\"",
            "",
            "WORKDIR=$(pwd)",
            "export CAMTOLOGY_VERSION=\"%s\"" % version,
            "if [ -z ${VO_CAMONT_SW_DIR} ]; then",
            "   VO_CAMONT_SW_DIR=\"%s\"" % softwareDir,
            "fi",
         ]
      return ( lineList, outbox )

   def body( self, job = None, docDir = "", docList = [] ):

      lineList = []

      lineList.extend\
         ( [
         "DOWNLOAD_START_TIME=\\",
         "%s" % ptime,
         "N_DOWNLOAD_SUCCESS=0",
         "N_DOWNLOAD_FAILURE=0",
         ])

      xList, inbox, removeList  = self.bodyUrlList( docList = docList )   
      lineList.extend( xList )

      lineList.extend\
         ( [
         "DOWNLOAD_END_TIME=\\",
         "%s" % ptime,
         "DOWNLOAD_SIZE=$(du -b -s ${WORKDIR}/media | cut -f 1)",
         ])

      lineList.extend\
         ( [
         "cd ${CLASSIFIER_HOME}",
         "COMMAND=\"${VO_CAMONT_SW_DIR}/Camtoloty/version-${CAMTOLOGY_VERSION}"\
            +"/src/bin/pdf2fbSxml-inputDir.sh ${WORKDIR}/media\"",
         "COMMAND=\"./pdf2fbSxml-inputDir.sh ${WORKDIR}/media\"",
         "echo \"Performing PDF analysis with command:\"",
         "echo \"   ${COMMAND}\"",
         "EXECUTION_START_TIME=\\",
         "%s" % ptime,
         "${COMMAND}",
         "EXECUTION_END_TIME=\\",
         "%s" % ptime,
         ] )

      lineList.extend( removeList )

      return ( lineList, inbox )

   def bodyUrlList( self, docList = [] ):

      lineList = []
      removeList = []
      inbox = []

      for docUrl, relativePath in docList:

         if relativePath:
            docFile = os.path.basename( relativePath )
            docSubdir = os.path.join\
               ( "media", os.path.dirname( relativePath ) )
            docRemoteDir = os.path.join( "${WORKDIR}", docSubdir )
            docRemotePath = os.path.join( docRemoteDir, docFile )
            removeList.append( "rm %s" % docRemotePath )
            lineList.extend\
               ( [
               "mkdir -p %s" % docRemoteDir,
               "wget -q -P %s '%s'" % ( docRemoteDir, docUrl ),
               "if [ -f %s ]; then" % docRemotePath,
               "   N_DOWNLOAD_SUCCESS=$((${N_DOWNLOAD_SUCCESS}+1))",
               "else",
               "   N_DOWNLOAD_FAILURE=$((${N_DOWNLOAD_FAILURE}+1))",
               "fi",
#              "sleep 30s",
               ] )
         else:
            docFile = os.path.basename( docUrl )
            docSuffix = os.path.splitext( docFile )[ 1 ]  
            lineList.extend\
               ( [
               "wget -q %s -O %s" % ( docUrl, docFile ),
               "if [ -f %s ]; then" % docFile,
               "   DOC_MD5SUM=$(md5sum %s)" % docFile,
               "   DOC_MD5NAME=$(echo ${DOC_MD5SUM} | cut -d ' ' -f 1)",
               "   SUBDIR1=$(echo ${DOC_MD5NAME} | cut -c 1-3)",
               "   SUBDIR2=$(echo ${DOC_MD5NAME} | cut -c 4-6)",
               "   N_DOWNLOAD_SUCCESS=$((${N_DOWNLOAD_SUCCESS}+1))",
               ] )
            docSubdir = os.path.join( "media", "${SUBDIR1}", "${SUBDIR2}" )
            docRemoteDir = os.path.join( "${WORKDIR}", docSubdir )
            docRemotePath = os.path.join\
               ( docRemoteDir, "${DOC_MD5NAME}" + docSuffix )
            lineList.extend\
               ( [
               "   mkdir -p %s" % docRemoteDir,
               "   mv -f %s %s" % ( docFile, docRemotePath ),
               "else",
               "   N_DOWNLOAD_FAILURE=$((${N_DOWNLOAD_FAILURE}+1))",
               "fi",
#              "sleep 30s",
               ] )

      return ( lineList, inbox, removeList )

   def tail( self, job = None ):
      lineList = \
         [
         "",
         "RESULTS_SIZE=$(du -b -s ${WORKDIR}/media | cut -f 1)",
         "cd ${WORKDIR}/media",
         "TAR_START_TIME=\\",
         "%s" % ptime,
         "tar -zcf ${WORKDIR}/media.tar.gz *",
         "TAR_END_TIME=\\",
         "%s" % ptime,
         "TARBALL_SIZE=$(du -b -s ${WORKDIR}/media.tar.gz | cut -f 1)",
         "",
         "RUN_DATA=${WORKDIR}/execute.dat",
         "rm -rf ${RUN_DATA}",
         "touch ${RUN_DATA}",
         "echo \"Hostname: ${HOSTNAME}\" >> ${RUN_DATA}",
         "echo \"Job_start: ${JOB_START_TIME}\" >> ${RUN_DATA}",
         "echo \"Download_start: ${DOWNLOAD_START_TIME}\" >> ${RUN_DATA}",
         "echo \"Download_end: ${DOWNLOAD_END_TIME}\" >> ${RUN_DATA}",
         "echo \"Download_successes: ${N_DOWNLOAD_SUCCESS}\" >> ${RUN_DATA}",
         "echo \"Download_failures: ${N_DOWNLOAD_FAILURE}\" >> ${RUN_DATA}",
         "echo \"Download_size: ${DOWNLOAD_SIZE}\" >> ${RUN_DATA}",
         "echo \"Execution_start: ${EXECUTION_START_TIME}\" >> ${RUN_DATA}",
         "echo \"Execution_end: ${EXECUTION_END_TIME}\" >> ${RUN_DATA}",
         "echo \"Results_size: ${RESULTS_SIZE}\" >> ${RUN_DATA}",
         "echo \"Tar_start: ${TAR_START_TIME}\" >> ${RUN_DATA}",
         "echo \"Tar_end: ${TAR_END_TIME}\" >> ${RUN_DATA}",
         "echo \"Tarball_size: ${TARBALL_SIZE}\" >> ${RUN_DATA}",
         "JOB_END_TIME=\\",
         "%s" % ptime,
         "echo \"Job_end: ${JOB_END_TIME}\" >> ${RUN_DATA}",
         "echo \"End time: $(date)\"",
         ]
      outbox = [ "media.tar.gz", "execute.dat" ]
      return ( lineList, outbox )
