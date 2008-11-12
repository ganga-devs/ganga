###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ClassifyLocal.py,v 1.3 2008-11-12 11:56:43 karl Exp $
###############################################################################
# File: ClassifyLocal.py
# Author: K. Harrison
# Created: 070123

"""Module containing class dealing with preparation of jobs to run
   image-classification application on Local backend"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "7 November 2008"
__version__ = "1.3"

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

class ClassifyLocal( IRuntimeHandler ):

   def prepare( self, app, appsubconfig, appmasterconfig, jobmasterconfig ):

      job = app.getJobObject()
      version = appsubconfig[ "version" ]
      libList = appsubconfig[ "libList" ]
      classifierDir = appsubconfig[ "classifierDir" ]
      outDir = appsubconfig[ "outDir" ]
      imageDir = appsubconfig[ "imageDir" ]
      elementList = imageDir.split( os.sep )
      imageList = appsubconfig[ "imageList" ]
      tagFile = appsubconfig[ "tagFile" ]

      lineList = []
      inbox = []
      outbox = []

      headList, headBox = self.head( job = job, version = version, \
         libList = libList, classifierDir = classifierDir )
      lineList.extend( headList )
      outbox.extend( headBox )

      bodyList, bodyBox = self.body( job = job, imageDir = imageDir, \
         imageList = imageList, tagFile = tagFile )
      lineList.extend( bodyList )
      inbox.extend( bodyBox )

      tailList, tailBox = self.tail( job = job )
      lineList.extend( tailList )
      outbox.extend( tailBox )

      jobScript = "\n".join( lineList )
      jobWrapper = FileBuffer( "Classify.sh", jobScript, executable = 1 )

      outbox.extend( job.outputsandbox )

      return StandardJobConfig\
         ( exe = jobWrapper, inputbox = inbox, outputbox = outbox )

   def head( self, job = None, version = "", libList = [], classifierDir = "" ):
      outbox = []
      lineList = \
         [
            "#!/bin/bash",
            "",
            "# Run script for image-classification application",
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
            "if [ -z ${PATH} ]; then",
            "   export PATH=\"\"",
            "fi",
            "if [ -z ${VO_CAMONT_SW_DIR} ]; then",
            "   VO_CAMONT_SW_DIR=\"\"",
            "else",
            "   VO_CAMONT_SW_DIR=${VO_CAMONT_SW_DIR}/%s" % version,
            "fi",
            "JAVA_INSTALLATION=${VO_CAMONT_SW_DIR}/java/$(arch)",
            "if [ -d ${JAVA_INSTALLATION} ]; then",
            "   JAVA_VERSION=`ls ${JAVA_INSTALLATION}`",
            "   JAVA_HOME=${JAVA_INSTALLATION}/${JAVA_VERSION}",
            "   JAVA=${JAVA_HOME}",
            "   JDK=${JAVA_HOME}/bin",
            "   PATH=${JDK}:${PATH}",
            "fi",
            "JAVALIB=\"\"",
         ]

      pathsep = ""
      for libPath in libList:
         libName = os.path.basename( libPath )
         lineList.extend\
            ( [
               "if [ -f %s ]; then" % libPath,
               "   JAVALIB=${JAVALIB}%s%s" % ( pathsep, libPath ),
               "elif [ -f ${WORKDIR}/%s ]; then" % libName,
               "   JAVALIB=${JAVALIB}%s${WORKDIR}/%s" % ( pathsep, libName ),
               # used to be ${VO_CAMONT_SW_DIR}/java/%s
               "elif [ -f ${VO_CAMONT_SW_DIR}/%s ]; then" % libName,
               "   JAVALIB=${JAVALIB}%s${VO_CAMONT_SW_DIR}/%s" \
                  % ( pathsep, libName ),
               "else",
               "   echo \"Unable to locate library \'%s\'\"" % libName,
               "   echo \"Searched in:\"",
               "   echo \"  %s\"" % os.path.dirname( libPath ),
               "   echo \"  ${WORKDIR}\"",
               "   echo \"  ${VO_CAMONT_SW_DIR}\"",
               "   exit",
               "fi",
            ] )
         if not pathsep:
            pathsep = os.pathsep

      lineList.extend\
         ( [
            "CLASSIFIER_HOME=\"\"",
            "if [ -d %s ]; then" % classifierDir,
            "   CLASSIFIER_HOME=%s" % classifierDir,
            "   IMENSE_LIB_DIR=%s/lib" % classifierDir,
            # used to be ${VO_CAMONT_SW_DIR}/classifier
            "elif [ -d ${VO_CAMONT_SW_DIR} ]; then",
            "   CLASSIFIER_HOME=${VO_CAMONT_SW_DIR}",
            "   IMENSE_LIB_DIR=${VO_CAMONT_SW_DIR}/lib",
            "else",
            "   echo \"Unable to locate classifier directory\"",
            "   exit",
            "fi",
            "if [ -z ${LD_LIBRARY_PATH} ]; then",
            "   export LD_LIBRARY_PATH=\"\"",
            "fi",
            "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${WORKDIR}",
            "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${IMENSE_LIB_DIR}"
            "",
         ] )
      return ( lineList, outbox )

   def body( self, job = None, imageDir = "", imageList = [], tagFile = "" ):

      lineList = []

      lineList.extend\
         ( [
         "DOWNLOAD_START_TIME=\\",
         "%s" % ptime,
         "N_DOWNLOAD_SUCCESS=0",
         "N_DOWNLOAD_FAILURE=0",
         ])

      if tagFile:
         xList, inbox, removeList = self.bodyTagFile( tagFile = tagFile )
      else:
         xList, inbox, removeList  = self.bodyUrlList( imageList = imageList )   
      lineList.extend( xList )

      lineList.extend\
         ( [
         "DOWNLOAD_END_TIME=\\",
         "%s" % ptime,
         "DOWNLOAD_SIZE=$(du -b -s ${WORKDIR}/images | cut -f 1)",
         ])

      lineList.extend\
         ( [
         "cd ${CLASSIFIER_HOME}",
         "COMMAND=\"$(which java) -Xmx900m -cp ${JAVALIB} "\
            +"imenseLib.ProcessImages -c ${WORKDIR}/images\"",
         "echo \"Performing image classification with command:\"",
         "echo \"   ${COMMAND}\"",
         "EXECUTION_START_TIME=\\",
         "%s" % ptime,
         "${COMMAND}",
         "EXECUTION_END_TIME=\\",
         "%s" % ptime,
         ] )

      lineList.extend( removeList )

      return ( lineList, inbox )

   def bodyTagFile( self, tagFile = "" ):

      lineList = []
      removeList = []
      inbox = [ File( name = tagFile ) ]

      lineList.extend\
         ( [
         "JOB_TAGFILE=\"%s\"" % os.path.basename( tagFile ),
         "TAGFILE_DIR=\"${WORKDIR}/tag_data\"",
         "rm -rf ${TAGFILE_DIR}",
         "mkdir -p ${TAGFILE_DIR}",
         "split -l 6 -a 7 ${JOB_TAGFILE} ${TAGFILE_DIR}/tagfile_",  
         "TAGFILE_LIST=$(ls ${TAGFILE_DIR})",
         "",
         "for TAGFILE in ${TAGFILE_LIST}; do",
         "  TAGFILE_PATH=${TAGFILE_DIR}/${TAGFILE}",
         "  IMAGE_URL=\"\"",
         "  while read TAG_LINE; do",
         "    TAG=${TAG_LINE%%:*}",
         "    if [ \"${TAG}\" == \"URLImage\" ]; then",
         "      IMAGE_URL=${TAG_LINE#\"URLImage: \"}",
         "      IMAGE_FILE=${IMAGE_URL##*/}",
         "      IMAGE_SUFFIX=${IMAGE_FILE##*\\.}",
         "      break",
         "    fi",
         "  done < ${TAGFILE_PATH}",
         "",
         "  if [ \"${IMAGE_URL}\" != \"\" ]; then"
         "    wget -q ${IMAGE_URL} -O ${IMAGE_FILE}",
         "    if [ -f ${IMAGE_FILE} ]; then",
         "      N_DOWNLOAD_SUCCESS=$((${N_DOWNLOAD_SUCCESS}+1))",
         "      IMAGE_MD5SUM=$(md5sum ${IMAGE_FILE})",
         "      IMAGE_MD5NAME=$(echo ${IMAGE_MD5SUM} | cut -d ' ' -f 1)",
         "      SUBDIR1=$(echo ${IMAGE_MD5NAME} | cut -c 1-3)",
         "      SUBDIR2=$(echo ${IMAGE_MD5NAME} | cut -c 4-6)",
         "      IMAGE_REMOTE_DIR=\"${WORKDIR}/images/${SUBDIR1}/${SUBDIR2}\"",
         "      IMAGE_PATH_NO_SUFFIX=\"${IMAGE_REMOTE_DIR}/${IMAGE_MD5NAME}\"",
         "      IMAGE_REMOTE_PATH=\"${IMAGE_PATH_NO_SUFFIX}.${IMAGE_SUFFIX}\"",
         "      TAGFILE_REMOTE_PATH=\"${IMAGE_PATH_NO_SUFFIX}.txt\"",
         "      mkdir -p ${IMAGE_REMOTE_DIR}/vseg",
         "      mv -f ${IMAGE_FILE} ${IMAGE_REMOTE_PATH}",
         "      mv -f ${TAGFILE_PATH} ${TAGFILE_REMOTE_PATH}",
         "    else",
         "     N_DOWNLOAD_FAILURE=$((${N_DOWNLOAD_FAILURE}+1))",
         "    fi",
         "  fi",
         "done",
         ""
         ])

      return ( lineList, inbox, removeList )

   def bodyUrlList( self, imageList = [] ):

      lineList = []
      removeList = []
      inbox = []

      for imageUrl, relativePath in imageList:

         if relativePath:
            imageFile = os.path.basename( relativePath )
            imageSubdir = os.path.join\
               ( "images", os.path.dirname( relativePath ) )
            imageRemoteDir = os.path.join( "${WORKDIR}", imageSubdir )
            imageRemotePath = os.path.join( imageRemoteDir, imageFile )
            vsegDir = os.path.join( imageRemoteDir, "vseg" )
            removeList.append( "rm %s" % imageRemotePath )
            lineList.extend\
               ( [
               "mkdir -p %s" % vsegDir,
               "wget -q -P %s '%s'" % ( imageRemoteDir, imageUrl ),
               "if [ -f %s ]; then" % imageRemotePath,
               "   N_DOWNLOAD_SUCCESS=$((${N_DOWNLOAD_SUCCESS}+1))",
               "else",
               "   N_DOWNLOAD_FAILURE=$((${N_DOWNLOAD_FAILURE}+1))",
               "fi",
#              "sleep 30s",
               ] )
         else:
            imageFile = os.path.basename( imageUrl )
            imageSuffix = os.path.splitext( imageFile )[ 1 ]  
            lineList.extend\
               ( [
               "wget -q %s -O %s" % ( imageUrl, imageFile ),
               "if [ -f %s ]; then" % imageFile,
               "   IMAGE_MD5SUM=$(md5sum %s)" % imageFile,
               "   IMAGE_MD5NAME=$(echo ${IMAGE_MD5SUM} | cut -d ' ' -f 1)",
               "   SUBDIR1=$(echo ${IMAGE_MD5NAME} | cut -c 1-3)",
               "   SUBDIR2=$(echo ${IMAGE_MD5NAME} | cut -c 4-6)",
               "   N_DOWNLOAD_SUCCESS=$((${N_DOWNLOAD_SUCCESS}+1))",
               ] )
            imageSubdir = os.path.join( "images", "${SUBDIR1}", "${SUBDIR2}" )
            imageRemoteDir = os.path.join( "${WORKDIR}", imageSubdir )
            imageRemotePath = os.path.join\
               ( imageRemoteDir, "${IMAGE_MD5NAME}" + imageSuffix )
            vsegDir = os.path.join( imageRemoteDir, "vseg" )
            lineList.extend\
               ( [
               "   mkdir -p %s" % vsegDir,
               "   mv -f %s %s" % ( imageFile, imageRemotePath ),
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
         "RESULTS_SIZE=$(du -b -s ${WORKDIR}/images | cut -f 1)",
         "cd ${WORKDIR}/images",
         "TAR_START_TIME=\\",
         "%s" % ptime,
         "tar -zcf ${WORKDIR}/images.tar.gz *",
         "TAR_END_TIME=\\",
         "%s" % ptime,
         "TARBALL_SIZE=$(du -b -s ${WORKDIR}/images.tar.gz | cut -f 1)",
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
      outbox = [ "images.tar.gz", "execute.dat" ]
      return ( lineList, outbox )
