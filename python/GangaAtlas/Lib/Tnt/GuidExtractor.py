#!/usr/bin/env python

# GuidExtractor.py - pulls out GUIDs from an event collection 
# and finds corresponding logical filenames.
# Copyright (c) 2006 C.Nicholson, M. Kenyon.  All rights reserved.
# For licence conditions please read LICENCE file.

import xml.dom
import xml.dom.minidom
import getopt
import os
import re
import sys
import commands
import random
import popen2
from Ganga.Utility.logging import getLogger
logger = getLogger()

try:
   import lfc
except:
   logger.debug('Error importing lfc module')
   pass

try:
   from dq2.clientapi.DQ2 import DQ2
   from dq2.common.Config import Config
   from dq2.common.DQException import *
   from dq2.location.client.LocationClient import LocationClient
except ImportError:
   logger.warning("Environment not set [error importing DQ2 dependencies]!")
   logger.warning("Try setting PYTHONPATH to the dq2-client directory.")
   pass

import GenerateCatalogs

actions = {
   'test'   : [0, False],
   'query'   : [0, False]
}

# global maps to be stored
_guidLfnMap = {}
_guidDatasetMap = {}


def chkargs(command, args):
   """Check action and number of arguments
   """
   try:
      # arguments are args minus action
      nargs = len(args)
      # get number of mandatory args and whether there is a limit
      nm = actions[command][0]
      nolimit = actions[command][1]
      # check if number of args is correct
      if nargs < nm or (nargs > nm and not nolimit):
         return False
      if nolimit and actions[command][2] and (len(args)-nm)%2 == 1:
         logger.warning('Error: you must give matching pairs of arguments')
         return False
   except:
      return False

   return True



def usage():
   """
   Usage: will go here   """
   logger.info(usage.__doc__)



def pythonCalled(opts = []):
   
   generateMiniCats = False
   generatePOOLcat = False
   for o in opts:
     if o == 'm':
       generateMiniCats = True
     if o == 'p':
       generatePOOLcat = True
     if o != 'm' and o != 'p':
       #srcRootFile = o
       src_collection_name = o

   #return main(generateMiniCats, generatePOOLcat, srcRootFile)
   return main(generateMiniCats, generatePOOLcat, src_collection_name)



def standAlone(argv):

   generateMiniCats = False
   generatePOOLcat = False
   if len(argv) < 1:
      usage()
      return
   logger.info("length of args = " + str(len(argv)))
   logger.info("ARGS =" + str(argv))

   try:
      opts, args = getopt.getopt(argv[0:], "mp")
   except getopt.GetoptError:
      logger.info("Invalid arguments!")
      usage()
      return

   for o, a in opts:
      if o == '-m':
         generateMiniCats = True
      if o == '-p':
         generatePOOLcat = True

   return main(generateMiniCats, generatePOOLcat)

def getGuidDatasetMap():
   #print _guidDatasetMap
   return _guidDatasetMap


def getGuidLfnMap():
   #print _guidLfnMap
   return _guidLfnMap

def writeCollListFileGUIDXML(src_collection_name):
      #print "########################"
      #print "IN writeCollListFileGUIDXML - again using tag file as source and not myEvent created by CollAppend"
      #print "########################"
      collListfile = open("CollListFileGUID.exe.xml","w")
      collListfile.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>\n")
      collListfile.write("<!DOCTYPE ArgList>\n")
      collListfile.write("<ArgList>\n")
      collListfile.write("<ToolInfo date=\"Thu Nov 20 18:29:03 2008&#xA;\" toolID=\"CollListFileGUID.exe\">\n")
      #collListfile.write("<CliArg name=\"local\" option=\"-xmlInput\">ThisCondition &gt; ThisNumber</CliArg>\n")
      collListfile.write("<CliArg name=\"SrcInfo\" option=\"-src\">"+src_collection_name+" RootCollection"+"</CliArg>\n")
      #collListfile.write("<CliArg name=\"SrcInfo\" option=\"-src\">"+ srcRootFile +" RootCollection"+"</CliArg>\n")
      collListfile.write("</ToolInfo>\n")
      collListfile.write("</ArgList>\n")


#def main(generateMiniCats, generatePOOLcat, srcRootFile):
def main(generateMiniCats, generatePOOLcat, src_collection_name):

   guids = []
   outputLFNs = False
   outputGUIDs = False
   #print "########################"
   #print "IN GUIDEXTRACTOR MAIN"
   #print "########################"
   
   #does CollListFileGUID  exist in the current directory?
   cwd = os.getcwd()
   if not os.path.exists("CollListFileGUID.exe..xml"):
      writeCollListFileGUIDXML(src_collection_name)
   else:
      writeCollListFileGUIDXML(src_collection_name) 

   try:
      dq = DQ2 (
            con_url = Config().getConfig('dq2-content-client').get('dq2-content-client', 'insecure'),
            con_urlsec = Config().getConfig('dq2-content-client').get('dq2-content-client', 'secure'),
                                                                                                                                           
            loc_url = Config().getConfig('dq2-location-client').get('dq2-location-client', 'insecure'),
            loc_urlsec = Config().getConfig('dq2-location-client').get('dq2-location-client', 'secure'),
                                                                                                                                           
            rep_url = Config().getConfig('dq2-repository-client').get('dq2-repository-client', 'insecure'),
            rep_urlsec = Config().getConfig('dq2-repository-client').get('dq2-repository-client', 'secure'),
                                                                                                                                           
            sub_url = Config().getConfig('dq2-subscription-client').get('dq2-subscription-client', 'insecure'),
            sub_urlsec = Config().getConfig('dq2-subscription-client').get('dq2-subscription-client', 'secure'),
      )
         
     
      guid_string = commands.getoutput("CollListFileGUID.exe -xmlInput CollListFileGUID.exe.xml |\
                                   grep -E [[:alnum:]]{8}'-'[[:alnum:]]{4}'-'[[:alnum:]]{4}'-'[[:alnum:]]{4}'-'[[:alnum:]]{12} " \
                                  )
      #print guid_string
      ##########################################################
      if guid_string.find('Exception') > -1:
         logger.warning("Error: incorrect query, or problem with collection utilities")
         return
 
      guids = guid_string.split()
      if len(guids) == 0:
         logger.warning("Error: query returned no files")
         return

      guids = guid_string.split()

      # get file and dataset name by doing GUID lookup in DQ2
      dataset_names = []
      vuids = []
      guidDatasetMap = {}
      guidLfnMap = {}
      files = []
      firstLoop = True

      for guid in guids:
         if guid != "StreamRAW_ref" and guid != "StreamESD_ref" and guid != "Token":
            vuid = dq.contentClient.queryDatasetsWithFileByGUID(guid)
            if len(vuid) == 0:
               logger.warning("Error: guid "+ guid + " returned by query is not registered in any DQ2 dataset!")
               logger.warning("Skipping to next file...")
               continue
            else: 
               #TODO: choose 'best' dataset, not just the first one 
               n=0
               noDataset = 'False'
               try:
                  dataset = dq.repositoryClient.resolveVUID(vuid[n])
                  name = dataset.get('dsn')
               except:
                  if (len(vuid)==1):
                     noDataset = 'True'
                     continue
            # hack to avoid non-official datasets -- FIXME!!!
               match = re.compile('user')
               while (match.search(name) and n < len(vuid)-1):
                  try:
                     dataset = dq.repositoryClient.resolveVUID(vuid[n+1])
                     name = dataset.get('dsn')
                     noDataset = 'False'
                  except:
                     noDataset = 'True'
                     n=n+1
                     continue
                  n=n+1
               if match.search(name):
                  logger.warning("No official dataset found for guid "+guid)
                  noDataset = 'True'
                  continue

               if noDataset == 'True':
                  name = 'NO.DATASET'

               if vuids.count(vuid[n]) == 0:
                  vuids.append(vuid[n])
                  dataset_names.append(name)
               guidDatasetMap[guid] = name
               files = dq.listFilesInDataset(name)
               #print "###########FILES###########"
               #print files
               #print "######################"
               try:
                  lfn = (files[0][guid])['lfn']
                  #print lfn
                  guidLfnMap[guid] = str(lfn)
               except:
                  logger.warning("Could not resolve GUID "+guid+" to any file in dataset "+str(name))
                  guidDatasetMap[guid] = 'NO.DATASET'

         global _guidDatasetMap
         #print "guidDatasetMap:"
         #print _guidDatasetMap
         global _guidLfnMap
         #print "guidLfnMap:"
         #print _guidLfnMap
         _guidDatasetMap = guidDatasetMap
         _guidLfnMap = guidLfnMap
      

      # sort the list of guids by their datasets
      sorted_guids = []
      sorted_lfns = []
      i = 0
      for dataset in dataset_names:
         for item in guidDatasetMap.iteritems():
            if (item[1] == dataset):
               sorted_guids.append(item[0])                  
               sorted_lfns.append(guidLfnMap[sorted_guids[i]])
               i = i + 1

      # find location of existing files
      # information not currently used so comment out.
      """
      sites = []
      site_map = dq.locationClient.queryDatasetLocations(vuids, None)
      for ds in site_map:
         complete = site_map.get(ds).get(1)
         i = 0
         while i < int(len(complete)):
            sites.append(complete.pop())
            i = i + 1
      """

      # generate POOL XML catalogue with required files
      # if specified, also generates a 'mini-catalogue' per file
      # Should be done on-the-fly by Ganga at worker nodes, so comment out
      """ 
      if generatePOOLcat == True:
         pfns = GenerateCatalogs.getPFNsFromLFC(guidLfnMap)
         GenerateCatalogs.generateCatalog(guidLfnMap, pfns, "PoolFileCatalog.xml")
         if generateMiniCats == True:
            # for each guid, generate a POOL file catalogue just with that guid
            for guid in sorted_guids:
               miniGuidLfnMap = {}
               lfn = guidLfnMap[guid]
               miniGuidLfnMap[guid] = lfn
               name = "PoolFileCatalog_"+guid+".xml"
               GenerateCatalogs.generateCatalog(miniGuidLfnMap, pfns, name)
      """
      return guids

   except DQException as e:
      logger.warning( "Error", e)

if __name__ == '__main__':
   standAlone(sys.argv[1:])

