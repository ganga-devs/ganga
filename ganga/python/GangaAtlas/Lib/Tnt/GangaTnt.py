#
# Splitter for TNT plugin
#

import sys, os, re
import popen2
import signal
import getopt, string, time 
import shutil

from Ganga.GPIDev.Base import *
from Ganga.GPIDev.Schema import *
from Ganga.Utility.logging import getLogger
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.Lib.Executable import Executable
from Ganga.GPIDev.Lib.Job import *
from Ganga.GPIDev.Lib.File import *
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import *

_subjobs = []
_subCollections = {} 
_guidLfnMap = {}
_guidDatasetMap = {}
_strippedCollection = ''

class TNTJobSplitter(ISplitter):
   
   _name = "TNTJobSplitter"
   _exportmethods = ['tagNavigatorTool','runQuery']
   _schema = Schema(Version(1,0), { 
      'src_collection_name'   : SimpleItem(defvalue='', doc='Source collection name'),
      'src_collection_type'   : SimpleItem(defvalue='RelationalCollection', doc='Source collection type'),
      'src_connection_string' : SimpleItem(defvalue='', doc='Source connection string'),
      'query'                 : SimpleItem(defvalue='', doc='Database query to execute'),
      'logfile'               : SimpleItem(defvalue='TNT.log-'+ str(os.getpid()),doc='Logfile name'),
      'auth_name'             : SimpleItem(defvalue='', doc='Username from authentication.xml'),
      'auth_pass'             : SimpleItem(defvalue='', doc='Password for username from authentication.xml'),
      #'output_collection'     : SimpleItem(defvalue='myEvents.root', doc='Name of query output collection'),
      #'collection': SimpleItem(defvalue='myEvents.root', doc="Collection you want to process"),
      'minevents' : SimpleItem(defvalue='0', doc='Minimum number of events per sub collection'),
      'match_ce'  : SimpleItem(defvalue=False, doc='Run jobs on CEs with local dataset')
      } )

   # method to add date and time to every print statement
   def tntPrint(self, textForPrinting):
         #open the logfile
         writefile = open(self.inDir + "/" + self.logfile,"a")
         writefile.write("[" + time.strftime('%a %b %d %H:%M:%S') + "]  " + textForPrinting +"\n")
         logger.info(textForPrinting)
         writefile.close
  
   def writeAuth(self):
      authfile = open("authentication.xml","w")
      authfile.write("<connectionlist>\n")
      authfile.write("<connection name=\"" + self.src_connection_string + "\">\n")
      authfile.write("<parameter name=\"user\" value=\"" + self.auth_name + "\"/>\n")
      authfile.write("<parameter name=\"password\" value=\"" + self.auth_pass + "\"/>\n")
      authfile.write("</connection>\n")
      authfile.write("</connectionlist>\n")
  
   def runQuery(self):
      ### ---------------------  initialisation -------------------------------------------
      jobObj = self.getJobObject()
      self.inDir = jobObj.getInputWorkspace().getPath()
      self.output_collection = 'myEvents.root'   # fix these instead of having them variable - too confusing
      self.collection = 'myEvents.root'
      self.tntPrint("**** Welcome to TNT! ****")
      self.tntPrint("Using " + str(self.inDir) + " to store intermediary files")
      self.tntPrint(self.logfile)
      #does authentication.xml exist in the current directory?
      cwd = os.getcwd()
      if not os.path.exists("authentication.xml"):
         self.tntPrint("authentication.xml not found in current directory ("+cwd+")...creating file.")
         self.writeAuth()
      else:
         self.tntPrint("authentication.xml found in " + cwd)
   

      #remove existing files 
      #mjk not necessary now because all done in workspace?
      if os.path.exists(self.inDir + "/" + self.output_collection):
         os.remove(self.inDir + "/" + self.output_collection)
   
   
      ### ----------------------------- querying and extracting of GUIDs ----------------------------
      
      stripped_coll_name = re.split('\.root', self.output_collection)[0]
      
      ### execute query and write events to myEvents.root
      self.tntPrint("Executing query '" + self.query + "' on tag database...")

      #prepare the CollAppend command for passing to the system.
      collcommand="CollAppend -src " + self.src_collection_name + " " + \
      self.src_collection_type + " -srcconnect " + self.src_connection_string + \
      " -dst " + self.inDir + stripped_coll_name + " RootCollection -queryopt \'RunNumber, EventNumber, StreamAOD_ref\' -query \"" + \
      self.query + "\""


      #pass CollAppend command to the system
      outputhandle, inputhandle = popen2.popen4(collcommand)
      inputhandle.close()
      output = outputhandle.readlines()
      outputhandle.close()
      for line in output:
         self.tntPrint(line)
         
   def collSplit(self):
      """ splits an event collection into a number of sub-collections, 
          based on GUID boundaries but with a specified number of events per 
          sub-collection """
 
      self.tntPrint("TNTJobSplitter being called now")

      # split the collection into sub-collections
      masterCollection = re.split('\.root', self.collection)[0]
      global _strippedCollection
      _strippedCollection = masterCollection

      # make sure there is nothing already stored in collection list
      global _subCollections
      _subCollections = {}
  
      # TODO: when CollSplitByGUID in POOL release, use that instead of having local executable
      exe = os.path.join(os.path.dirname(__file__),'CollSplitByGUID')
      splitCommand=exe + " -src " + self.inDir + masterCollection + " RootCollection -minevents " + self.minevents 
      self.tntPrint(splitCommand)
      outputhandle, inputhandle = popen2.popen4(splitCommand)
      inputhandle.close()
      output = outputhandle.readlines()
      outputhandle.close()
      for line in output:   
         collName = ""
         guidList = []
         if line.startswith("Created"):
            words = line.split()
            collName = words[3]
            self.tntPrint("Moving " + collName + ".root to " + self.inDir)
            #move sub-collections to Ganga workspace
            shutil.move(collName + ".root",self.inDir)
            for word in words[6:]:
               guidList.append(word)
            # collect mapping of sub-collections to the guids they contain
            _subCollections[self.inDir + collName] = guidList   


   def collInfo(self, collection):
      """ gets GUID, LFN and dataset info about given collection, using GuidExtractor """
      from GangaAtlas.Lib.Tnt import GuidExtractor
      self.tntPrint("About to call GuidExtractor")
      GuidExtractor.pythonCalled(['p', collection])
      global _guidLfnMap 
      _guidLfnMap = GuidExtractor.getGuidLfnMap()
      #print _guidLfnMap
      global _guidDatasetMap 
      _guidDatasetMap = GuidExtractor.getGuidDatasetMap()
      #print _guidDatasetMap
      #move PoolFileCatalogues to Ganga workspace
      #if os.path.exists("PoolFileCatalog.xml"):
      #   shutil.move("PoolFileCatalog.xml",self.inDir)
   
   def _initSubJob(self, masterjob, dataset, lfnList, guidList, subCollection): 
       from Ganga.GPIDev.Lib.Job import Job
       from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
       
       subjob = Job()
       subjob.inputsandbox = masterjob.inputsandbox
       subjob.application = masterjob.application
       subjob.outputdata = masterjob.outputdata
       subjob.outputsandbox = masterjob.outputsandbox
       subjob.backend = masterjob.backend

       # attributes which are different for each sub-job
       subjob.inputdata = DQ2Dataset()   
       #subjob.inputdata.datatype = 'DATA'
       subjob.inputdata.dataset = dataset
       subjob.inputdata.names = lfnList
       subjob.inputdata.guids = guidList
         
       if self.match_ce == True:
           subjob.inputdata.type = 'TNT_LOCAL'
           ces = getLocationsCE(subjob.inputdata.get_locations())
           #self.tntPrint("CEs are : " + str(ces))
           if str(ces) != '[]':
               subjob.backend.requirements.other += ['( %s )' % ' || '.join([ 'RegExp("%s",other.GlueCEInfoHostName)' % ce for ce in ces])]
           else:
               subjob.inputdata.type = 'TNT_DOWNLOAD'
       else:
           subjob.inputdata.type = 'TNT_DOWNLOAD'   #requires PFNs in sfn:// format

       subjob.inputsandbox += [ File(os.path.join(subCollection+".root")) ]
       return subjob

   def jobSplit(self, job):
      """ generates 1 sub-job per sub-collection """
      from Ganga.GPIDev.Lib.Job import Job
      from GangaAtlas.Lib.ATLASDataset import DQ2Dataset

      # make sure correct collection info is present
      self.tntPrint("Getting collection info...")
      self.tntPrint("Called guid extractor with " + str(self.inDir + _strippedCollection))
      self.collInfo(self.inDir + _strippedCollection)

      self.tntPrint("About to split job")
      
      subjobs = []
      for collection in _subCollections:
         guidList = _subCollections[collection]
         finalGuidList = []
         datasetList = []
         lfnList = []
         i = 0
         for guid in guidList:
            dataset = _guidDatasetMap[guid]
            if dataset != 'NO.DATASET':
               if datasetList.count(dataset) == 0:
                  datasetList.append(dataset)
                  i = i + 1
               finalGuidList.append(guid)
               lfn = _guidLfnMap[guid]
               lfnList.append(lfn)
         if i > 1:
            self.tntPrint("More than one dataset has been identified for a sub-job,")
            self.tntPrint("so sub-job has been copied into " + str(i) + " sub-jobs.")
            self.tntPrint("Correct behaviour of your analysis is not guaranteed in this case.")
            for dataset in datasetList:
                sj = self._initSubJob(job, dataset, lfnList, finalGuidList, collection)                        
                subjobs.append(sj)
         else:
             if dataset != 'NO.DATASET':
                sj = self._initSubJob(job, dataset, lfnList, finalGuidList, collection)           
                subjobs.append(sj)
      
      global _subjobs
      _subjobs = subjobs


   def split(self,job):
      self.runQuery()
      self.collSplit()
      self.jobSplit(job)
      return _subjobs


