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
from Ganga.Core.exceptions import ApplicationConfigurationError

_subjobs = []
_subCollections = {} 
_guidLfnMap = {}
_guidDatasetMap = {}
_strippedCollection = ''
_tagfileCollection = ''

class TNTJobSplitter(ISplitter):
   
   _name = "TNTJobSplitter"
   _exportmethods = ['tagNavigatorTool','runQuery']
   _schema = Schema(Version(1,0), { 
      'src_collection_name'   : SimpleItem(defvalue='', doc='Source collection name'),
      'src_collection_type'   : SimpleItem(defvalue='RootCollection', doc='Source collection type'),
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

   def writeCollAppendXML(self):
      collAppendfile = open("CollAppend.exe.xml","w")
      collAppendfile.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>\n")
      collAppendfile.write("<!DOCTYPE ArgList>\n")
      collAppendfile.write("<ArgList>\n")
      collAppendfile.write("<ToolInfo date=\"Thu Nov 20 18:29:03 2008&#xA;\" toolID=\"CollAppend.exe\">\n")
      collAppendfile.write("<CliArg name=\"General\" option=\"-ignoreschemadiff\"/>\n")
      collAppendfile.write("<CliArg name=\"General\" option=\"-nevents\">1</CliArg>\n")
      collAppendfile.write("<CliArg name=\"General\" option=\"-nevtcached\">1000000</CliArg>\n")
      collAppendfile.write("<CliArg name=\"General\" option=\"-nevtperprint\">100</CliArg>\n")
      collAppendfile.write("<CliArg name=\"General\" option=\"-noattrib\"/>\n")
      #collAppendfile.write("<CliArg name=\"QueryInfo\" option=\"-queryopt\">RunNumber, EventNumber, StreamAOD_ref</CliArg>\n")
      collAppendfile.write("<CliArg name=\"DstInfo\" option=\"-dst\">"+self.inDir + self.stripped_coll_name + " RootCollection"+"</CliArg>\n")
      collAppendfile.write("<CliArg name=\"DstInfo\" option=\"-dstconnect\">none</CliArg>\n")
      collAppendfile.write("<CliArg name=\"SrcInfo\" option=\"-src\">"+self.src_collection_name+" RootCollection"+"</CliArg>\n")
      collAppendfile.write("</ToolInfo>\n")
      collAppendfile.write("</ArgList>\n")


   def writeCollSplitByGUIDXML(self):
      collSplitfile = open("CollSplitByGUID.exe.xml","w")
      collSplitfile.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>\n")
      collSplitfile.write("<!DOCTYPE ArgList>\n")
      collSplitfile.write("<ArgList>\n")
      collSplitfile.write("<ToolInfo date=\"Thu Nov 20 18:29:03 2008&#xA;\" toolID=\"CollSplitByGUID.exe\">\n")
      collSplitfile.write("<CliArg name=\"General\" option=\"-minevents\">0</CliArg>\n")
      #collSplitfile.write("<CliArg name=\"General\" option=\"-splitref\">StreamESD</CliArg>\n")        
      collSplitfile.write("<CliArg name=\"SrcInfo\" option=\"-src\">"+self.src_collection_name+" RootCollection"+"</CliArg>\n")
      collSplitfile.write("</ToolInfo>\n")
      collSplitfile.write("</ArgList>\n") 
       
   def runQuery(self):
      ### ---------------------  initialisation -------------------------------------------
      jobObj = self.getJobObject()
      self.inDir = jobObj.getInputWorkspace().getPath()
      self.output_collection = 'myEvents.root'   # fix these instead of having them variable - too confusing
      self.stripped_coll_name = re.split('\.root', self.output_collection)[0]
      #print self.stripped_coll_name
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
   
      #does CollAppend.exe..xml exist in the current directory?
      cwd = os.getcwd()
      if not os.path.exists("CollAppend.exe..xml"):
         self.tntPrint("CollAppend.exe.xml not found in current directory ("+cwd+")...creating new file.")
         self.writeCollAppendXML()
      else:
         self.tntPrint("CollAppend.exe.xml found -  overwriting old version " + cwd)      
         self.writeCollAppendXML()

      #does CollAppend.exe..xml exist in the current directory?
      cwd = os.getcwd()
      if not os.path.exists("CollSplitByGUID.exe..xml"):
         self.tntPrint("CollSplitByGUID.exe.xml not found in current directory ("+cwd+")...creating new file.")
         self.writeCollSplitByGUIDXML()
      else:
         self.tntPrint("CollSplitByGUID.exe.xml found -  overwriting old version " + cwd)
         self.writeCollSplitByGUIDXML()      

      #remove existing files 
      #mjk not necessary now because all done in workspace?
      if os.path.exists(self.inDir + "/" + self.output_collection):
         os.remove(self.inDir + "/" + self.output_collection)
   
   
      ### ----------------------------- querying and extracting of GUIDs ----------------------------
      
      #stripped_coll_name = re.split('\.root', self.output_collection)[0]
      
      ### execute query and write events to myEvents.root
      self.tntPrint("Executing query '" + self.query + "' on tag database...")

      #prepare the CollAppend command for passing to the system.
      #collcommand="which CollAppend && CollAppend -src " + self.src_collection_name + " " + \
      #self.src_collection_type + " -srcconnect " + self.src_connection_string + \
      #" -dst " + self.inDir + stripped_coll_name + " RootCollection -queryopt \'RunNumber, EventNumber, StreamAOD_ref\' -query \"" + \
      #self.query + "\""
      collcommand="CollAppend.exe -xmlInput CollAppend.exe.xml"
      #print "########################"
      #print "CALLING CollAppend - but not really needed as not executing query and therefore not making collection - MyEvent"
      #print "########################"

      #pass CollAppend command to the system
      outputhandle, inputhandle = popen2.popen4(collcommand)
      inputhandle.close()
      output = outputhandle.readlines()
      #print output
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
      #print "########################"
      #print "Master collection is"
      #print masterCollection
      #print "########################"
      global _strippedCollection
      _strippedCollection = masterCollection
      global _tagfileCollection
      _tagfileCollection = self.src_collection_name

      # make sure there is nothing already stored in collection list
      global _subCollections
      _subCollections = {}
  
      # TODO: when CollSplitByGUID in POOL release, use that instead of having local executable
      #exe = os.path.join(os.path.dirname(__file__),'CollSplitByGUID')
      #splitCommand=exe + " -src " + self.inDir + masterCollection + " RootCollection -minevents " + self.minevents 
      #print "########################"
      #print "CALLING CollSplitByGUID - Note using input tag file and not myEvent.root created from CollAppend"
      #print "########################" 
      splitCommand="CollSplitByGUID.exe -xmlInput CollSplitByGUID.exe.xml" 
      self.tntPrint(splitCommand)
      outputhandle, inputhandle = popen2.popen4(splitCommand)
      inputhandle.close()
      output = outputhandle.readlines()
      #print output
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
      #GuidExtractor.pythonCalled(['p', collection])
      # NOTE USED TO PASS IN myEVENT above but no CollAppend used to put tag file straight in
      GuidExtractor.pythonCalled(['p', collection])
      global _guidLfnMap 
      _guidLfnMap = GuidExtractor.getGuidLfnMap()
      #print "guidLfnMap"
      #print _guidLfnMap
      #print "guidDatasetMap"
      global _guidDatasetMap 
      _guidDatasetMap = GuidExtractor.getGuidDatasetMap()
      #print _guidDatasetMap
      #move PoolFileCatalogues to Ganga workspace
      #if os.path.exists("PoolFileCatalog.xml"):
      #   shutil.move("PoolFileCatalog.xml",self.inDir)
   
   def _initSubJob(self, masterjob, dataset, lfnList, guidList, subCollection): 
       from Ganga.GPIDev.Lib.Job import Job
       from GangaAtlas.Lib.ATLASDataset import DQ2Dataset

       #print "########################"
       #print "SUBJOB INITIALISED"
       #print "########################"
       
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
       #print "########################"
       #print "DATASET USED"
       #print dataset
       #print "########################"
       subjob.inputdata.names = lfnList
       #print "########################"
       #print "LFNLIST USED"
       #print lfnList
       #print "########################" 
       subjob.inputdata.guids = guidList
       #print "########################"
       #print "GUIDLIST USED"
       #print guidList
       #print "########################"
      
         
       if self.match_ce == True:
            #subjob.inputdata.type = 'TNT_LOCAL'

            # Sort out the possible sites taking into account requirements
            allowed_sites = []
            if subjob.backend.requirements.sites:
               allowed_sites = subjob.backend.requirements.sites
            elif subjob.backend.requirements.cloud:
               allowed_sites = subjob.backend.requirements.list_sites_cloud()
            else:
               raise ApplicationConfigurationError(None,'TntJobSplitter requires a cloud or a site to be set - please use the --cloud option, j.backend.requirements.cloud=CLOUDNAME (T0, IT, ES, FR, UK, DE, NL, TW, CA, US, NG) or j.backend.requirements.sites=SITENAME')

            if subjob.backend.requirements.sites:
               allowed_sites = subjob.backend.requirements.sites

            allowed_sites_all = subjob.backend.requirements.list_sites(True,True)
            # Apply GangaRobot blacklist
            newsites = []
            for site in allowed_sites:
               if site in allowed_sites_all:
                  newsites.append(site)
                  
            allowed_sites = newsites
                    
            # go through and check which sites with the dataset is given by the requirements
            sub_sites = []
            for site in subjob.inputdata.get_locations():
               if site in allowed_sites:
                  sub_sites.append(site)
                  
            if len(sub_sites) == 0:
               raise ApplicationConfigurationError(None,'TntJobSplitter could not find a location for dataset %s in cloud %s. Try another cloud!' % (subjob.inputdata.dataset, subjob.backend.requirements.cloud))
            else:
               subjob.backend.requirements.sites = sub_sites

            #print sub_sites
                        
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
      #print "Called guid extractor with " 
      #print self.inDir + _strippedCollection
      #print _tagfileCollection
      # Want tag file passed in not stripped collection
      #self.collInfo(self.inDir + _strippedCollection)
      self.collInfo(_tagfileCollection)

      self.tntPrint("About to split job")
      #print "########################"
      #print "About to split job"
      #print "########################"
      
      subjobs = []
      for collection in _subCollections:
         guidList = _subCollections[collection]
         #print "########################"
         #print "This is the guidList "
         #print "########################"
         #print guidList 
         finalGuidList = []
         datasetList = []
         lfnList = []
         i = 0
         for guid in guidList:
            #print "########################"
            #print "This is the current guid in the list "
            #print guid
            #print "########################"  
            #print "########################"
            #print "This is the guid dataset map "
            #print _guidDatasetMap
            #print "########################"
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
      #print "########################"
      #print "CALLING self.runQuery()"
      #print "########################"
      self.runQuery()
      #print "########################"
      #print "CALLING self.collsplit()"
      #print "########################"
      self.collSplit()
      #print "########################"
      #print "CALLING self.jobSplit()"
      #print "########################"
      self.jobSplit(job)
      return _subjobs


