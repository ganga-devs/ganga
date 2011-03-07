########################################################################
# File : AnalysisTask.py
########################################################################

""" LHCb Analysis Task

    The main motivations for data oriented processing using Tasks is the following:
    
    - users have access to experiment specific tools that provide a list of LFNs they are interested in;
    - accommodating changes to the dataset by design (data being added and removed) is currently not easy;
    - users want to 
    
    Therefore users would define a bookkeeping query and configured job template and the system would define 
    and submit jobs for subsets of the files. 
    
    Over time the query will be manually flushed meaning either: 
    
    - new data is added and jobs are automatically created and submitted accordingly;
    - already processed data might have been declared bad meaning this should be conveyed to the user.
    
    The monitoring loop of the task should discover which data has not yet been processed implying a simple 
    status machine for files like:
    
    - "New" initial status for files;
    - "Assigned" for conveying that a job has been submitted;
    - "Processed" for when the job is completed;
    - "Failed" transient status to trigger resubmission of a job
    
    In order to automatically arrive at 100% of the data being processed the system should check "Assigned" 
    and "Failed" files with a configurable timeout and number of retries. 
    
    A simple example would be: 

      # Define task
      template = JobTemplate(....)
      t = AnalysisTask()
      t.template = template
      t.query = BKquery('/foo/bar')
      t.setTemplate(template)
      
      # Update dataset to make it start processing
      t.updateQuery()
      
      # See the process
      t.status()
      
      # Update query to get new data
      t.updateQuery()

"""

from Ganga.GPIDev.Lib.Tasks.common import *

from AnalysisTransform import AnalysisTransform

import string

# First let's make the help string print nicely with all necessary feature information

help = []
adj = 32
sep = ' : '
command=fgcol("blue")
header=fgcol("red")

help.append(markup('\n<====  LHCb Analysis Task For Data Oriented Processing ====>\n',command))
help.append(markup('Procedure for normal analysis'+sep,header))
help.append('LHCb Analysis Task'.ljust(adj)+sep+markup('t = AnalysisTask()',command))
help.append('Analysis Job Template'.ljust(adj)+sep+markup('template = JobTemplate()',command))
help.append('Add Template to Task'.ljust(adj)+sep+markup('t.setTemplate(template)',command))
help.append('TODO: BK query'.ljust(adj)+sep+markup('t.query = "my.bkquery"',command))
help.append(markup('\nOther useful commands'+sep,header))
help.append('TODO: Flush dataset (refresh BK query)'.ljust(adj)+sep+markup('t.updateQuery()',command))
help.append('TODO: Monitor progress'.ljust(adj)+sep+markup('t.status()',command))
help.append('TODO: Reset file status'.ljust(adj)+sep+markup('t.forceStatus(lfn)',command))
help.append(markup('\nAnalysis Task Properties'+sep,header))
#TODO
help.append('Task input data'.ljust(adj)+sep+'t.inputdata CHECK')

help = string.join(help,'\n')+'\n'
help_nocolor = help.replace(fgcol("blue"),"").replace(fx.normal, "").replace(fgcol("red"),"")

from Ganga.GPIDev.Lib.Tasks import Task
from Ganga.Core.exceptions import ApplicationConfigurationError

from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *

#atlas specific
#from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname 

class AnalysisTask(Task):
    __doc__ = help_nocolor
    _schema = Schema(Version(1,1), dict(Task._schema.datadict.items() + {
         'template' : SimpleItem(defvalue=None, transient=1, typelist=["object"], doc='Job template'),
#         'analysis': SimpleItem(defvalue=None, transient=1, typelist=["object"], doc='Analysis Transform'),
         'container_name': SimpleItem(defvalue="",protected=True,transient=1, getter="get_container_name", doc='name of the output container'),
        }.items()))
    _category = 'tasks'
    _name = 'AnalysisTask'
    _exportmethods = Task._exportmethods + ["setTemplate","query","updateQuery","setDataset"]
    #["initializeFromDatasets",
    
    def initialize(self):
        """Initialize the class. 
        """
        super(AnalysisTask, self).initialize()
        transform = AnalysisTransform()
        transform.name = "LHCbAnalysisTask"
        transform.inputdata = LHCbDataset()
        self.transforms = [transform]
        self.setBackend(None)
    
    #try to keep simple initially
    def setTemplate(self,jobTemplate):
        """ LHCb specific method to pass the template for a transformation step.
        """
        #try to pass application from job template
        app = jobTemplate.application
        self.transforms[0].application=app 
        self.setBackend(jobTemplate.backend)
        #must think about input / output data / sandboxes etc. as necessary
        inputSandbox=jobTemplate.inputsandbox
        self.transforms[0].inputsandbox=inputSandbox
        outputSandbox=jobTemplate.outputsandbox
        self.transforms[0].outputsandbox=outputSandbox
        outputData=jobTemplate.outputdata
        self.transforms[0].outputdata=outputData
            
    def query(self,bkQuery):
        """ Allows an LHCb BK query object to define the dataset.
        """
        print 'TODO: simple wrapping to get an LHCbDataset from BKQuery object.'      
        return 1
    
    def setDataset(self,datasetList):
        """ Instead of using a BK query can provide an LHCbDataset for input 
            data files directly. Dataset can be individual dataset or a 
            list of datasets.
            
             For each dataset in the dataset_list a transform is created. 
             
            TODO: Must think about output file names.
        """
        self.initAliases()
        if not type(datasetList) is list:
            logger.debug('Assuming setDataset() argument is a single dataset object.')
            datasetList = [datasetList]
        
        transform = None
        if self.transforms:
            transform = self.transforms[0]
            #Do this to reset the existing data i.e. setDataset is a one off operation  
            transform.inputdata = LHCbDataset()    
                
        #GPIDev/Base/Proxy method stripProxy was called here in the ATLAS case, not
        #sure if this is necessary.
        finalDatasets = []
        for dataset in datasetList:
            if not len(dataset.files):
                logger.warning('Ignoring empty dataset specified to setDataset() method.')
                continue
              
            #probably need more protection in here eventually e.g. check LFNs if backend DIRAC etc. 
            finalDatasets.append(dataset)

        #is there an issue with configuring the application? 

        transformsList = []
        order = 0
        for processable in finalDatasets:
            #Name the transforms via the order (also encode number of files)
            order+=1
            newTransform = transform.clone()
            newTransform.name = '%s_Files%s_Dataset%s' %(self.name,processable.__len__(),order)
            newTransform.inputdata.extend(processable)
            transformsList.append(newTransform)
            
        self.transforms = transformsList
        self.initAliases()
    
    def updateQuery(self):
        """ If the AnalysisTask dataset is defined via a BK query object this 
            method allows to retrieve the latest files from the BK as well as
            managing deprecated, lost or missing files. 
        """
        print 'TODO: update BK query, must think about having a "data" view'
        print '      this method will also call run() internally'  
    
    def status(self):
        """ LHCb specific monitoring function for Analysis Task.
        """
        print 'TODO: LHCb specific status stuff'
            
    def get_container_name(self): #DQ2 specific
        nickname = 'user' #was atlas specific getNickname()
        name_base = ["user",nickname,self.creation_date,"task_%s" % self.id]
        return ".".join(name_base + [self.name]) + "/"
           
    def startup(self):
        super(AnalysisTask,self).startup()
        self.initAliases()
    
    def check(self):
        self.initAliases()
        if not self.name.replace(".","").replace("_","").isalnum(): # accept . and _
           logger.error("Invalid character in task name! Task names will be used as job names so no spaces, slashes or other special characters are allowed.")
           raise ApplicationConfigurationError(None, "Invalid Task name!")
        super(AnalysisTask,self).check()
    
    def initAliases(self):
        self.template = None
        if len(self.transforms) == 1:
           self.template = self.transforms[0].application
    
    def help(self):
        print help
    
    def overview(self):
        super(AnalysisTask, self).overview()
        print
        print "container of transform output datasets: %s" % self.container_name