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
      t.setTemplate(template)
      t.setQuery(BKquery('/foo/bar'))
      
      # Update dataset to make it start processing
      t.updateQuery()
      
      # See the progress
      t.progress()
      
      # Update query to get new data
      t.updateQuery()

"""
import string
import Ganga.Utility.logging

from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks import Task
from Ganga.GPIDev.Base.Proxy import isType
from Ganga.Core.exceptions import ApplicationConfigurationError,GangaAttributeError

from AnalysisTransform import AnalysisTransform

from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *
from GangaLHCb.Lib.LHCbDataset.BKQuery import BKQuery

logger = Ganga.Utility.logging.getLogger()

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
help.append('BK query'.ljust(adj)+sep+markup('t.setQuery(BKQuery("/foo/bar"))',command))
help.append(markup('\nOther useful commands'+sep,header))
help.append('Monitor progress'.ljust(adj)+sep+markup('t.progress()',command))
help.append('Get BK metadata for Task data'.ljust(adj)+sep+markup('t.getMetadata()',command))
help.append('Flush dataset (refresh BK query)'.ljust(adj)+sep+markup('t.updateQuery()',command))
help.append('Abandon processing a file'.ljust(adj)+sep+markup('t.abandonData(data)',command))
help.append('Forget about failed jobs and make another attempt'.ljust(adj)+sep+markup('t.retryAllFailedJobs()',command))
#help.append(markup('\nAnalysis Task Properties'+sep,header))

help = string.join(help,'\n')+'\n'
help_nocolor = help.replace(fgcol("blue"),"").replace(fx.normal, "").replace(fgcol("red"),"")

class AnalysisTask(Task):
    __doc__ = help_nocolor
    schema = {}
    schema['template']=SimpleItem(defvalue=None, transient=1, typelist=["object"], doc='Job template')
    # should make metadata, data, lostData  protected=1 in the future i.e. not modifiable via GPI
    schema['metadata']=SimpleItem(defvalue=[],sequence=1,typelist=['dict'],hidden=1,doc='BK metadata if specified via a query') #["dict","str","object","list"]
    schema['queryList']=SimpleItem(defvalue=[],typelist=['str'],sequence=1,protected=1,doc='List of BK paths.')
    schema['jobsData']=SimpleItem(defvalue={},typelist=["str"],hidden=1,copyable=0,doc='Job IDs and their data after creation.')
#    schema['jobsData']=SimpleItem(defvalue={},typelist=["str"],hidden=0,copyable=0,doc='Job IDs and their data after creation.')
    schema['data']=SimpleItem(defvalue={},typelist=["str"],hidden=1,copyable=0,doc='Dictionary of full file names and processing status.')
#    schema['data']=SimpleItem(defvalue={},typelist=["str"],hidden=0,copyable=0,doc='Dictionary of full file names and processing status.')
    schema['lostData']=SimpleItem(defvalue=[],sequence=1,typelist=["str"],protected=1,copyable=0,doc='Data no longer appearing in the BK after updateQuery() has been run.')
#    schema['lostData']=SimpleItem(defvalue=[],sequence=1,typelist=["str"],protected=1,copyable=0,doc='Data no longer appearing in the BK after updateQuery() has been run.')
    schema['abandonedData']=SimpleItem(defvalue=[],sequence=1,typelist=["str"],protected=1,copyable=0,doc='Data that has been removed from the sample and will not be processed.')
    schema['failedJobs']=SimpleItem(defvalue=[],sequence=1,protected=1,copyable=0,hidden=0,doc='List of jobs that have been rerun after abandoning a subset of the original data.')
    schema['container_name']=SimpleItem(defvalue="",protected=True,transient=1, getter="get_container_name", doc='name of the output container')
    schema['filesPerJob']=SimpleItem(defvalue=10,protected=True,hidden=1,doc='Files per job as chosen during last call to setDataset or updateQuery methods')
    _schema = Schema(Version(1,1), dict(Task._schema.datadict.items() + schema.items()))
    _category = 'tasks'
    _name = 'AnalysisTask'
    exportMethods =  ["setTemplate","setQuery","updateQuery","setDataset","getMetadata","getData","progress","abandonData","retryAllFailedJobs","getDistinctJobsList"]
    _exportmethods = Task._exportmethods + exportMethods
    
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
        #pass application from job template
        app = jobTemplate.application
        self.transforms[0].application=app 
        self.setBackend(jobTemplate.backend)
        #must think about input / output data / sandboxes etc. as necessary
        #TODO: might be best just to copy all template properties to the transform
        inputSandbox=jobTemplate.inputsandbox
        self.transforms[0].inputsandbox=inputSandbox
        outputSandbox=jobTemplate.outputsandbox
        self.transforms[0].outputsandbox=outputSandbox
        outputData=jobTemplate.outputdata
        self.transforms[0].outputdata=outputData
#  Not yet tested merging and how that would work but could be part of the post processing actions
#        merger=jobTemplate.merger
#        self.transforms[0].merger = merger
        splitter=jobTemplate.splitter
        self.transforms[0].splitter=splitter
            
    def retryAllFailedJobs(self):
        """ Calls retryFailed() for each transform.
        """
        for t in self.transforms:
            logger.debug('Attempting to retry failed jobs for transform %s' %(t.name))
            t.retryFailed()
            
    def abandonData(self,data,force=False):
        """ This method will allow to permanently abandon a selection of input data e.g. 
            if there is lost data this can be excluded from the sample of the rest.  
            
            Any leftover files for a given partition are recycled automatically via the
            creation of a new transform to process them.
        """
        if not data and not self.lostData:
            logger.info('No data specified, nothing to do')
            return

        if data:
            if not isType(data,LHCbDataset):
                raise GangaAttributeError(None,'abandonData() method only accepts LHCbDataset objects')
        else:
            data = LHCbDataset()
        
        ignore = []
        for fname in data.getFullFileNames():
            if not fname in self.data.keys():
                ignore.append(fname)
        
        msg = 'The following files will be ignored because they were not found in the sample:'
        for fname in ignore:
            msg.append('\n%s' %fname)
        if ignore:
            logger.warning(msg)
        
        if self.lostData:
            logger.info('%s files that were declared lost via updateQuery will be treated' %(len(self.lostData)))
            data.extend(self.lostData)
                         
        logger.debug(str(data.getFullFileNames()))
        if not self.status=='pause' and force: #don't change task status unless we are going to do something
            logger.info('Ensuring task %s is paused to avoid submission of new jobs...' %(self.id))
            self.pause()
        
        logger.debug('Jobs to examine: %s' %(self.jobsData.keys()))
        affected = {} #dict of jobIDs and data that should be removed (to track all files to be abandoned are found) 
        updated = {} #dict of existing, affected jobIDs and subset of data still ok
        for jobID,jobData in self.jobsData.items():
            for fname in data.getFullFileNames():
                if fname in jobData:
                    if not affected.has_key(jobID):
                        affected[jobID]=[fname] #of course can be many files in same job affected
                        newData = jobData
                        newData.remove(fname)
                        updated[jobID]=newData
                    else:
                        abandonedData = affected[jobID]
                        updatedData = updated[jobID]
                        abandonedData.append(fname)
                        updatedData.remove(fname)
                        affected[jobID]=abandonedData
                        updated[jobID]=updatedData
        
        #above handles case where jobs are already there, what happens if files are "New" i.e. not yet submitted?
        newFiles = []
        for lfn in data.getFullFileNames():
            if not lfn in ignore:
                if self.data[lfn]=='New':
                    newFiles.append(lfn)
        
        if not affected and not newFiles:
            logger.info('No affected job inputs or "New" input files were found for the specified data')
            return
        
        #Whether force is True or False ensure a detailed summary is printed:
        case = 'would'
        if force: case = 'will'        
        
        #handle New files by removing them from the affected partitions
        if newFiles:
            newDataset = LHCbDataset()
            newDataset.extend(newFiles)
            for i in range(len(self.transforms)):
                newPartitionsData = []
                for dataset in self.transforms[i].partitions_data:
                    newData = dataset.difference(newDataset)
                    newPartitionsData.append(newData)
                
                for dataset in self.transforms[i].partitions_data:
                    index = self.transforms[i].partitions_data.index(dataset)
                    dfiles = dataset.getFullFileNames()
                    nfiles = newPartitionsData[index].getFullFileNames()
                    if len(dfiles) != len(nfiles):
                        logger.info('%s "New" unprocessed file(s) %s be removed from Task data, %s left in partition' %((len(dfiles)-len(nfiles)),case,len(nfiles)))
               
                if force:
                    self.transforms[i].partitions_data = newPartitionsData
                    self.addAbandonedData(newFiles)
                    
        for job,jobData in affected.items():
            logger.info('%s file(s) %s be removed from original inputs of job %s, %s %s be recycled from partition' %(len(jobData),case,job,len(updated[job]),case))
        
        if not force:
            logger.info('** Please rerun command with force flag set to True to remove data **')
            logger.info('** e.g. abandonData(<dataset>,force=True) **')
            logger.info('** Also note that this is a one-way operation that cannot easily be undone. **')
            return
                
        #Now to finally handle the case where jobs have been created for the partition
        #that should now be properly re-created with a subset of the data.        
        #We have a dict of jobIDs and the new list of data that should be set as inputs
        #must work out which partitions these come from, reset the input data sets to the
        #new value, declare the partition as failed and carry on with the processing.
        for i in range(len(self.transforms)):
            newPartitionData = {}
            for jobID,newData in updated.items():
                print jobID,newData
                nd = LHCbDataset()
                nd.extend(newData)
                for part in self.transforms[i].partitions_data:
                    if nd.isSubset(part):
                        index = self.transforms[i].partitions_data.index(part)
                        newPartitionData[index]=nd
                if self.jobsData.has_key(jobID): #to ensure correct progress summary
                    logger.debug('Removing jobID %s from task job data dictionary' %(jobID))
                    jdcopy = self.jobsData.copy()
                    del jdcopy[jobID]
                    self.jobsData = jdcopy
                                
            for index,newDataset in newPartitionData.items():
                logger.debug('Resetting input data for transformation %s, partition %s' %(self.transforms[i].name,index+1))
                self.transforms[i].partitions_data[index]=newDataset                
                self.transforms[i].setFailed(index+1) #moves status to ready such that resubmission occurs with new dataset
        
        #Keep track of lost data which is now treated (i.e. any other affected files are recycled)
        self.addAbandonedData(self.lostData)
        self.lostData = []             
        for job,jobData in affected.items():
            self.addAbandonedData(jobData)
        
        #Keep track of jobs that will still be related to the underlying transforms but have older input data
        self.failedJobs += affected.keys()
        
        #Can print or set the task to run automatically now
        logger.info('** Operation complete, set task to run() in order to continue processing')
        return

    def addAbandonedData(self,data):
        """ Simple internal method to ensure abandoned data list is properly tracked.
        """
        for d in data:
            if not d in self.abandonedData:
                self.abandonedData.append(d)
            if self.data.has_key(d):
                self.data[d] = 'Abandoned'
            
    def setQuery(self,bkQuery,filesPerJob=10):
        """ Allows one or more LHCb BK query objects to define the dataset.
        """
        if self.metadata:
            self.metadata=[]
        
        bkQueryList = bkQuery
        if not type(bkQuery) is list:
            logger.debug('Assuming setQuery() argument is a single BK query object.')
            bkQueryList = [bkQuery]
        
        for bk in bkQueryList:
            if not isType(bk,BKQuery):
                raise GangaAttributeError(None,'setQuery() method only accepts BKQuery() objects (or a list of them)')
        
        self.queryList = [bk.path for bk in bkQueryList]
        
        #Now we can retrieve the datasets corresponding to the BK query objects
        datasets = []
        for bk in bkQueryList:
            result = bk.getDataset()
            #print result
            datasets.append(result)
            
        #At this point we know that the data is coming from the BK so can retrieve 
        #metadata that could be useful for the analysis (e.g. run numbers, event stats, etc.)
        for data in datasets:
            mdata = data.bkMetadata()
            if mdata['OK'] and mdata['Value']:
                result=mdata['Value']
                self.metadata.append(result) #i.e. a list of dictionaries containing {<LFN>:<metadata>} 

        #TODO: could add an "Are you sure?" dialogue to prevent accidentally trying to 
        #      append a query rather than creating a new task altogether.
        self.setDataset(datasets,filesPerJob,False)
    
    def setDataset(self,datasetList,filesPerJob=10,append=False):
        """ Instead of using a BK query can provide an LHCbDataset for input 
            data files directly. Dataset can be individual dataset or a 
            list of datasets.
            
             For each dataset in the datasetList a transform is created. 
             
             Can optionally set filesPerJob at the same time (can also be set
             for each transform independently.  
             
        """
        self.initAliases()
        if not type(datasetList) is list:
            logger.debug('Assuming setDataset() argument is a single dataset object.')
            datasetList = [datasetList]
        
        if not filesPerJob:
            filesPerJob=self.filesPerJob
        
        transform = None
        if self.transforms:
            transform = self.transforms[0]
                
        #GPIDev/Base/Proxy method stripProxy was called here in the ATLAS case, not
        #sure if this is necessary.
        finalDatasets = []
        for dataset in datasetList:
            if not len(dataset.files):
                logger.warning('Ignoring empty dataset specified to setDataset() method.')
                continue
              
            #probably need more protection in here eventually e.g. check LFNs if backend DIRAC etc. 
            finalDatasets.append(dataset)

        transformsList = []
        order = 0
        tData = {}
        for processable in finalDatasets:
            #Name the transforms via the order (also encode number of files)
            order+=1
            newTransform = transform.clone()
            tName = '%s_Files%s_Dataset%s' %(self.name,processable.__len__(),order)
            newTransform.name = tName
            newTransform.inputdata.extend(processable)
            newTransform.files_per_job=filesPerJob
            transformsList.append(newTransform)
            for lfn in processable.getFullFileNames():
                tData[lfn]='New'
        
        if append:
            # i.e. we are adding to existing transforms via updateQuery()
            self.transforms += transformsList
            self.data.update(tData)
        else:
            self.transforms = transformsList
            self.data = tData
        
        self.filesPerJob = filesPerJob
        self.initAliases()
    
    def updateQuery(self,filesPerJob=10):
        """ If the AnalysisTask dataset is defined via a BK query object this 
            method allows to retrieve the latest files from the BK as well as
            managing deprecated, lost or missing files. 
        """
        if not self.queryList:
            raise GangaAttributeError(None,'updateQuery() requires a previously defined query via setQuery()')
        
        bkPaths = self.queryList
        logger.info('Refreshing the following BK queries:\n%s' %(string.join(bkPaths,'\n')))
        
        toCheck = []
        for bk in self.queryList:
            toCheck.append(BKQuery(bk).getDataset())            

        #first check for files that have been added to the sample       
        taskData = self.getData()
        toAdd = []
        for check in toCheck:
            new = check.difference(taskData)
            if new.files:
                logger.info('Found %s new file(s) to be processed!' %(len(new.files)))
                toAdd.append(new)
        
        if toAdd:
            self.setDataset(toAdd,filesPerJob,True)
            #Must also ensure the metadata for new files is not lost
            for data in toAdd:
                mdata = data.bkMetadata()
                if mdata['OK'] and mdata['Value']:
                    result=mdata['Value']
                    self.metadata.append(result)
    
        #next look for files that may have been lost
        problematic = []
        compare = LHCbDataset()
        for check in toCheck:
            compare = compare.union(check)
        
        lost = taskData.difference(compare)
        if lost.files:
            logger.warn('Found %s file(s) that are no longer in BK!' %(len(lost.files)))
            logger.warn('View lost data using: task.lostData')
            problematic.append(lost)
        
        #what to do in this case? initially set a parameter of the task with the lost files
        #could potentially offer a method to help clean the problematic cases
        for prob in problematic:
            for fname in prob.getFullFileNames():
              if fname not in self.lostData:
                  self.lostData.append(fname)
        
        if not toAdd and not problematic:
            logger.info('No new data added to (or existing data removed fom) the bookkeeping for current task.')
    
    def getData(self):
        """ Uses the dictionary published during setDataset() to return an LHCbDataset 
            containing all task data. Any data declared as abandoned (e.g. lost after 
            updating a BK query or declared bad by the user) will be removed from the
            returned dataset.
        """
        dataList = self.data.keys()
        data = LHCbDataset()
        final = []
        for d in dataList:
            if not d in self.lostData and d not in self.abandonedData:
                final.append(d)
        data.extend(final)
        return data
    
    def getDistinctJobsList(self):
        """ Return the current distinct set of jobs charged with processing the 
            data sample.  As opposed to the parent class method getJobs() this method
            ignores previous attempts at processing the data and returns the list of
            jobIDs that are available.  
        """
        return self.jobsData.keys()
    
    def getMetadata(self):
        """ Retrieve BK metadata for all files in the dataset.
        """
        metadata = {}
        for mdata in self.metadata:
            metadata.update(mdata)
            
        #BK will normally strip "LFN:" from LFNs
        final = {}
        for l,m in metadata.items():
            final['LFN:%s' %(l.replace('LFN:',''))]=m
            
        return final
    
    def progress(self):
        """ LHCb specific monitoring function for Analysis Task.
        """
        #Can use the BK metadata and data overview to provide an
        #LHCb specific picture of the task progress
        print markup('<==== Summary of progress for task %s %s in "%s" status ====>' %(self.id,self.name,self.status),command)
        padj=20      
        jobIDs = self.jobsData.keys()
        if not jobIDs:
            print markup('\nJobs summary:\n',header)
            print 'No jobs found to examine for task %s %s in status "%s", try again after run()' %(self.id,self.name,self.status)
        else:
            print markup('\nJobs summary, total of %s submitted:\n' %(len(jobIDs)),header)
            statusCount = {}
            for j in self.getJobs():
                status = j.status
                if statusCount.has_key(status):
                    new = statusCount[status]+1
                    statusCount[status]=new
                else:
                    statusCount[status]=1

            statuses = statusCount.keys()
            statuses.sort()        
            print markup('Status'.ljust(padj)+'Number of jobs'.ljust(padj)+'Percentage',command)
            for s in statuses:
                print s.ljust(padj)+str(statusCount[s]).ljust(padj)+str((int(100*statusCount[s]/len(jobIDs))))
               
        print markup('\nData summary, total of %s files:\n' %(len(self.data.keys())),header)        
        dataCount = {}
        for lfn,status in self.data.items():
            if dataCount.has_key(status):
                new = dataCount[status]+1
                dataCount[status]=new
            else:
                dataCount[status]=1
        
        statuses = dataCount.keys()
        totalData = 0
        for i in dataCount.values(): totalData+=i
        statuses.sort()
        print markup('Status'.ljust(padj)+'Number of files'.ljust(padj)+'Percentage',command)
        for s in statuses:
            print s.ljust(padj)+str(dataCount[s]).ljust(padj)+str(int(100*dataCount[s]/totalData))
            
        metadata = self.getMetadata()
        if not metadata:
            print 'No BK metadata is available for an in-depth summary.'
            return
        
        runNumbers = []
        runData = {}
        eventStat = 0
        fileSize = 0 #bytes from BK by default
        for lfn,mdata in metadata.items():
            currentStat = mdata['EventStat']
            eventStat+=currentStat
            fsize = mdata['FileSize']
            fileSize+=fsize
            run=mdata['Runnumber'] #note lower case second n
            dstatus = self.data[lfn]
            if run not in runNumbers:
                runNumbers.append(run)
                runData[run]={dstatus:1}
            else:
                if not runData[run].has_key(dstatus):
                    runData[run][dstatus]=1
                else:
                    new = runData[run][dstatus]+1
                    runData[run][dstatus]=new

        print markup('\nRun Summary, %s distinct runs:' %(len(runNumbers)),header)
        runNumbers.sort()
        for run in runNumbers:
            runFiles=0
            for d in runData[run].values(): runFiles+=d
            print markup('\nRun %s, total files %s' %(run,runFiles),command)
            print 'Status'.ljust(padj)+'Number of files'.ljust(padj)+'Percentage' 
            for s in statuses:
                if runData[run].has_key(s):
                    print s.ljust(padj)+str(runData[run][s]).ljust(padj)+str(int(100*runData[run][s]/runFiles))
        
        print markup('\nBK Metadata:\n',header)
        print markup('Total events in sample (EventStat) : ',command).ljust(padj*2)+str(eventStat)
        print markup('Total file size of sample (GB) : ',command).ljust(padj*2)+'%.2f' %(float(fileSize)/float(1024*1024*1024))
                
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