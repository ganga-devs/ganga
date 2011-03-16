########################################################################
# File : AnalysisTransform.py
########################################################################

""" LHCb Analysis Transform

    This class looks after the LHCb specific analysis step, creating jobs
    and allowing for retries etc. 

"""

from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks import Transform

from TaskApplication import GaudiTask 

import time

from Ganga.Core.exceptions import ApplicationConfigurationError

from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *
from GangaLHCb.Lib.Gaudi.Splitters import SplitByFiles
from GangaLHCb.Lib.DIRAC.DiracSplitter import DiracSplitter

class AnalysisTransform(Transform):
    """ Analyzes Events """
    schema = {}
    schema['files_per_job']=SimpleItem(defvalue=5, doc='files per job', modelist=["int"])
    schema['partitions_data']=ComponentItem('datasets', defvalue=[], optional=1, sequence=1, hidden=1, doc='Input dataset for each partition')
    schema['dataset_name']=SimpleItem(defvalue="", transient=1, getter="get_dataset_name", doc='name of the output dataset')
    #removed from LHCb case
    #schema['partitions_sites']=SimpleItem(defvalue=[], hidden=1, modelist=["str","list"],doc='Input site for each partition')
    #schema['outputdata']=ComponentItem('datasets', defvalue=DQ2OutputDataset(), doc='Output dataset')
    _schema = Schema(Version(1,0), dict(Transform._schema.datadict.items() + schema.items()))
    _category = 'transforms'
    _name = 'AnalysisTransform'
    _exportmethods = Transform._exportmethods
    
    def initialize(self):
        super(AnalysisTransform, self).initialize()
        #      self.application = GaudiTask() #TODO specify splitter here?
        self.application=GaudiTask() 
        self.inputdata=LHCbDataset()
    
    ## Internal methods
    def checkCompletedApp(self, app):
        """ Post processing actions, this will be more clearly understood in the context
            of real LHCb use-cases. 
        """
        task = self._getParent()
        j = app._getParent()
        if not j.outputdata: #can't check anything
            self.setTaskFilesStatus(j.id,j.inputdata.getFullFileNames(),'Processed')
            logger.info('Job %s has completed but had no output data specified' %(j.id))
        elif j.outputdata:
            logger.info('Job %s produced output data %s' %(j.id,outputdata)) 
            self.setTaskFilesStatus(j.id,j.inputdata.getFullFileNames(),'Processed')
        else:
            #TODO: can check whether the output data LFNs exist here before declaring
            #      inputs are really processed                 
            logger.warning('Job %s with name %s completed but did not produce any output data...' %(j.id,j.name))
            self.setTaskFilesStatus(j.id,j.inputdata.getFullFileNames(),'Processed_NoOutputs')

        # if this is the first app to complete the partition...
        if self.getPartitionStatus(self._app_partition[app.id]) != "completed":
            task_container, subtask_dsname = task.container_name, self.dataset_name
        
        return True
     
#    def updateInputStatus(self, ltf, partition):
#        """Is called my the last transform (ltf) if the partition 'partition' changes status"""
#        # per default no dependencies exist
#        logger.debug('No actions to update input statuses after last transform will be taken.')

    def get_dataset_name(self):
        """ Propagated from the ATLAS case, perhaps not useful.
        """
        task = self._getParent()
        name_base = ["user",task.creation_date,"task_%s" % task.id]
        subtask_dsname = ".".join(name_base +["subtask_%s" % task.transforms.index(self)])
        #logger.warning('TODO: check whether this name "%s" is useful for LHCb' %(subtask_dsname))
        return subtask_dsname        
    
    def check(self):
        super(AnalysisTransform,self).check()
        if not self.inputdata.getFileNames():
            logger.error('Empty dataset for transform...')
            return
        if not self.backend:
            logger.warning("Determining backend and cloud TODO...")
            assert self.backend
        
        logger.info("Determining partition splitting...")
        logger.warning('TODO: disabled stuff here from the ATLAS case, splitter could be passed via template')
        
        #TODO: should the splitter be explicitly specified by the user via
        #      the job template or determined like the below? 
         
        #Choose splitter e.g. either DiracSplitter or SplitByFiles
        splitter = None
        if self.inputdata.hasLFNs():
          splitter = DiracSplitter()
        else:
          splitter = SplitByFiles()

        splitter.filesPerJob = self.files_per_job
        
        # is it necessary to send the instance self here? 
        sjl = splitter._splitFiles(self.inputdata)  
#        self.partitions_data = [sj.inputdata for sj in sjl]
        self.partitions_data = sjl
        
        # TODO: check that banned sites / forced destination sites propagation will work for LHCb
        
#        try:
#            self.partitions_sites = [sj.backend.requirements.sites for sj in sjl]
#        except AttributeError:
#            self.partitions_sites = [sj.backend.site for sj in sjl]
#            pass
        self.setPartitionsLimit(len(self.partitions_data)+1)
        self.setPartitionsStatus([c for c in range(1,len(self.partitions_data)+1) if self.getPartitionStatus(c) != "completed"], "ready")

    def getJobsForPartitions(self, partitions):
        """ This overrides the Transform baseclass method
        """
        j = self.createNewJob(partitions[0])
        print 'Note: disabled some ATLAS stuff here'
        #       if len(partitions) >= 1:
        #           j.splitter = AnaTaskSplitterJob()
        #           j.splitter.subjobs = partitions
        j.inputdata = self.partitions_data[partitions[0]-1]
        j.outputdata = self.outputdata
        task = self._getParent()
#        nickname = 'user'
#        dsn = ["user",nickname,task.creation_date,"%i.t_%s_%s" % (j.id, task.id, task.transforms.index(self))]
#        j.outputdata.datasetname = ".".join(dsn)

        # keep track of the jobs vs. data view:
        self.setTaskFilesStatus(j.id,j.inputdata.getFullFileNames(),'Assigned')
        return [j]

    def setTaskFilesStatus(self,jobID,dataList,status):
        """ Method to update the parent task dictionary of data and statuses.
        """
        task = self._getParent()
        for d in dataList:
            task.data[d]=status
        if not task.jobsData.has_key(jobID):
            task.jobsData[jobID]=dataList

    def info(self):
        """ Show some details about the current transform.
        """
        print markup("%s '%s'" % (self.__class__.__name__, self.name), status_colours[self.status])
        filesList = self.inputdata.getFileNames()
        filesTotal = len(filesList)
        print "* total of %s file(s) in dataset" %filesTotal
        print "* example file: %s" %filesList[0]
        print "* processing up to %s per job" % say(self.files_per_job,"file")
        print "* backend: %s" % self.backend.__class__.__name__
        print "* application:"
        self.application.printTree() 
       