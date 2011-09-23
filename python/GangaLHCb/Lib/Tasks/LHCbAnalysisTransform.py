
#from sets import Set
#from TaskApplication import ExecutableTask, taskApp
from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Job.Job import Job, JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.GPIDev.Lib.Tasks.Transform import Transform
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base.Proxy import *# isType#,stripProxy
from GangaLHCb.Lib.LHCbDataset.BKQuery import BKQuery
from GangaLHCb.Lib.DIRAC.DiracSplitter import DiracSplitter
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from LHCbTaskDummySplitter import LHCbTaskDummySplitter
from Ganga.Core import GangaException
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
from copy import deepcopy
import sets

status_colours['ready']=''
job_colours = {
    'new' : overview_colours['ready'],
    'submitting' : overview_colours['running'],
    'submitted' : overview_colours['running'],
    'running' : overview_colours['running'],
    'completing' : overview_colours['running'],
    'completed' : overview_colours['completed'],
    'killed' : overview_colours['attempted'],
    'failed' : overview_colours['failed'],
    'incomplete' : overview_colours['bad'],
    'unknown':overview_colours['unknown']
    }

class LHCbAnalysisTransform(Transform):
    
    _schema = Transform._schema.inherit_copy()
    _schema.datadict['name'].defvalue='LHCbAnalysisTransform' 
    _schema.datadict['query'] =  ComponentItem('query',defvalue=None,load_default=0,hidden=0,protected=0,optional=1,copyable=1,doc='Bookkeeping query object BKQuery()')
    _schema.datadict['splitter'] = ComponentItem('splitters',defvalue=None,load_default=0,hidden=0,protected=0,optional=1,copyable=1,doc='optional splitter')
    _schema.datadict['merger'] = ComponentItem('mergers',defvalue=None,load_default=0,hidden=0,protected=0,optional=1,copyable=1,doc='optional merger')
    _schema.datadict['removed_data']  =  ComponentItem('datasets', defvalue=None, optional=1, load_default=False,doc='removed dataset')
 #    _schema.datadict['toProcess_dataset']  =  ComponentItem('datasets', defvalue=None, hidden=1,optional=0,copyable=0, load_default=True,doc='dataset to process')
    _schema.datadict['task_id'] = SimpleItem(defvalue=-1,hidden=0,protected=1,optional=0,copyable=0,doc='index of the parent task',typelist=['int'])
    _schema.datadict['transform_id'] = SimpleItem(defvalue=-1,hidden=0,protected=1,optional=0,copyable=0,doc='index of this transform within parent task',typelist=['int'])
    _category = 'transforms'
    _name = 'LHCbAnalysisTransform'
    _exportmethods = Transform._exportmethods 
    _exportmethods += [ 'update','resubmit','resubmitFailedSubjobs' ]#,'addQuery']


## Special methods:

    def __init__(self):
        super(LHCbAnalysisTransform,self).__init__()
        # self.job=None
        #      self.num_updates=-1
        self.toProcess_dataset=None
        self.removed_data=LHCbDataset()
        # self.task = self._getParent()

    def startup(self):
        super(LHCbAnalysisTransform,self).startup()
        self.toProcess_dataset = None

    def __deepcopy__(self,memo = None):
        l=LHCbAnalysisTransform()
        l.application = deepcopy(self.application,memo)
        l.backend = deepcopy(self.backend,memo)
        l.splitter = deepcopy(self.splitter,memo)
        l.name = self.name
        return l

    def resubmitFailedSubjobs(self):
        status = ['failed','killed','attempted']
        for p in self._partition_status:
            for j in self.getPartitionJobs(p):
                if j.status in status: j.resubmit()
    
    def _attribute_filter__set__(self,name,value):
        if name is 'inputdata':
            pass
        elif name is 'query':
            if not isType(value,BKQuery):
                raise GangaAttributeError(None,'LHCbTransform expects a BKQuery object for its query attribute!')
        return value


    def _getJobsWithRemovedData(self, removed_dataset):
        logger.info('Checking for lost data, please wait...')
        jobs=[]
        running_status = set(['submitting','submitted','running','completing'])
        rf = set([file.name for file in removed_dataset.files])
        for p in self._partition_status:
            for pj in self.getPartitionJobs(p):
                pjf = set([file.name for file in pj.inputdata.files])
                dead_datafiles = pjf.intersection(rf)
                for ddf in dead_datafiles:
                    if pj.status in running_status:
                        logger.info('running job %s from %s has an obsolete datafile(s), it will be killed and re-submitted'%(pj.fqid,pj.name))
                        pj.kill()
                    del pj._impl.inputdata.files[pj.inputdata.getFileNames().index(ddf)]
                    jobs += [pj]
##                 if pj.status != 'completed': continue
##                 pf = set(pj.inputdata.files)
##                 dead_datafiles = pf.intersection(rf)
##                 for j in dead_datafiles:
##                     del pj.inputdata.files[pj.inputdata.files.index(j)]
##                     jobs += [pj]
        return jobs
    
    def update(self):
        #self.job=None
        if self.query is None:
            raise GangaException(None,'Cannot call update() on an LHCbTransform without the query attribute set')
        if self.toProcess_dataset is not None:
            raise GangaException(None,'Cannot call update() on an LHCbTransform that has already been updated. There is outstanding data to process.')
      
        # Get the latest dataset
        logger.info('Retrieving latest bookkeeping information for transform %i:%i, please wait...'%(self.task_id,self.transform_id))
        latest_dataset=self.query.getDataset()
        self.toProcess_dataset=latest_dataset
        redo_jobs = False
        if self.inputdata is not None:
            ## Get new files
            self.toProcess_dataset = latest_dataset.difference(self.inputdata)
            ## Get removed files
            self.removed_data.files += self.inputdata.difference(latest_dataset).files
            ## If nothing to be updated then exit
            for j in self._getJobsWithRemovedData(self.removed_data):
                logger.info('Resubmitting job \'%s\' as it\'s dataset is out of date.'%j.name)
                j.resubmit()
                redo_jobs = True

        new_jobs = len(self.toProcess_dataset.files)
        if not new_jobs and not redo_jobs:
#            task = self._getParent()
            logger.info('Transform %i:%i is already up to date'%(self.task_id,self.transform_id))
            return

        if new_jobs:
            logger.info('Transform %i:%i updated, adding partition %i containing %i more file(s) for processing'%(self.task_id,self.transform_id,len(self._partition_status),len(self.toProcess_dataset.files)))
            self.setPartitionStatus(len(self._partition_status),'ready')
            if self.status != 'new': self.submitJobs(1) ## After the first time, when transform is running, calling update will submit the jobs so no waiting for the monitoring thread
        self.inputdata = LHCbDataset(latest_dataset.files)

    def run(self, check=True):
        if self.status == "new" and check:
            self.check()
        if self.status != "completed":
            self.submitJobs(1)
            self.updateStatus("running")
            # self.status = "running"
            # Check if this transform has completed in the meantime
            is_complete = True
            for s in self._partition_status.values():
                if s != "completed" and s != "bad":
                    is_complete = False
                    break
            if is_complete:
                self.updateStatus("completed")
        else:
            logger.warning("Transform is already completed!")
        #super(LHCbAnalysisTransform,self).run(check)


    def createNewJob(self, partition):
        """ Returns a new job initialized with the transforms application, backend and name """
        # task = self._getParent() # this works because createNewJob is only called by a task
        # id = task.transforms.index(self)
##         if self.toProcess_dataset is None:
##             logger.warning('No dataset to attach to new job, this message could arise due to conflict with the monitoring thread in which case please ignore')
##             return []
## #            raise GangaException(None,'Cannot create a job if there is no data to process')

        j = GPI.Job()
        j._impl.backend = self.backend.clone()
        j._impl.application = self.application.clone()
        j._impl.application.tasks_id = "%i:%i" % (self.task_id, self.transform_id)
        j._impl.application.id = self.getNewAppID(partition)
        if self.splitter is not None:
            j._impl.splitter = LHCbTaskDummySplitter(self.splitter)
        if self.merger is not None:
            j._impl.merger = self.merger
        j.inputdata = self.toProcess_dataset
        j.outputdata = self.outputdata
        j.inputsandbox = self.inputsandbox
        j.outputsandbox = self.outputsandbox
        j.name = "T%i Tr%i P%i" % (self.task_id, self.transform_id, partition)
        j.do_auto_resubmit = True
        self.toProcess_dataset = None
        return j

## seems to only be called by the monitoring thread, leaving room for the user to
## quit the session before everything is submitted. for large submits associated with
## transform.run() and t.update() this is called now by the user thread using
## self.submitJobs(1)
    def getJobsForPartitions(self, partitions):
        """This is only an example, this class should be overridden by derived classes"""
        if len(partitions) > 1:
            raise GangaException(None,'Dont know how to deal with multiple partition creation yet.')
        if self.toProcess_dataset is None:
            # need to fix this in future releases
            #logger.warning('No dataset to attach to new job, this message could arise due to conflict with the monitoring thread in which case please ignore')
            return []
#            raise GangaException(None,'Cannot create a job if there is no data to process')

        j=self.createNewJob(partitions[0])
        return [j]

#        return [self.createNewJob(p) for p in partitions]
    


# seems to be called at startup only, now calling it also in overview to pick up complete state
# could use a thread
    def updatePartitionStatus(self, partition):
        """ Calculate the correct status of the given partition. 
        "completed" and "bad" is never changed here
        "hold" is only changed to "completed" here. """
        #print "updatePartitionStatus ", partition, " transform ", self.id

        running_status = set(['submitting','submitted','running','completing'])
        # If the partition has status, and is not in a fixed state, check it!        
        if partition in self._partition_status and (not self._partition_status[partition] in ["bad","completed"]):
            ## if we have no applications, we are in "ready" state
            if not partition in self.getPartitionApps():
                if self._partition_status[partition] != "hold":
                    self._partition_status[partition] = "ready"
            elif self._partition_status[partition] != "hold":
                status = set([pj.status for pj in self.getPartitionJobs(partition)])
                
                if status.intersection(running_status):
                    self._partition_status[partition] = "running"
                    return

                if 'new' in status:
                    self._partition_status[partition] = 'ready'
                    return

                if 'failed' in status:
                    failures = self.getPartitionFailures(partition)
                    if failures >= self.run_limit:
                        self._partition_status[partition] = "failed"
                    elif failures > 0:
                        self._partition_status[partition] = "attempted"
                else:
                    self._partition_status[partition] = "completed"

        self.notifyNextTransform(partition)

        ## Update the Transform and Tasks status if necessary
        task = self._getParent()
        status=set(self._partition_status.values())
#        print "self.status =",self.status
#        print "status.intersection(running_status) =",status.intersection(running_status)
        if self.status=='running' and not status.intersection(running_status):
            if 'failed' in status:
#                print "failed in status"
                self.updateStatus("failed")
            elif 'attempted' in status:
#                print 'attempted in status'
                self.updateStatus('attempted')
            else:
#                print "completed"
                self.updateStatus("completed")
        elif self.status=='completed' and status.intersection(running_status):
            self.updateStatus('running')
#        elif self.status=='completed' and 'ready' in status:
#            self.updateStatus('new')
        
        if task:
            task.updateStatus()
            

##         if partition in self._partition_status and self._partition_status[partition] in ["completed","bad"] and self.status == "running":
##             for s in self._partition_status.values():
##                 if s != "completed" and s != "bad":
##                     return
##             # self.status = "completed"
##             self.updateStatus("completed")
##             if task:
##                 task.updateStatus()
##         elif self.status == "completed":
##             for s in self._partition_status.values():
##                 if s != "completed" and s != "bad":
##                     self.updateStatus("running")
##                     # self.status = "running"
##                     if task:
##                         task.updateStatus()
##                     return

                
##                 status = [self._app_status[app] for app in self.getPartitionApps()[partition] 
##                           if app in self._app_status and not self._app_status[app] in ["removed","killed"]]
##                 ## Check if we have completed this partition
##                 if "completed" in status:
##                     self._partition_status[partition] = "completed"
##                 ## Check if we are not on hold
##                 elif self._partition_status[partition] != "hold":
##                     ## Check if we are running
##                     running = False
##                     for stat in ["completing", "running", "submitted", "submitting"]:
##                         if stat in status:
##                             self._partition_status[partition] = "running"
##                             running = True
##                             break
##                     if not running:
##                         ## Check if we failed
##                         #failures = len([stat for stat in status if stat in ["failed","new"]])
##                         failures = self.getPartitionFailures(partition)
                        
##                         if failures >= self.run_limit:
##                             self._partition_status[partition] = "failed"
##                         elif failures > 0:
##                             self._partition_status[partition] = "attempted"
##                         else:
##                             ## Here we only have some "unknown" applications
##                             ## This could prove difficult when launching new applications. Care has to be taken
##                             ## to get the applications out of "unknown" stats as quickly as possible, to avoid double submissions.
##                             #logger.warning("Partition with only unknown applications encountered. This is probably not a problem.")
##                             self._partition_status[partition] = "ready"
##         ## Notify the next transform (if any) of the change in input status
##         self.notifyNextTransform(partition)

##       ## Update the Tasks status if necessary
##       task = self._getParent()
##       if partition in self._partition_status and self._partition_status[partition] in ["completed","bad"] and self.status == "running":
##          for s in self._partition_status.values():
##             if s != "completed" and s != "bad":
##                return
##          #self.status = "completed"
##          self.updateStatus("completed")
##          if task:
##             task.updateStatus()
##       elif self.status == "completed":
##          for s in self._partition_status.values():
##             if s != "completed" and s != "bad":
##                self.updateStatus("running")
##                #self.status = "running"
##                if task:
##                   task.updateStatus()
##                return

    def _datafile_count(self,job_reg_slice):
        r = 0
        for j in job_reg_slice:
            r+= len(j.inputdata.files)
        return r

    def overview(self):
        """ Get an ascii art overview over task status. Can be overridden """
        task = self._getParent() 
        if not task is None:
            id = str(task.transforms.index(self))
        else:
            id = "?"
        o = markup("#%s: %s '%s'\n" % (id, self.__class__.__name__, self.name), status_colours[self.status])
#        i = 0
        partitions = self._partition_status.keys()
        partitions.sort()
        for c in partitions:
            self.updatePartitionStatus(c)
            s = self._partition_status[c]
            if c in self.getPartitionApps():
                failures = self.getPartitionFailures(c)


                p_jobs = [pj for pj in self.getPartitionJobs(c)]
#                print "Alex unsorted",[pj.id for pj in p_jobs]
                p_jobs.sort(key=lambda job: job.id)
                master_jobNo = p_jobs[0].fqid.split('.')[0]
                o += markup("Partition %i (attached to job# %s, containing %i datafiles):%i\n" % (c,master_jobNo,self._datafile_count(self.getPartitionJobs(c)),failures), status_colours[s])
## TODO: at the moment the line above reports failures while that below reports
## submissions
#                print "Alex sorted",[pj.id for pj in p_jobs]
#                if p_jobs[-1].id==0: p_jobs = p_jobs[-1:]+p_jobs[:-1] # strange but sort puts 0 at the end
                for j in p_jobs:
                    fails = 0
                    if j.info.submit_counter is not 0: fails = j.info.submit_counter-1
                    if j.status is 'failed': fails+=1
                    o += markup("%i:%i "%(j.id,fails),job_colours[j.status])
                    if (p_jobs.index(j)+1) % 20 == 0: o+="\n"
                o+="\n"
            else:
                o += markup("Partition %i" % c, overview_colours[s])
        print o

