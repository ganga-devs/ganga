from Ganga import GPI
from Ganga.GPIDev.Lib.Tasks.common import status_colours, overview_colours, markup, fgcol, col
from Ganga.GPIDev.Lib.Job.Job import Job
from Ganga.GPIDev.Lib.Tasks.Transform import Transform
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base.Proxy import isType
from GangaLHCb.Lib.LHCbDataset.BKQuery import BKQuery
from LHCbTaskDummySplitter import LHCbTaskDummySplitter
from Ganga.Core import GangaException, GangaAttributeError
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
import Ganga.Utility.Config
from copy import deepcopy
import sets
config = Ganga.Utility.Config.getConfig('Configuration')

partition_colours = {
    'ignored'    : "",
    'hold'       : fgcol("lgray"),
    'ready'      : fgcol("lgreen"),
    'running'    : fgcol("green"),
    'completed'  : fgcol("blue"),
    'attempted'  : fgcol("yellow"),
    'failed'     : fgcol("lred"),
    'bad'        : fgcol("red"),
    'unknown'    : fgcol("white"),
    }

job_colours = {
    'new'        : col("black","white"),
    'submitting' : col("black","orange"),
    'submitted'  : col("black","orange"),
    'running'    : col("black","green"),
    'completing' : col("black","green"),
    'completed'  : col("white","blue"),
    'killed'     : col("white","lred"),
    'failed'     : col("black","lred"),
    'incomplete' : col("red","lcyan"),
    'unknown'    : col("white","magenta")
    }

class LHCbAnalysisTransform(Transform):
    
    _schema = Transform._schema.inherit_copy()
    _schema.datadict['name'].defvalue='LHCbAnalysisTransform' 
    _schema.datadict['query'] =  ComponentItem('query',defvalue=None,load_default=0,hidden=0,protected=0,optional=1,copyable=1,doc='Bookkeeping query object BKQuery()')
    _schema.datadict['splitter'] = ComponentItem('splitters',defvalue=None,load_default=0,hidden=0,protected=0,optional=1,copyable=1,doc='optional splitter')
    _schema.datadict['merger'] = ComponentItem('mergers',defvalue=None,load_default=0,hidden=0,protected=0,optional=1,copyable=1,doc='optional merger')
    _schema.datadict['removed_data']  =  ComponentItem('datasets', defvalue=None, optional=1, load_default=False,doc='removed dataset')
    _schema.datadict['toProcess_dataset']  =  ComponentItem('datasets', defvalue=None, hidden=1,optional=0,copyable=0, load_default=True,doc='dataset to process')
    _schema.datadict['task_id'] = SimpleItem(defvalue=-1,hidden=0,protected=1,optional=0,copyable=0,doc='index of the parent task',typelist=['int'])
    _schema.datadict['transform_id'] = SimpleItem(defvalue=-1,hidden=0,protected=1,optional=0,copyable=0,doc='index of this transform within parent task',typelist=['int'])
    _category = 'transforms'
    _name = 'LHCbAnalysisTransform'
    _exportmethods = Transform._exportmethods 
    _exportmethods += [ 'update' ]

    ## Special methods
    #####################################################################

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def __init__(self):
        super(LHCbAnalysisTransform,self).__init__()
        self.toProcess_dataset=LHCbDataset()
        self.removed_data=LHCbDataset()

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def __deepcopy__(self,memo = None):
        l=LHCbAnalysisTransform()
        l.application = deepcopy(self.application,memo)
        l.backend = deepcopy(self.backend,memo)
        l.splitter = deepcopy(self.splitter,memo)
        l.name = self.name
        return l

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def _attribute_filter__set__(self,name,value):
        if name is 'inputdata':
            pass ## to do...
        elif name is 'query':
            if not isType(value,BKQuery):
                raise GangaAttributeError(None,'LHCbTransform expects a BKQuery object for its query attribute!')
        return value

    ## Public GPI methods
    #####################################################################

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def overview(self):
        """ Get an ascii art overview over task status. Can be overridden """
        o = markup("Transform %s: %s '%s'\n" % (self.transform_id, self.__class__.__name__, self.name), status_colours[self.status])
        o += "------------------------------------------------------------\n"
        partitions = self._partition_status.keys()
        partitions.sort()
        for c in partitions:
            s = self._partition_status[c]
            if c in self.getPartitionApps():
                
                mj = self._getPartitionMasterJob(c)
                failure = 0
                if mj.status in ['submitting','submitted','running','completing','completed']:
                    failure = mj.info.submit_counter - 1
                else: failure = mj.info.submit_counter
                
                o += markup("Partition %i (attached to job# %s, containing %i datafiles):%i" % (c,mj.id,self._datafile_count(self.getPartitionJobs(c)),failure), partition_colours[s])
                o += '\n'
                p_jobs = [pj for pj in self.getPartitionJobs(c)]
                p_jobs.sort(key=lambda job: job.id)
                for j in p_jobs:
                    fails = 0
                    if j.status in ['submitting','submitted','running','completing','completed']:
                        fails = j.info.submit_counter - 1
                    else: fails = j.info.submit_counter
                    o += markup("%i:%i"%(j.id,fails),job_colours[j.status])
                    o += " "
                    if (p_jobs.index(j)+1) % 20 == 0: o+="\n"
                o+="\n"
            else:
                o += markup("Partition %i" % c, overview_colours[s])
                o+="\n"
        print o

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def run(self, check=True):
        self.submitJobs(1)
        return super(LHCbAnalysisTransform,self).run(check)

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def update(self, resubmit=False):
        if self.query is None:
            raise GangaException(None,'Cannot call update() on an LHCbTransform without the query attribute set')
        if len(self.toProcess_dataset.files):
            raise GangaException(None,'Cannot call update() on an LHCbTransform that has already been updated. There is outstanding data to process, try typing transform.run()')
      
        ## Get the latest dataset
        logger.info('Retrieving latest bookkeeping information for transform %i:%i, please wait...'%(self.task_id,self.transform_id))
        latest_dataset=self.query.getDataset()
        self.toProcess_dataset.files = latest_dataset.files

        if self.inputdata is not None:
            ## Get new files
            self.toProcess_dataset.files = latest_dataset.difference(self.inputdata).files
            ## Get removed files
            self.removed_data.files += self.inputdata.difference(latest_dataset).files
            ## If nothing to be updated then exit
            redo_jobs = self._getJobsWithRemovedData(self.removed_data)
            if redo_jobs and not resubmit:
                logger.info('There are jobs with out-of-date datasets, some datafiles must'\
                            'be removed. Updating will mean loss of existing output and mean that mergers'\
                            'must be rerun. Due to the permenant nature of this request please recall'\
                            'update with the True argument as update(True)')
                logger.info('Continuing to look for new data...')
            else:
                for j in redo_jobs:
                # for j in self._getJobsWithRemovedData(self.removed_data):
                    logger.info('Resubmitting job \'%s\' as it\'s dataset is out of date.'%j.name)
                    j.resubmit()

        new_jobs = len(self.toProcess_dataset.files)
        if not new_jobs and not redo_jobs:
            logger.info('Transform %i:%i is already up to date'%(self.task_id,self.transform_id))
            return

        if new_jobs:
            logger.info('Transform %i:%i updated, adding partition %i containing %i more file(s) for processing'%(self.task_id,self.transform_id,len(self._partition_status),len(self.toProcess_dataset.files)))
            self.setPartitionStatus(len(self._partition_status),'ready')
            if self.status != 'new': self.submitJobs(1) ## After the first time, when transform is running or complete, calling update will submit the jobs thereby blocking the user thread
        self.inputdata = LHCbDataset(latest_dataset.files)

    ## Public methods
    #####################################################################

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    ## Called as part of the tasks monitoring loop when task not in 'new'
    def checkStatus(self):
        self.updatePartitions()
        status = set(self._partition_status.values())
        if 'running' in status:
            self._resubmitAttemptedJobs() ## Check here as well else have to wait until all partitions are finished.
            self.updateStatus('running')
        elif 'ready' in status:
            self.updateStatus('running')
        elif 'attempted' in status:
            self._resubmitAttemptedJobs()
            self.updateStatus('running')
        # elif 'failed' in status:
            # self.updateStatus('completed')
        else:
            self.updateStatus('completed')

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def createNewJob(self, partition):
        """ Returns a new job initialized with the transforms application, backend and name """
        j = GPI.Job()
        j._impl.backend = self.backend.clone()
        j._impl.application = self.application.clone()
        j._impl.application.tasks_id = "%i:%i" % (self.task_id, self.transform_id)
        j._impl.application.id = self.getNewAppID(partition)
        if self.splitter is not None:
            j._impl.splitter = LHCbTaskDummySplitter(self.splitter)
        # if self.merger is not None:
            # j._impl.merger = self.merger
        j.inputdata = self.toProcess_dataset
        j.outputdata = self.outputdata
        j.inputsandbox = self.inputsandbox
        j.outputsandbox = self.outputsandbox
        j.name = "T%i Tr%i P%i" % (self.task_id, self.transform_id, partition)
        j.do_auto_resubmit = True
        self.toProcess_dataset.files = []
        return j

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    ## seems to only be called by the monitoring thread as part of submitJobs, leaving room for the user to
    ## quit the session before everything is submitted. for large submits associated with
    ## transform.run() and t.update() this is called now by the user thread via
    ## self.submitJobs(1)
    def getJobsForPartitions(self, partitions):
        """This is only an example, this class should be overridden by derived classes"""
        ## need to fix this in future releases
        if len(partitions) > 1:
            logger.warning('Dont know how to deal with multiple partition creation yet.')
            return []
        if not len(self.toProcess_dataset.files):
            # logger.warning('No dataset to attach to new job, this message could arise due to conflict with the monitoring thread in which case please ignore')
            return []

        j=self.createNewJob(partitions[0])
        return [j]

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def updatePartitions(self):
        for p in self._partition_status:
            self.updatePartitionStatus(p)
            
    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    ## Sseems to be called at startup only!?, now calling it also in checkStatus to make
    ## it part of the Tasks monitoring thread loop.
    def updatePartitionStatus(self, partition):
        """ Calculate the correct status of the given partition."""
        running_status = set(['submitting','submitted','running','completing'])
        nonProcessing_states = set(['bad','ignored','unknown'])
        ## If the partition has status, and is not in a fixed state...
        if partition in self._partition_status and (not self._partition_status[partition] in nonProcessing_states):

            ## if we have no applications, we are in "ready" state
            if not partition in self.getPartitionApps():
                if self._partition_status[partition] != "hold":
                    self._partition_status[partition] = "ready"
                    
            elif self._partition_status[partition] != "hold":
                status = set([pj.status for pj in self.getPartitionJobs(partition)])
                
                if status.intersection(running_status):
                    self._partition_status[partition] = "running"
                elif 'new' in status:
                    self._partition_status[partition] = 'ready'
                elif 'failed' in status:
                    mj = self._getPartitionMasterJob(partition)
                    if mj.status not in running_status:
                        failures = mj.info.submit_counter
                        if failures >= self.run_limit:
                            self._partition_status[partition] = "failed"
                        elif failures > 0:
                            if len(mj.subjobs): mj._impl.info.submit_counter +=1 ## Catches the fact that master job submit counter doesnt increment when subjobs resubmitted.
                            self._partition_status[partition] = "attempted"
                else:
                    self._partition_status[partition] = "completed"

    ## Private methods
    #####################################################################

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def _datafile_count(self,job_reg_slice):
        r = 0
        for j in job_reg_slice:
            r+= len(j.inputdata.files)
        return r

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
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
        return jobs

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def _getPartitionMasterJob(self,partition):
        partition_jobs = self.getPartitionJobs(partition) ## only call method once
        if not len(partition_jobs):
            raise GangaException(None,'Cant get partition master job when NO jobs assigned to partition')
        elif len(partition_jobs) is 1:
            return partition_jobs[0]
        ## Need registry access here might be better to get registry directly
        ## as in prepared stuff, see Executable for example or even tasksregistry.py!
        return GPI.jobs(partition_jobs[0].fqid.split('.')[0])

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    ## Once partition finished, if in state 'partition_status' then resubmit
    ## jobs in state 'job_status'. This works in conjunction with the auto_resubmit
    ## which only resubmits subjobs while the master job is running IF they have not
    ## been submitted more than n times and IF the ratio of failed to complete is betetr
    ## than x. This will resubmit ALL failed jobs once the partition has finished.
    def _resubmitAttemptedJobs(self):
        partition_status = ['attempted']
        job_status = ['failed','killed']
        for p in (part for part, state in self._partition_status.iteritems() if state in partition_status):
            for j in (job for job in self.getPartitionJobs(p) if job.status in job_status):
                if j.info.submit_counter >= self.run_limit: continue
                j.resubmit()

## End of class LHCbAnalysisTransform
########################################################################
