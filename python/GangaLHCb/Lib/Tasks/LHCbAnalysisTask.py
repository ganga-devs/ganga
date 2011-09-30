#from Ganga.GPIDev.Lib.Tasks.common import *
#from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
#import time
from LHCbAnalysisTransform import *
from Ganga.GPIDev.Lib.Tasks.Task import Task
#from Ganga.GPIDev.Base.Proxy import isType
#from GangaLHCb.Lib.LHCbDataset.BKQuery import BKQuery
#from copy import deepcopy
#from multiprocessing import Process
#import sys,time
########################################################################

class LHCbAnalysisTask(Task):
    """The LHCbAnalysisTask class looks after the running of LHCb Analysis jobs, including helping to keep
    them up to date when new data is added or removed.

    Create a task using the following syntax

    In[1]: t = LHCbAnalysisTask()

    Append a transform to the task as below:

    In[2]: tr = LHCbAnalysisTransform()
    In[3]: t.appendTransform(tr)

    Or use an existing transform as a template to clone many new transforms, each with their
    own bookkeeping query (BKQuery) object.

    In[4]: t.addQuery(tr,[BKQuery('<dataset1>'), BKQuery(<dataset2>)])

    To set task running, do:

    In[5]: t.run()

    Can view the tasks registry by using either of the following

    In[6]: tasks          # less verbose
    In[7]: tasks.table()  # more verbose

    A Pretty view of all the partition and subjobs within a task can be obtained with.

    In[8]: t.overview()
    
    Finally can update all the transforms within a task with.

    In[9]: t.update()"""

    _schema = Task._schema.inherit_copy()
    _schema.datadict['name'].defvalue='LHCbAnalysisTask'
    _category = 'tasks'
    _name = 'LHCbAnalysisTask'
    _exportmethods = Task._exportmethods
    _exportmethods +=['addQuery','update','resubmitFailedSubjobs']
    
    default_registry = "tasks"

    

    def resubmitFailedSubjobs(self):
        """If some of the transforms in this task have failed subjobs within a partition
        then this method will automatically resubmit them all"""
        for t in self.transforms:
            t.resubmit()

    def update(self, resubmit=False):
        ## Tried to use multithreading, better to check the tasksregistry class
        for t in self.transforms:
            t.update(resubmit)

    def addQuery(self, transform,bkQuery,associate=True):
        """ Allows one or more LHCb BK query objects to define the dataset. """
        if not isType(transform,LHCbAnalysisTransform):
            raise GangaException(None,'First argument must be an LHCbAnalysisTransform objects to use as the basis for establishing the new transforms')

        try:
            self.transforms.index(transform)
        except:
            if associate:
                logger.info('The transform is not associated with this Task, doing so now.')
                self.appendTransform(transform)

        
        if type(bkQuery) is not list: ## If single objects arg
            if not isType(bkQuery,BKQuery):
                raise GangaAttributeError(None,'LHCbTransform expects a BKQuery object or list of BKQuery objects passed to the addQuery method')
            if transform.query is not None: ## If self.query already set
                logger.info('Duplicating transform to add new query.')

                tr = deepcopy(transform)
                tr.query = stripProxy(bkQuery)
                self.appendTransform(tr)
#                tr.update()
            else: ## If transform.query still = None
                logger.info('Attaching query to transform')
                transform.query=stripProxy(bkQuery)
                transform.update()
        else: ## If list arg
            for bk in bkQuery:
                if not isType(bk,BKQuery):
                    raise GangaAttributeError(None,'LHCbTransform expects a BKQuery object or list of BKQuery objects passed to the addQuery method!')
                self.addQuery(transform,bk)


    def overview(self):
        """ Get an ascii art overview over task status. Can be overridden """
        print "Partition Colours: " + ", ".join([markup(key, partition_colours[key])
            for key in ["hold", "ready", "running", "completed", "attempted", "failed", "bad", "unknown"]])
        print "Job Colours: " + ", ".join([markup(job, job_colours[job])
            for job in ["new", "submitting", "running", "completed", "killed", "failed", "incomplete", "unknown"]])
        print "Lists the transforms, their partitions and partition subjobs, as well as the number of failures."
        print "Format: (partition/subjob number)[:(number of failed attempts)]"
        print
        for t in self.transforms:
            t.overview()


    def help(self):
        print "This is an LHCbTask, Which simplifies the query driven analysis of data"

    def appendTransform(self,transform):
        r=super(LHCbAnalysisTask,self).appendTransform(transform)
        if hasattr(transform,'task_id'):
            transform._impl.task_id = self.id
        else:
            raise GangaException(None,'Couldnt set the task id')
        if hasattr(transform,'transform_id'):
            try:
                transform._impl.transform_id = self.transforms.index(transform)
            except:
                raise GangaException(None,'transform not added to task properly')
        else:
            raise GangaException(None,'Coundnt set the transform id')
        transform.update()
        self.updateStatus()
        return r


    def updateStatus(self):
        if self.status is not 'new':# only want to start this when user runs the task else jobs submitted when appending
            running_state = set(['submitting','submitted','running','completing'])
            for t in self.transforms:
                t.updatePartitions()
                status = set(t._partition_status.values())
                if status.intersection(running_state):
                    t.updateStatus('running')
                elif 'ready' in status:
                    t.updateStatus('running')
                elif 'attempted' in status:
                    t.updateStatus('running')
##             elif 'failed' in status:
##                 t.updateStatus('completed')
                else:
                    t.updateStatus('completed')
        return super(LHCbAnalysisTask,self).updateStatus()
    
