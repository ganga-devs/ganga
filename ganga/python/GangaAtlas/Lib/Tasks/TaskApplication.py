
from Ganga.GPIDev.Schema import *
from common import *
from new import classobj
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

def __task__init__(self):
    ## This assumes TaskApplication is #1 in MRO ( the list of methods )
    baseclass = self.__class__.mro()[2]
    baseclass.__init__(self)
    ## Now do a trick to convince classes to use us if they foolishly check the name
    ## (this is a bug workaround)
    self._name = baseclass._name

def taskify(baseclass,name):
    smajor = baseclass._schema.version.major
    sminor = baseclass._schema.version.minor

    if baseclass._category == "applications":
        schema_items = {
            'id'       : SimpleItem(defvalue=-1, protected=1, copyable=1,splitable=1,doc='number of this application in the transform.', typelist=["int"]),
            'tasks_id' : SimpleItem(defvalue="-1:-1", protected=1, copyable=1,splitable=1,doc='id of this task:transform',typelist=["str"]),
            }.items()
        taskclass = TaskApplication
    elif baseclass._category == "splitters":
        schema_items = []
        taskclass = TaskSplitter

    classdict = {
        "_schema"   : Schema(Version(smajor,sminor), dict(baseclass._schema.datadict.items() + schema_items)), 
        "_category" : baseclass._category,
        "_name"     : name,
        "__init__"  : __task__init__,
        }

    for var in ["_GUIPrefs","_GUIAdvancedPrefs","_exportmethods"]:
        if var in baseclass.__dict__: 
            classdict[var] = baseclass.__dict__[var]
    cls = classobj(name,(taskclass,baseclass), classdict)
 
    ## Use the same handlers as for the base class
    for backend in allHandlers.getAllBackends(baseclass.__name__):
        allHandlers.add(name, backend, allHandlers.get(baseclass.__name__,backend))
    return cls

class TaskApplication(object):
    def getTransform(self):
        tid = self.tasks_id.split(":")
        if len(tid) == 2 and tid[0].isdigit() and tid[1].isdigit():
           task = GPI.tasks(int(tid[0]))
           if task:
              return task.transforms[int(tid[1])]
        return None 

    def transition_update(self,new_status):
        #print "Transition Update of app ", self.id, " to ",new_status
        try:
            if self.tasks_id == "00": ## Master job
               if new_status == "new": ## something went wrong with submission
                  for sj in self._getParent().subjobs:
                     sj.application.transition_update(new_status)
               return
            transform = self.getTransform()
            if transform:
               transform._impl.setAppStatus(self, new_status)
        except Exception, x:
            logger.error("Exception in call to transform[%s].setAppStatus(%i, %s)", self.tasks_id, self.id, new_status)
            logger.error("%s", x)


class TaskSplitter(object):
    ### Splitting based on numsubjobs
    def split(self,job):
        subjobs = self.__class__.mro()[2].split(self,job)
        ## Get information about the transform
        transform = job.application.getTransform()._impl
        id = job.application.id
        partition = transform._app_partition[id]
        ## Tell the transform this job will never be executed ...
        transform.setAppStatus(job.application, "removed")
        ## .. but the subjobs will be
        for i in range(0,len(subjobs)):
            subjobs[i].application.tasks_id = job.application.tasks_id
            subjobs[i].application.id = transform.getNewAppID(subjobs[i].application.partition_number)
            # Do not set to submitting - failed submission will make the applications stuck...
            # transform.setAppStatus(subjobs[i].application, "submitting")
        job.application.tasks_id = "00"
        return subjobs

from Ganga.GPIDev.Adapters.ISplitter import ISplitter

class AnaTaskSplitterJob(ISplitter):
    """AnaTask handler for job splitting"""
    _name = "AnaTaskSplitterJob"
    _category = "splitters"
    _schema = Schema(Version(1,0), {
        'subjobs'           : SimpleItem(defvalue=[],sequence=1, doc="List of subjobs", typelist=["int"]),
    } )
    def split(self,job):
        from Ganga.GPIDev.Lib.Job import Job
        logger.debug("AnaTaskSplitterJob split called")
        sjl = []
        transform = stripProxy(job.application.getTransform())
        transform.setAppStatus(job.application, "removed")
        # Do the splitting
        for sj in self.subjobs:
            j = Job()
            j.inputdata = transform.partitions_data[sj-1]
            j.outputdata = job.outputdata
            j.application = job.application
            j.application.atlas_environment.append("OUTPUT_FILE_NUMBER=%i" % sj)
            j.backend = job.backend
            if stripProxy(j.backend)._name == 'LCG':
                j.backend.requirements.sites = transform.partitions_sites[sj-1]
            j.inputsandbox = job.inputsandbox
            j.outputsandbox = job.outputsandbox
            sjl.append(j)
            # Task handling
            j.application.tasks_id = job.application.tasks_id
            j.application.id = transform.getNewAppID(sj)
            #transform.setAppStatus(j.application, "submitting")
        job.application.tasks_id = "00"
        return sjl

from Ganga.Lib.Executable.Executable import Executable
from GangaAtlas.Lib.AthenaMC.AthenaMC import AthenaMC, AthenaMCSplitterJob
from GangaAtlas.Lib.Athena.Athena import Athena

ExecutableTask = taskify(Executable,"ExecutableTask")
AthenaMCTask = taskify(AthenaMC,"AthenaMCTask")
AthenaMCTaskSplitterJob = taskify(AthenaMCSplitterJob,"AthenaMCTaskSplitterJob")
AthenaTask = taskify(Athena,"AthenaTask")
