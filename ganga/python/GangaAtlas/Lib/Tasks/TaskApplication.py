
from Ganga.GPIDev.Schema import *
from common import *
from new import classobj
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

def __task__init__(self):
    ## This assumes TaskApplication is first in MRO ( the list of methods )
    baseclass = self.__class__.mro()[2]
    # some black magic to allow derived classes to specify inherited methods in
    # the _exportmethods variable without redefining them
    from Ganga.GPIDev.Base.Proxy import ProxyMethodDescriptor
    for t in baseclass.__dict__:
        if (not t in self._proxyClass.__dict__) and (t in self._exportmethods):
            f = ProxyMethodDescriptor(t,t)
            f.__doc__ = baseclass.__dict__[t].__doc__
            setattr(self._proxyClass, t, f)
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
        "__init__"  : __task__init__
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
        return GPI.tasks.get(int(self.tasks_id.split(":")[0])).transforms[int(self.tasks_id.split(":")[1])]

    def transition_update(self,new_status):
        #print "Transition Update of app ", self.id, " to ",new_status
        try:
            if self.tasks_id == "00": ## Silent job
               return
            self.getTransform()._impl.setAppStatus(self, new_status)
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
        tasksid = job.application.tasks_id
        partition = transform._app_partition[id]
        ## Tell the transform this job will never be executed ...
        transform.setAppStatus(job.application, "removed")
        ## .. but the subjobs will be
        for i in range(0,len(subjobs)):
            subjobs[i].application.tasks_id = tasksid
            subjobs[i].application.id = transform.getNewAppID(subjobs[i].application.partition_number)
            transform.setAppStatus(subjobs[i].application, "submitting")
        return subjobs

from Ganga.Lib.Executable.Executable import Executable
from GangaAtlas.Lib.AthenaMC.AthenaMC import AthenaMC, AthenaMCSplitterJob
from GangaAtlas.Lib.Athena.Athena import Athena

ExecutableTask = taskify(Executable,"ExecutableTask")
AthenaMCTask = taskify(AthenaMC,"AthenaMCTask")
AthenaMCTaskSplitterJob = taskify(AthenaMCSplitterJob,"AthenaMCTaskSplitterJob")
AthenaTask = taskify(Athena,"AthenaTask")
